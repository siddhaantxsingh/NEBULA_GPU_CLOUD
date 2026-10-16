# Architecture

```
 tenants ──HTTP (API key)──►┐
 operators ─HTTP (admin)───►│  nebulad
                            │   ├─ HTTP API (axum)           src/server.rs
                            │   ├─ control loop (tick_ms):   dead-node detection → scheduling pass
                            │   ├─ scheduler (pure)          src/sched.rs
                            │   └─ store: snapshot + journal src/store.rs
 nebula-agent ──heartbeat──►┘        ▲ reply: full list of job attempts this node must run
   (one per node; fake GPUs; runs job commands with CUDA_VISIBLE_DEVICES)
```

## State and scheduling

* **Model** (`src/model.rs`). A `Cluster` has nodes, tenants and jobs.
  * Each node has GPUs with a model name, an NVLink *island* id and a health flag, a state (`up`, `draining`, `down`) and a map from GPU to the job holding it.
  * A job has a spec (tenant, GPU count, optional GPU model, priority, preemptible, runtime estimate, command), a state, a restart count and metered GPU-seconds.
* **Scheduler** (`src/sched.rs`). Pure functions of `(Cluster, now, Policy)`. The same code runs in the server, the simulator and the property tests.
  * **Order.** Priority first. Then either submission order (FIFO) or **fair share**: GPUs the tenant holds ÷ its weight, recomputed after every start in a pass.
  * **Quota.** A job that would push its tenant past `quota_gpus` is skipped without blocking anyone else.
  * **Placement (topology-aware).**
    * A job that fits on one node goes to the node with the fewest free GPUs that still fits it (best fit, so whole nodes stay free for big jobs). Inside that node it goes to one island if possible, again best fit.
    * A larger job is a **gang**: it takes whole free nodes first, then best-fits the remainder. It is placed all-or-nothing.
    * `first_fit` is the topology-unaware baseline.
  * **Preemption.** When the first job that cannot be placed (the *head*) has a higher priority than some running preemptible jobs:
    * candidates are evicted lowest priority, then youngest, first, until the head fits;
    * the set is then **minimised**: each victim is dropped again if the head still fits without it;
    * victims are requeued, with their restart count incremented and their GPU time metered.
  * **Backfill (EASY).** Otherwise the head gets a reservation at the *shadow time*: when enough compatible GPUs free up, going by running jobs' estimates. A later job may start now only if it:
    * ends before the shadow time; or
    * uses only GPUs that are still spare at the shadow time; or
    * uses a different GPU model.

    Without backfill, the pass stops at the head (strict order).
  * **Failures.**
    * `node_down` requeues every job with a GPU on that node. That includes gang jobs that also hold GPUs elsewhere: all their GPUs are released.
    * `gpu_failed` marks the GPU unhealthy and requeues its job.
  * **Invariants** (`check_invariants`, checked by the property tests after every operation):
    * every running job holds exactly the GPUs it asked for, of the right model;
    * no GPU is shared, and the node maps match the job placements;
    * no tenant is over quota;
    * no running job is on a down node or a failed GPU.

## Durability

* **Journal.** Every state change is a `Cmd`, applied through one deterministic function (`store::apply`).
  * Records are framed `[len][crc32c][seq][json]`; a record is appended and fsynced before the request that caused it returns.
  * A scheduling pass is journaled as its *decisions* (start X on these GPUs, preempt Y), not as "run the scheduler", so replay does not depend on the configured policy.
* **Snapshots.** Every 10,000 records the cluster is written to `snapshot.json` (temporary file, fsync, rename, fsync of the directory), and then a new empty journal is started.
* **Recovery.** Load the snapshot, replay journal records with a higher sequence number, and truncate a torn or corrupt tail.
  * A crash between the snapshot rename and the journal reset leaves old records, which replay skips by sequence number.
  * If a journal write fails, the process aborts rather than serve state that is not on disk.
* **Heartbeat times are not journaled.** After a restart every node gets a fresh timeout.

## Node protocol

* **Registration.** `POST /v1/nodes/register` with the node's GPUs (index, model, island).
  * Re-registering a known node means the agent restarted and lost its processes, so the jobs it held are requeued.
* **Heartbeats.** `POST /v1/nodes/{id}/heartbeat` every `--interval-ms` carries:
  * the attempts the node is running;
  * attempts that exited, with exit codes (re-sent until a heartbeat succeeds);
  * GPUs that failed.
* **Level-triggered replies.** The reply is the *complete* list of `(job, attempt)` pairs this node should run, with the GPU indices, the command and the environment:
  * `CUDA_VISIBLE_DEVICES`;
  * `NEBULA_JOB_ID`, `NEBULA_ATTEMPT` and `NEBULA_TENANT`;
  * `NEBULA_NODE_RANK`, `NEBULA_NUM_NODES` and `NEBULA_MASTER_NODE`.
* **Reconciliation.** The agent starts any listed attempt it is not running and stops any process that is not listed: SIGTERM to the job's process group, then SIGKILL after 3 s.
  * A lost reply, or a duplicated one, therefore cannot leave a stray process or start a job twice.
  * Attempt numbers keep a preempted attempt's late exit report from completing the restarted job.
* **Completion.**
  * A non-zero exit on any node fails the job.
  * Exit 0 on rank 0 (the first node of the placement) completes it; the other ranks' processes are then stopped.
* **Failure detection.**
  * A node that misses heartbeats for `heartbeat_timeout_s` is marked down and its jobs are requeued.
  * Its next heartbeat brings it back up, and the agent kills processes that are no longer assigned.
  * A GPU failure is injected by creating `<work-dir>/fail-gpu-<n>` on the agent.

## API

| Method | Path | Auth | Purpose |
|---|---|---|---|
| POST | `/v1/jobs` | tenant key | submit `{gpus, est_runtime, command, gpu_model?, priority?, preemptible?, name?}` |
| GET | `/v1/jobs`, `/v1/jobs/{id}` | tenant key | own jobs only (others' are 404) |
| DELETE | `/v1/jobs/{id}` | tenant key | cancel |
| GET | `/v1/usage` | tenant key | GPU-hours (finished attempts + running so far), GPUs in use, quota, jobs by state |
| GET | `/v1/admin/cluster`, `/v1/admin/usage` | admin token | nodes, GPU health, counters; usage for every tenant |
| POST/DELETE | `/v1/admin/nodes/{id}/drain` | admin token | drain / undrain (running jobs continue; no new work) |
| POST | `/v1/nodes/register`, `/v1/nodes/{id}/heartbeat` | node token | agent protocol |
| DELETE | `/v1/nodes/{id}` | node token | agent leaving (its jobs are requeued) |
| GET | `/metrics`, `/healthz` | — | Prometheus text (GPUs, jobs, nodes, passes, starts, preemptions, failures, requeues) |

* Secrets are compared in constant time.
* A tenant may not request a priority above its configured `max_priority`, which defaults to 0. Only trusted tenants can preempt.
