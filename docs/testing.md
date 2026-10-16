# Testing

`cargo test --release` runs everything (about 30 s).

| Suite | What it checks |
|---|---|
| `tests/sched.rs` (16 tests) | Targeted scheduler behaviour, plus two properties (below). |
| `tests/store.rs` (3 tests) | Recovery: replay and crash safety (below). |
| `tests/sim.rs` (3 tests) | The simulator (below). |
| `tests/agent_runtime.rs` (4 tests) | The Docker runtime against the real daemon, and GPU detection with a stand-in `nvidia-smi` (below). |
| `src/gpu.rs`, `src/runtime.rs` unit tests | `nvidia-smi` CSV parser (5 tests); `docker run` arguments: hardening flags, limits, GPU pass-through and renumbering, root refused (3 tests). |
| `tests/e2e.rs` (2 tests) | Real `nebulad` and two `nebula-agent` processes on loopback, with shell commands as jobs (below). |
| `src/store.rs` unit tests | The CRC-32C known-answer value; record decoding stops at a torn or corrupt record at every cut point. |

## `tests/sched.rs`

The targeted tests cover:

* **Submit validation.**
* **Island placement**, including best fit between islands.
* **Topology versus first fit.** First fit splits a 4-GPU job across nodes; topology placement does not.
* **Single-node best fit** keeps empty nodes free.
* **Gangs.** A gang takes whole nodes, and a gang that cannot fit allocates nothing.
* **Strict FIFO and EASY backfill.** A short job backfills; a long job that would delay the head does not; the head starts at its shadow time.
* **Backfill into GPUs the head will not need.**
* **Preemption.**
  * Equal priority never preempts.
  * The youngest low-priority job is evicted first.
  * Nothing is evicted when eviction cannot help.
  * The victim set is minimal, not just a prefix of the eviction order.
* **Quotas** do not block other tenants.
* **Fair share**, including weights.
* **Node and GPU failures** requeue jobs, and a failed GPU is never reused.
* **Metering across attempts.**
* **GPU-model constraints.**

The two properties, each over 300 cases:

* **`invariants_and_eventual_progress`:**
  * Random sequences of submits, completions, cancels, node down/up, GPU failures and scheduling passes, under all 16 policy combinations, on 4 nodes and 3 tenants with quotas and weights.
  * `check_invariants` and the extra checks (nothing on down nodes or failed GPUs) hold after every step.
  * Then, with the hardware repaired, every queued job runs within 500 passes under 5 policies (no deadlock or starvation).
* **`topology_placement_is_compact`:** for random occupancy, a job that fits on one node is never split, and a job that fits in one island stays in one island.

**Mutation checks** (done by hand while developing). Each of these mutations of `src/sched.rs` makes at least one test fail:

* backfill ignores the reservation;
* the preemption set is not minimised;
* `release` does not free GPUs;
* island choice is worst fit instead of best fit.

## `tests/store.rs`

* **`replay_reproduces_live_state`** (64 cases): random command streams drive the state the way the server does, with compaction every 1–19 records or never. Reopening yields exactly the live state.
* **`crash_at_every_byte_recovers_a_prefix`:** the journal is cut at every byte. Recovery yields the state after the last whole record, every record boundary is reached, and the torn tail is truncated.
* **`crash_between_snapshot_and_new_journal_does_not_double_apply`:** fails if replay does not skip records already in the snapshot (checked by mutation).

## `tests/sim.rs`

* Every job finishes, and runs are deterministic, including with node failures.
* The placement slow-down model gives the expected factors.
* On a loaded cluster:
  * backfill lowers mean wait against FIFO;
  * topology placement is more compact than first fit;
  * fair share lowers the light tenants' wait;
  * preemption does not raise high-priority wait.

## `tests/e2e.rs`

Real `nebulad` and two `nebula-agent` processes (8 fake GPUs each, 2 islands) on loopback; jobs are shell commands. The scenarios:

* **Auth and validation.** Wrong key: 401. Priority above the tenant maximum: 403. Too many GPUs: 422. Empty command: 400. Another tenant's job: 404.
* **A 4-GPU job** gets one island. `CUDA_VISIBLE_DEVICES` and `NEBULA_*` are set correctly.
* **A 12-GPU gang** runs as ranks 0 and 1 on two nodes, 8 + 4 GPUs.
* **A failing command** (`exit 3`) fails the job with the exit code in its reason.
* **Preemption.**
  * A non-preemptible priority-10 job evicts a running 16-GPU job.
  * Both ranks of attempt 0 are killed: the log shows `start 0` twice, then `start 1` twice and `end 1` twice, and no `end 0`.
  * The evicted job restarts and succeeds with `restarts = 1`, and its GPU time is metered.
* **Node failure.**
  * n1 is drained so the job lands on n2. Then n2's agent is SIGKILLed.
  * After the 2 s heartbeat timeout (and not before), the job is requeued onto n1 and succeeds. n2 shows as down.
  * The restarted agent brings n2 back.
* **GPU failure.** An injected GPU failure requeues the running job. The GPU is marked unhealthy and capacity drops to 15.
* **Control-plane restart.** `nebulad` is stopped with SIGTERM and restarted on the same data directory.
  * All jobs keep their state.
  * A job that was running at the time finishes without a restart; its command ran exactly once.
* **Usage and `/metrics`.**

The second test, `example_config_parses`, loads `configs/example.json`.

The e2e test was run 5 times in a row without failure while developing (about 17 s each).

## `tests/agent_runtime.rs`

Real `nebulad` and `nebula-agent --runtime docker` processes, with the real Docker daemon and `alpine:3.20`. Without Docker the tests print `SKIPPED` and pass, unless `NEBULA_REQUIRE_DOCKER=1` is set; CI sets it.

* **Limits and hardening.** A probe job prints its uid/gid (65534), fails to write to `/` (read-only rootfs), shows `CapEff` 0 and `NoNewPrivs` 1, sees only the `lo` interface, and can write to `/tmp`. A running container's `docker inspect` shows 0.5 CPU, 64 MiB memory and swap, read-only root, `CapDrop [ALL]`, `no-new-privileges`, network `none`, PID limit 256, user `65534:65534`, and the `nebula.*` labels.
* **Cancel.** Cancelling a job runs its SIGTERM handler, then the container is removed. A job whose PID 1 ignores SIGTERM (`sleep`) is killed after the 2 s grace period, not before.
* **OOM.** `tail /dev/zero` under a 16 MiB limit fails the job with `exit code 137 ... OOM-killed (memory limit 16 MiB)`. Docker on GitHub's cgroup-v2 runner did not set `OOMKilled`; there the agent reports `killed by SIGKILL; probable OOM`, and the test accepts either. A small job on the same node still succeeds.
* **Orphans.** The agent is SIGKILLed while a job's container runs. The container survives. A new agent with the same id removes it and runs attempt 1 in a new container.
* **GPU detection.** A stand-in `nvidia-smi` script prints one RTX 4060 line. The node registers with that name, `memory_mb` 8188 and the UUID, and a job constrained to that model runs with `CUDA_VISIBLE_DEVICES=0`.

Runs on 2026-10-15 (machine shared with other agents): [agent_runtime_tests.log](evidence/2026-10-15/agent_runtime_tests.log), three more in [agent_runtime_repeat3.log](evidence/2026-10-15/agent_runtime_repeat3.log). Full suite, fmt and clippy: [ci_local.log](evidence/2026-10-15/ci_local.log).

## Not tested

* Real GPUs or CUDA, and `--gpus` pass-through. See [OWNER_ACTIONS.md](OWNER_ACTIONS.md).
* Network partitions where an agent keeps running jobs while the control plane has requeued them elsewhere. The jobs would run twice until the agent's next successful heartbeat; see [limitations](limitations.md).
