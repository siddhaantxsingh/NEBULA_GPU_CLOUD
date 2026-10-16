# Nebula GPU cloud (project 18)

Nebula is the control plane for a multi-tenant GPU cluster. It is written in Rust and has three binaries:

* **`nebulad`** is the scheduler and API server. It does topology-aware gang scheduling over NVLink islands and nodes, weighted fair share with quotas, EASY backfill, priority preemption with a minimal victim set, failure handling and usage metering. Its state is durable: an fsynced journal plus snapshots.
* **`nebula-agent`** runs on each node. It runs job commands with `CUDA_VISIBLE_DEVICES` and gang-rank environment variables, and reconciles its processes against the control plane on every heartbeat.
  * `--runtime docker` runs each job attempt in a container: CPU and memory limits from the job spec, read-only root, all capabilities dropped, no-new-privileges, non-root user, no network, and `--gpus device=<ids>` when real GPUs exist. An OOM kill fails the job; containers left behind by a crashed agent are removed.
  * If `nvidia-smi` is installed the agent reports the real GPUs (name, memory, UUID); otherwise it simulates them.
* **`nebula-sim`** is a discrete-event simulator. It compares scheduling policies using the same scheduler code.

**Status:** all tests pass, and the benchmarks were measured on this machine. The Docker runtime is tested against a real Docker daemon. **There are no real GPUs here.** Real-GPU detection is tested with a stand-in `nvidia-smi`, and GPU pass-through has never run on a GPU (BLOCKED; [owner actions](docs/OWNER_ACTIONS.md)). There is no TLS or mTLS. See [limitations](docs/limitations.md).

## Results

All figures are from [docs/benchmarks.md](docs/benchmarks.md): 32 nodes × 8 GPUs, offered load 1.0, mean of 3 seeds.

| policy | mean wait | p95 wait | light tenants' wait | high-priority wait | jobs on fewest nodes |
|---|---|---|---|---|---|
| FIFO | 5,121 s | 11,962 s | 5,140 s | 26 s | 76 % |
| FIFO + backfill | 1,471 s | 9,204 s | 1,450 s | 42 s | 79 % |
| fair share + backfill | **1,169 s** | **7,157 s** | **216 s** | 32 s | 77 % |
| … with first-fit placement | 1,705 s | 12,778 s | 292 s | 34 s | 63 % |
| … with preemption | 1,327 s | 8,941 s | 242 s | **0 s** | 77 % |

* **Placement.** The first-fit row runs slower because the simulator *assumes* spread jobs are 1.1–1.3× slower. Without that assumption the two placements give the same waits; the run is in benchmarks.md.
* **Control-plane throughput.** It sustains 100 submits/s with 256 nodes heartbeating every second, with every change fsynced. Submit p99 is 35 ms and heartbeat p99 is 7 ms.
* **Under overload** (a flood of 2,000 submits/s), scheduling passes reach 46 ms and p99 latency about 1 s.

## Quick start

```sh
cargo build --release
./target/release/nebulad --config configs/example.json &
./target/release/nebula-agent --server http://127.0.0.1:7070 --token change-me-node --id node-1 --gpus 8 --islands 2 &
curl -s -H 'Authorization: Bearer change-me-research' -H 'content-type: application/json' \
     -d '{"gpus": 4, "est_runtime": 60, "command": ["sh", "-c", "echo $CUDA_VISIBLE_DEVICES; sleep 5"]}' \
     http://127.0.0.1:7070/v1/jobs                                    # {"id":1}
curl -s -H 'Authorization: Bearer change-me-research' http://127.0.0.1:7070/v1/jobs/1
curl -s -H 'Authorization: Bearer change-me-research' http://127.0.0.1:7070/v1/usage
curl -s http://127.0.0.1:7070/metrics
# container runtime: limits come from the job ("cpus", "memory_mb", "image") or the agent defaults
./target/release/nebula-agent --server http://127.0.0.1:7070 --token change-me-node --id node-2 --gpus 8 \
     --runtime docker --image alpine:3.20 --default-memory-mb 512 &
./target/release/nebula-sim --jobs 4000 --load 1.0 --seeds 3          # policy comparison
```

## Documentation

* [Architecture](docs/architecture.md): the scheduler, durability, the node protocol and the API.
* [Testing](docs/testing.md): property tests of the scheduler invariants, journal crash tests at every byte, and a multi-process end-to-end test covering gangs, preemption, node and GPU failure, and a control-plane restart.
* [Benchmarks](docs/benchmarks.md); reproduce them with `bench/run.sh`.
* [Limitations](docs/limitations.md).
* [Requirements matrix](docs/requirements.md).

## Layout

```
src/model.rs     cluster, nodes, GPUs, tenants, jobs
src/sched.rs     scheduler (pure functions) and invariant checker
src/store.rs     commands, journal, snapshots, recovery
src/server.rs    HTTP API, control loop, node protocol
src/sim.rs       workload model and discrete-event simulator
src/runtime.rs   Docker container runtime for the agent
src/gpu.rs       nvidia-smi GPU discovery
src/bin/         nebulad, nebula-agent, nebula-sim, nebula-bench
tests/           sched (incl. proptest), store, sim, e2e, agent_runtime (Docker)
scripts/         owner_gpu_check.sh (real-GPU check for the owner)
bench/           run.sh, report.py, results/
```

## Scorecard (2026-10-15, self-assessed)

| Score | /10 | Why not higher |
|---|---|---|
| Fresher-portfolio strength | 8 | Strong scheduler, durability and container work, but nothing has run on a real GPU. |
| Engineering maturity | 7 | Property, crash and real-Docker tests; but one lock, no TLS, and the agent shells out to the docker CLI. |
| Deployment readiness | 4 | No TLS/mTLS, no HA, no real-GPU validation, no packaging or service units. |
| Completion of intended scope | 7 | Scheduling, recovery, quotas and containers done; real GPU, NVML health and mTLS are not. |
