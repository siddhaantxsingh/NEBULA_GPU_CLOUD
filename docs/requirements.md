# Requirements matrix — Project 18 (Nebula GPU cloud)

| Requirement | Status | Evidence |
|---|---|---|
| Multi-tenant job API with per-tenant credentials and isolation | VERIFIED | e2e: 401 / 403 / 404 cases; tenants see only their own jobs |
| Topology-aware placement (NVLink islands, nodes) | VERIFIED | sched tests; `topology_placement_is_compact` property; e2e island check; simulator compactness |
| Gang scheduling for multi-node jobs (all-or-nothing) | VERIFIED | sched gang test; e2e 12-GPU job on two nodes with ranks |
| Fair share with weights; quotas | VERIFIED | sched tests; simulator per-tenant waits |
| Backfill (EASY) that does not delay the reserved job | VERIFIED | sched backfill tests; simulator |
| Priority preemption with a minimal victim set | VERIFIED | sched tests (minimality checked by mutation); e2e preemption with process kill and restart |
| Node failure detection and job requeue | VERIFIED | e2e agent SIGKILL → requeue after timeout; property tests |
| GPU failure handling | VERIFIED (injected, simulated GPUs) | e2e `fail-gpu-N`; property tests |
| Node agents running real commands with GPU isolation env | VERIFIED (env only) | e2e checks `CUDA_VISIBLE_DEVICES`, `NEBULA_*`; no real GPUs |
| Durable, crash-safe control-plane state | VERIFIED | journal cut at every byte; replay = live state (property); snapshot/journal crash window; e2e restart with running jobs |
| Usage metering per job / tenant | VERIFIED | metering test; e2e `/v1/usage` |
| Billing (prices, invoices) | NOT IMPLEMENTED | — |
| Policy comparison by simulation | VERIFIED | `nebula-sim`; docs/benchmarks.md (slow-down factors are assumptions) |
| Control-plane load measurement | VERIFIED | `nebula-bench`; docs/benchmarks.md |
| Observability | VERIFIED (basic) | `/metrics` (GPUs, jobs, nodes, passes, preemptions, failures); structured logs |
| Drain / undrain nodes | VERIFIED | e2e |
| Container job runtime (`--runtime docker`): CPU/memory limits from the job spec, read-only rootfs, no-new-privileges, all caps dropped, non-root, `--network none`, PID limit | VERIFIED (2026-10-15) | `tests/agent_runtime.rs::container_limits_hardening_and_cancel` checks `docker inspect` values and the sandbox from inside; [evidence](evidence/2026-10-15/agent_runtime_tests.log), 3 more runs in [agent_runtime_repeat3.log](evidence/2026-10-15/agent_runtime_repeat3.log) |
| OOM kill reported as job failure | VERIFIED (2026-10-15) | `container_oom_kill_fails_the_job`: 16 MiB limit, reason `exit code 137 ...: OOM-killed (memory limit 16 MiB)` on this machine (cgroup v1). On GitHub's runner (cgroup v2) Docker reported `OOMKilled=false` (CI run 38784660051 failed on this); the agent now reports an unrequested SIGKILL as `probable OOM` |
| Cancel stops the container (SIGTERM, then SIGKILL after the grace period) | VERIFIED (2026-10-15) | same test: the job's TERM handler runs; a TERM-ignoring PID 1 is removed after ≥ 2 s grace |
| Orphaned containers cleaned up after agent `kill -9` | VERIFIED (2026-10-15) | `orphaned_containers_are_removed_after_agent_kill9`: the old container survives the agent, the restarted agent removes it and runs attempt 1. Orphans are removed, not adopted (the control plane already requeued them) |
| Real GPU detection via `nvidia-smi` (name, memory, UUID) | VERIFIED (parser and agent wiring only) | `src/gpu.rs` unit tests; `agent_reports_gpus_from_nvidia_smi` uses a stand-in `nvidia-smi` script. The CSV fixtures follow the documented format; they were not captured from real hardware |
| GPU pass-through to containers (`--gpus device=<ids>`, renumbered `CUDA_VISIBLE_DEVICES`) | IMPLEMENTED BUT UNVERIFIED | unit test of the `docker run` arguments only. Real run BLOCKED: no GPU here; owner script `scripts/owner_gpu_check.sh` (RTX 4060), see [OWNER_ACTIONS.md](OWNER_ACTIONS.md) |
| NVML/DCGM health (ECC, Xid, thermals) | NOT IMPLEMENTED | file-based failure injection only |
| Process runtime cgroups / GPU memory limits | NOT IMPLEMENTED | the default `process` runtime has no resource limits; use `--runtime docker` |
| HA control plane, fencing of partitioned nodes | NOT IMPLEMENTED | docs/limitations.md |
| TLS / mTLS, per-node identity | NOT IMPLEMENTED | static bearer tokens; not done in the 2026-10-15 pass (time budget); docs/limitations.md |
| CI | VERIFIED | `.github/workflows/nebula-gpu-cloud.yml` (run 38522697861 green) |
