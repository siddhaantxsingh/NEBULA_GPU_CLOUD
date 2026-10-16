# Limitations

* **No real GPUs.** This was built on a machine without GPUs.
  * Without `nvidia-smi` the agent reports a configured number of fake devices and sets `CUDA_VISIBLE_DEVICES`.
  * With `nvidia-smi` it reports the real devices (name, memory, UUID). This path was tested only with a stand-in script that prints the documented CSV format.
  * `--gpus device=<ids>` pass-through in the Docker runtime has never run against a real GPU. `scripts/owner_gpu_check.sh` is the owner's check.
  * NVLink topology is not detected (`nvidia-smi topo -m` is not parsed). Islands come from `--islands`.
  * Health checks (NVML/DCGM: ECC errors, Xid events, thermals) are replaced by a file-based failure injector.
  * Nothing here has run a CUDA program.
* **The container runtime shells out to the `docker` CLI.**
  * One `docker ps` per heartbeat interval, plus `inspect`/`logs`/`rm` when a container exits. This is fine for tens of containers per node, not for hundreds.
  * The CLI calls block the agent's single-threaded event loop for their duration.
  * Logs are copied with `docker logs` after the container exits, not streamed.
  * OOM detection relies on Docker's `OOMKilled` flag. Where Docker does not set it (seen on a cgroup-v2 CI runner), any SIGKILL the agent did not send is reported as a probable OOM, which can be wrong (for example an external `docker kill`).
  * Orphaned containers (after an agent crash) are removed, not adopted. The control plane requeues all of a node's jobs when the node registers again, so adopting them would run the job twice.
  * The job gets a read-only root and a 64 MiB `/tmp` tmpfs; there is no persistent volume or dataset mount.
  * Rootless Docker, user namespaces, seccomp profiles beyond Docker's default, and gVisor are not configured.
  * The image is chosen by the job (`image`) without an allow-list or signature check.
* **The topology model is two-level** (NVLink island inside a node, then nodes). There is no notion of racks, rail-optimised networks, spine oversubscription or PCIe/NUMA affinity.
* **The simulator's slow-down factors (1.1× across islands, 1.3× across extra nodes) are assumptions**, not measurements. The placement-quality metrics do not depend on them, and one run in [benchmarks.md](benchmarks.md) removes them. The workload is synthetic, not a production trace.
* **Fencing is missing.**
  * When a node is declared down, its jobs are requeued at once.
  * If the node was only partitioned, its agent keeps running the old attempt until a heartbeat gets through and the reply no longer lists the attempt; the agent then kills it.
  * Meanwhile the job can run twice. Jobs must tolerate this, for example by checkpointing per attempt (`NEBULA_ATTEMPT`).
  * A production system would have the agent stop its jobs after its own lease expires.
* **A gang job's completion is rank 0's exit code.** Ranks that exit 0 before rank 0 are not restarted. A non-zero exit on any rank fails the whole job immediately.
* **Preemption kills; it does not checkpoint.** A preempted job restarts from scratch unless its command checkpoints by itself.
* **The control plane is one process with one lock.**
  * Every state change is fsynced while the lock is held.
  * With about 10,000 queued jobs a scheduling pass takes tens of milliseconds, during which API calls wait.
  * Handlers wait for the lock on Tokio worker threads. In the overload benchmark, p99 latency reaches about 1 s ([benchmarks.md](benchmarks.md)).
  * There is no HA or replication. A restart keeps all state, but the API is unavailable while the process is down.
* **The job history grows without bound.** Finished jobs are never pruned from the state or the snapshot.
* **Security is minimal.**
  * There are static bearer tokens from the config file, compared in constant time. One shared node token is used by all agents, with no per-node identity or mTLS.
  * There is no TLS: put it behind a TLS-terminating proxy.
  * With the default `process` runtime, commands run as the agent's user, with no isolation, cgroups or GPU memory limits. `--runtime docker` adds container isolation and CPU/memory limits; GPU memory is still not limited.
* **Usage is metered but not billed.** There are GPU-seconds per job and per tenant, but no prices, invoices or budgets.
