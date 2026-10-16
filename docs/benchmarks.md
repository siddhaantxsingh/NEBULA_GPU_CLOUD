# Benchmarks

Produced by `bench/run.sh 2026-10-14` on a 4-vCPU cloud VM (no GPUs: GPUs are simulated in both the simulator and the load test). Raw JSON is in `bench/results/`.

## Scheduling policies (discrete-event simulator)

`nebula-sim` runs the scheduler code the control plane uses (`src/sched.rs`) against a synthetic workload (`src/sim.rs`): Poisson arrivals; 1/2/4/8/16/32-GPU jobs with probabilities 60/10/12/12/4/2 %; lognormal runtimes (median 10 min, σ = 1.2, capped at 24 h); user estimates 1–3× the true runtime; four tenants with equal weights, one of which (the *heavy* tenant) submits half the jobs; 10 % high-priority, non-preemptible jobs.

**Assumed, not measured:** a job placed across NVLink islands runs 1.1× slower and one spread over more nodes than necessary 1.3× slower. These factors feed *efficiency* and the waits; the placement columns (*min nodes*, *one island*) do not depend on them. The run without slow-down shows the policies with the assumption removed.

Columns: *util* = mean share of GPUs allocated while jobs are still arriving; *efficiency* = compact-placement GPU time of all jobs ÷ GPU time actually allocated (lost to slow placements, preemption and failures); waits are total time queued per job; *bounded slowdown* = mean of max(1, (wait + run) / max(run, 60 s)); *min nodes* = share of starts on the fewest possible nodes; *one island* = share of ≤ 4-GPU starts inside one island.

### Offered load 0.8

32 nodes × 8 GPUs (2 NVLink islands per node), 4,000 jobs, offered load 0.80, mean of seeds [1, 2, 3].

| policy | util | efficiency | mean wait | p95 wait | high-prio wait | heavy tenant wait | light tenants wait | bounded slowdown | min nodes | one island | preemptions | restarts (failures) | sched pass |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| fifo | 80.5 % | 91.7 % | 629 s | 1,999 s | 14 s | 619 s | 638 s | 2.86 | 89.2 % | 91.3 % | 0 | 0 | 106 µs mean / 2.2 ms max |
| fifo+backfill | 80.1 % | 92.6 % | 176 s | 905 s | 18 s | 174 s | 178 s | 1.42 | 91.1 % | 92.3 % | 0 | 0 | 109 µs mean / 3.7 ms max |
| fairshare+backfill | 80.3 % | 92.7 % | 103 s | 422 s | 14 s | 170 s | 37 s | 1.25 | 90.5 % | 92.1 % | 0 | 0 | 100 µs mean / 6.5 ms max |
| fairshare+backfill, first-fit | 89.4 % | 80.8 % | 307 s | 1,447 s | 24 s | 538 s | 79 s | 1.73 | 64.0 % | 76.1 % | 0 | 0 | 110 µs mean / 3.0 ms max |
| fairshare+backfill+preemption | 80.3 % | 92.3 % | 151 s | 613 s | 0 s | 265 s | 39 s | 1.36 | 89.6 % | 91.4 % | 123 | 0 | 126 µs mean / 5.8 ms max |

### Offered load 0.9

32 nodes × 8 GPUs (2 NVLink islands per node), 4,000 jobs, offered load 0.90, mean of seeds [1, 2, 3].

| policy | util | efficiency | mean wait | p95 wait | high-prio wait | heavy tenant wait | light tenants wait | bounded slowdown | min nodes | one island | preemptions | restarts (failures) | sched pass |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| fifo | 89.5 % | 87.3 % | 2,238 s | 5,972 s | 19 s | 2,228 s | 2,247 s | 7.72 | 82.0 % | 88.2 % | 0 | 0 | 98 µs mean / 2.8 ms max |
| fifo+backfill | 88.9 % | 88.9 % | 575 s | 3,738 s | 30 s | 583 s | 567 s | 2.23 | 85.8 % | 90.4 % | 0 | 0 | 136 µs mean / 4.4 ms max |
| fairshare+backfill | 89.3 % | 87.9 % | 451 s | 1,756 s | 21 s | 814 s | 92 s | 1.92 | 83.7 % | 89.4 % | 0 | 0 | 137 µs mean / 14.5 ms max |
| fairshare+backfill, first-fit | 94.5 % | 80.7 % | 839 s | 5,709 s | 28 s | 1,540 s | 148 s | 2.71 | 63.0 % | 75.1 % | 0 | 0 | 137 µs mean / 2.1 ms max |
| fairshare+backfill+preemption | 88.5 % | 88.6 % | 490 s | 2,055 s | 0 s | 884 s | 101 s | 1.96 | 84.2 % | 89.4 % | 213 | 0 | 164 µs mean / 3.9 ms max |

### Offered load 1.0

32 nodes × 8 GPUs (2 NVLink islands per node), 4,000 jobs, offered load 1.00, mean of seeds [1, 2, 3].

| policy | util | efficiency | mean wait | p95 wait | high-prio wait | heavy tenant wait | light tenants wait | bounded slowdown | min nodes | one island | preemptions | restarts (failures) | sched pass |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| fifo | 93.4 % | 84.1 % | 5,121 s | 11,962 s | 26 s | 5,102 s | 5,140 s | 16.49 | 76.4 % | 85.1 % | 0 | 0 | 125 µs mean / 3.4 ms max |
| fifo+backfill | 94.4 % | 85.4 % | 1,471 s | 9,204 s | 42 s | 1,492 s | 1,450 s | 4.11 | 78.7 % | 87.3 % | 0 | 0 | 221 µs mean / 4.0 ms max |
| fairshare+backfill | 94.0 % | 84.8 % | 1,169 s | 7,157 s | 32 s | 2,137 s | 216 s | 3.29 | 77.2 % | 86.1 % | 0 | 0 | 204 µs mean / 6.2 ms max |
| fairshare+backfill, first-fit | 95.5 % | 80.7 % | 1,705 s | 12,778 s | 34 s | 3,141 s | 292 s | 4.29 | 62.8 % | 75.4 % | 0 | 0 | 193 µs mean / 4.9 ms max |
| fairshare+backfill+preemption | 94.1 % | 84.5 % | 1,327 s | 8,941 s | 0 s | 2,429 s | 242 s | 3.60 | 76.9 % | 85.1 % | 357 | 0 | 235 µs mean / 25.7 ms max |

### Offered load 1.1

32 nodes × 8 GPUs (2 NVLink islands per node), 4,000 jobs, offered load 1.10, mean of seeds [1, 2, 3].

| policy | util | efficiency | mean wait | p95 wait | high-prio wait | heavy tenant wait | light tenants wait | bounded slowdown | min nodes | one island | preemptions | restarts (failures) | sched pass |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| fifo | 94.4 % | 84.0 % | 7,552 s | 17,024 s | 29 s | 7,523 s | 7,580 s | 23.90 | 75.7 % | 85.1 % | 0 | 0 | 126 µs mean / 3.5 ms max |
| fifo+backfill | 95.2 % | 85.2 % | 2,227 s | 14,146 s | 40 s | 2,234 s | 2,220 s | 5.49 | 78.9 % | 88.4 % | 0 | 0 | 260 µs mean / 8.1 ms max |
| fairshare+backfill | 94.9 % | 84.6 % | 1,882 s | 13,873 s | 38 s | 3,394 s | 396 s | 4.56 | 77.3 % | 86.3 % | 0 | 0 | 243 µs mean / 9.1 ms max |
| fairshare+backfill, first-fit | 96.0 % | 80.7 % | 2,314 s | 18,005 s | 43 s | 4,176 s | 483 s | 5.37 | 63.1 % | 75.0 % | 0 | 0 | 219 µs mean / 7.3 ms max |
| fairshare+backfill+preemption | 94.8 % | 84.4 % | 2,102 s | 13,435 s | 0 s | 3,848 s | 385 s | 4.86 | 77.4 % | 85.9 % | 382 | 0 | 310 µs mean / 9.6 ms max |

### Load 1.0 without the placement slow-down assumption

32 nodes × 8 GPUs (2 NVLink islands per node), 4,000 jobs, offered load 1.00, mean of seeds [1, 2, 3], **no placement slow-down**.

| policy | util | efficiency | mean wait | p95 wait | high-prio wait | heavy tenant wait | light tenants wait | bounded slowdown | min nodes | one island | preemptions | restarts (failures) | sched pass |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| fifo | 90.0 % | 100.0 % | 1,110 s | 2,995 s | 17 s | 1,099 s | 1,121 s | 4.28 | 82.1 % | 88.1 % | 0 | 0 | 103 µs mean / 10.6 ms max |
| fifo+backfill | 90.5 % | 100.0 % | 390 s | 1,949 s | 24 s | 394 s | 386 s | 1.78 | 84.2 % | 90.3 % | 0 | 0 | 143 µs mean / 22.5 ms max |
| fairshare+backfill | 90.3 % | 100.0 % | 240 s | 1,105 s | 18 s | 418 s | 64 s | 1.46 | 83.4 % | 89.0 % | 0 | 0 | 124 µs mean / 4.3 ms max |
| fairshare+backfill, first-fit | 90.3 % | 100.0 % | 240 s | 1,105 s | 18 s | 418 s | 64 s | 1.46 | 63.9 % | 76.0 % | 0 | 0 | 122 µs mean / 2.6 ms max |
| fairshare+backfill+preemption | 90.4 % | 99.7 % | 259 s | 1,195 s | 0 s | 451 s | 69 s | 1.51 | 82.6 % | 88.5 % | 230 | 0 | 159 µs mean / 2.9 ms max |

### Node failures

32 nodes × 8 GPUs (2 NVLink islands per node), 4,000 jobs, offered load 0.90, mean of seeds [1, 2, 3], node MTBF 100 h with 30 min repair.

| policy | util | efficiency | mean wait | p95 wait | high-prio wait | heavy tenant wait | light tenants wait | bounded slowdown | min nodes | one island | preemptions | restarts (failures) | sched pass |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| fifo | 90.6 % | 83.8 % | 2,755 s | 6,778 s | 23 s | 2,739 s | 2,771 s | 9.26 | 79.9 % | 87.4 % | 0 | 23 | 115 µs mean / 8.4 ms max |
| fifo+backfill | 89.9 % | 87.0 % | 673 s | 4,355 s | 31 s | 685 s | 662 s | 2.47 | 83.9 % | 90.1 % | 0 | 18 | 143 µs mean / 7.7 ms max |
| fairshare+backfill | 90.9 % | 85.2 % | 540 s | 2,322 s | 25 s | 968 s | 119 s | 2.09 | 80.9 % | 87.8 % | 0 | 22 | 133 µs mean / 4.3 ms max |
| fairshare+backfill, first-fit | 94.3 % | 78.7 % | 1,037 s | 7,014 s | 29 s | 1,916 s | 172 s | 3.11 | 63.1 % | 75.1 % | 0 | 34 | 146 µs mean / 2.7 ms max |
| fairshare+backfill+preemption | 91.4 % | 84.0 % | 577 s | 2,461 s | 0 s | 1,044 s | 116 s | 2.15 | 80.1 % | 88.0 % | 262 | 21 | 173 µs mean / 4.2 ms max |

### Larger cluster (scheduler cost)

256 nodes × 8 GPUs (2 NVLink islands per node), 20,000 jobs, offered load 1.00, mean of seeds [1].

| policy | util | efficiency | mean wait | p95 wait | high-prio wait | heavy tenant wait | light tenants wait | bounded slowdown | min nodes | one island | preemptions | restarts (failures) | sched pass |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| fifo | 94.2 % | 85.0 % | 2,648 s | 6,782 s | 3 s | 2,644 s | 2,651 s | 9.34 | 74.9 % | 84.5 % | 0 | 0 | 1,123 µs mean / 56.9 ms max |
| fifo+backfill | 94.3 % | 87.5 % | 571 s | 6,228 s | 5 s | 570 s | 571 s | 2.73 | 81.6 % | 89.8 % | 0 | 0 | 3,493 µs mean / 60.2 ms max |
| fairshare+backfill | 94.2 % | 86.1 % | 588 s | 2,539 s | 4 s | 1,163 s | 16 s | 2.69 | 78.1 % | 87.1 % | 0 | 0 | 3,686 µs mean / 60.2 ms max |
| fairshare+backfill, first-fit | 96.2 % | 80.3 % | 1,069 s | 11,127 s | 4 s | 2,118 s | 27 s | 4.11 | 61.2 % | 74.1 % | 0 | 0 | 4,128 µs mean / 48.2 ms max |
| fairshare+backfill+preemption | 94.3 % | 86.1 % | 608 s | 3,009 s | 0 s | 1,204 s | 16 s | 2.80 | 77.1 % | 86.3 % | 1,643 | 0 | 3,602 µs mean / 71.5 ms max |

## Control plane under load

`nebula-bench` runs `nebulad` in-process (real HTTP on loopback, real fsynced journal) with simulated nodes that heartbeat on a fixed interval and report each assigned job finished 2 s after it starts, and 16 tenant clients submitting 1–8 GPU jobs. Every submit and every state change from a heartbeat is fsynced before the response.

| scenario | nodes (GPUs) | submits/s | submit p50 / p99 / max | heartbeats/s | heartbeat p50 / p99 / max | jobs finished | queued at end | sched pass mean | journal |
|---|---|---|---|---|---|---|---|---|---|
| 50 submits/s | 64 (512) | 51 | 4.4 / 12.6 / 33 ms | 71 | 0.2 / 3.6 / 24 ms | 882 | 0 | 0.2 ms | 0.4 MB |
| 100 submits/s, 256 nodes | 256 (2048) | 101 | 4.6 / 34.5 / 92 ms | 283 | 0.2 / 6.9 / 83 ms | 1,736 | 0 | 0.6 ms | 0.8 MB |
| closed-loop flood | 64 (512) | 2,009 | 4.2 / 70.4 / 1069 ms | 71 | 5.8 / 1060.1 / 1064 ms | 895 | 39,128 | 46.0 ms | 0.2 MB |

## Reading the results

* **Backfill is the largest single win.** At offered load 1.0, EASY backfill cuts mean wait 3.5× against strict FIFO (5,121 s → 1,471 s) and bounded slowdown from 16.5 to 4.1, at the same utilisation. The reserved head job is never delayed (this is tested, not just measured).
* **Fair share protects light tenants.** One tenant submits half the jobs. Under FIFO + backfill every tenant waits about the same (≈ 1,450–1,490 s at load 1.0). Under fair share the three light tenants wait 216 s and the heavy tenant 2,137 s: the heavy tenant pays for its own backlog. Mean wait over all jobs drops too (1,471 → 1,169 s).
* **Topology-aware placement keeps jobs compact:** 76–91 % of starts on the minimum number of nodes, against 63–64 % for first fit, and 85–92 % of small jobs in one NVLink island, against 75–76 %. Whether that matters depends on the slow-down assumption.
  * With it (1.1× / 1.3×), first fit wastes about 19 % of allocated GPU time (efficiency 80.7 %) and waits 1.5× longer. Its higher *util* figure only means GPUs are held longer by slower jobs.
  * Without it, the two placements give identical waits, because a job that fits somewhere is placed by both. Compactness is then only worth what real interconnect behaviour makes it worth.
* **Preemption buys latency for important work, and others pay for it.** High-priority jobs start immediately (0 s mean wait instead of 14–38 s). Everyone else's mean wait rises 9–47 % against fair share without preemption, and there are 123–382 preemptions per 4,000 jobs. Each preemption throws away the victim's progress, because jobs here do not checkpoint.
* **Node failures** (MTBF 100 h) cost 18–34 job restarts per run and a few points of efficiency, with no stuck or lost jobs.
* **Scheduler cost.** A pass takes 0.1–0.3 ms on 256 GPUs. On 2,048 GPUs with a deep queue it takes 1–4 ms on average and up to 72 ms at worst. In the control plane the pass runs under the state lock, so API calls wait for it.
* **Control plane.**
  * At sustainable rates (50–100 submits/s, 64–256 nodes heartbeating every second) submit p99 is 13–35 ms and heartbeat p99 is 4–7 ms, with every state change fsynced.
  * In the closed-loop flood the API accepts about 2,000 submits/s but the cluster cannot run them, so 39,000 jobs queue. Scheduling passes then take 46 ms, and p99 latency reaches about 1 s.
  * Disabling snapshots did not remove the 1 s worst cases. The most likely cause is contention on the single state lock, which handlers wait for on Tokio worker threads; this was not diagnosed further.
  * The obvious fixes are listed in [limitations](limitations.md): incremental scheduling, a lock that does not block the async runtime, and pruning finished jobs.

