"""Render docs/benchmarks.md from bench/results/<date>/*.json: python3 bench/report.py DATE"""
import json
import os
import sys

D = sys.argv[1]
R = f"bench/results/{D}"


def load(name):
    p = os.path.join(R, name)
    return json.load(open(p)) if os.path.exists(p) else None


def sim_table(r):
    cfg = r["config"]
    L = [f"{cfg['nodes']} nodes × {cfg['gpus_per_node']} GPUs ({cfg['islands_per_node']} NVLink islands per node), "
         f"{cfg['jobs']:,} jobs, offered load {cfg['load']:.2f}, mean of seeds {r['seeds']}"
         + (f", node MTBF {cfg['node_mtbf_h']:.0f} h with {cfg['repair_min']:.0f} min repair" if cfg["node_mtbf_h"] > 0 else "")
         + ("" if cfg["cross_node"] != 1.0 else ", **no placement slow-down**") + ".\n",
         "| policy | util | efficiency | mean wait | p95 wait | high-prio wait | heavy tenant wait | light tenants wait | bounded slowdown | min nodes | one island | preemptions | restarts (failures) | sched pass |",
         "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|"]
    for x in r["results"]:
        L.append(f"| {x['policy']} | {100 * x['utilisation']:.1f} % | {100 * x['efficiency']:.1f} % | {x['mean_wait_s']:,.0f} s | "
                 f"{x['p95_wait_s']:,.0f} s | {x['mean_wait_high_prio_s']:,.0f} s | {x['wait_heavy_tenant_s']:,.0f} s | "
                 f"{x['wait_light_tenants_s']:,.0f} s | {x['mean_bounded_slowdown']:.2f} | {100 * x['frac_node_compact']:.1f} % | "
                 f"{100 * x['frac_island_compact']:.1f} % | {x['preemptions']:,} | {x['restarts_from_failures']:,} | "
                 f"{x['sched_mean_us']:,.0f} µs mean / {x['sched_max_us'] / 1000:,.1f} ms max |")
    return L + [""]


L = ["# Benchmarks\n",
     f"Produced by `bench/run.sh {D}` on a 4-vCPU cloud VM (no GPUs: GPUs are simulated in both the simulator and the load test). "
     "Raw JSON is in `bench/results/`.\n",
     "## Scheduling policies (discrete-event simulator)\n",
     "`nebula-sim` runs the scheduler code the control plane uses (`src/sched.rs`) against a synthetic workload (`src/sim.rs`): Poisson "
     "arrivals; 1/2/4/8/16/32-GPU jobs with probabilities 60/10/12/12/4/2 %; lognormal runtimes (median 10 min, σ = 1.2, capped at "
     "24 h); user estimates 1–3× the true runtime; four tenants with equal weights, one of which (the *heavy* tenant) submits half "
     "the jobs; 10 % high-priority, non-preemptible jobs.\n",
     "**Assumed, not measured:** a job placed across NVLink islands runs 1.1× slower and one spread over more nodes than necessary 1.3× "
     "slower. These factors feed *efficiency* and the waits; the placement columns (*min nodes*, *one island*) do not depend on them. "
     "The run without slow-down shows the policies with the assumption removed.\n",
     "Columns: *util* = mean share of GPUs allocated while jobs are still arriving; *efficiency* = compact-placement GPU time of all "
     "jobs ÷ GPU time actually allocated (lost to slow placements, preemption and failures); waits are total time queued per job; "
     "*bounded slowdown* = mean of max(1, (wait + run) / max(run, 60 s)); *min nodes* = share of starts on the fewest possible nodes; "
     "*one island* = share of ≤ 4-GPU starts inside one island.\n"]
for ld in ["0.8", "0.9", "1.0", "1.1"]:
    r = load(f"sim_load{ld}.json")
    if r:
        L += [f"### Offered load {ld}\n"] + sim_table(r)
for name, title in [("sim_nopenalty.json", "Load 1.0 without the placement slow-down assumption"),
                    ("sim_failures.json", "Node failures"),
                    ("sim_large.json", "Larger cluster (scheduler cost)")]:
    r = load(name)
    if r:
        L += [f"### {title}\n"] + sim_table(r)

L += ["## Control plane under load\n",
      "`nebula-bench` runs `nebulad` in-process (real HTTP on loopback, real fsynced journal) with simulated nodes that heartbeat on "
      "a fixed interval and report each assigned job finished 2 s after it starts, and 16 tenant clients submitting 1–8 GPU jobs. "
      "Every submit and every state change from a heartbeat is fsynced before the response.\n",
      "| scenario | nodes (GPUs) | submits/s | submit p50 / p99 / max | heartbeats/s | heartbeat p50 / p99 / max | jobs finished | queued at end | sched pass mean | journal |",
      "|---|---|---|---|---|---|---|---|---|---|"]
for name, title in [("api_steady.json", "50 submits/s"), ("api_256nodes.json", "100 submits/s, 256 nodes"), ("api_flood.json", "closed-loop flood")]:
    r = load(name)
    if not r:
        continue
    s, h = r["submit_ms"], r["heartbeat_ms"]
    L.append(f"| {title} | {r['nodes']} ({r['gpus']}) | {r['submits_per_s']:,.0f} | {s['p50']:.1f} / {s['p99']:.1f} / {s['max']:.0f} ms | "
             f"{r['heartbeats_per_s']:,.0f} | {h['p50']:.1f} / {h['p99']:.1f} / {h['max']:.0f} ms | {r['jobs_succeeded']:,} | "
             f"{r['jobs_queued_at_end']:,} | {r['sched_mean_ms']:.1f} ms | {r['journal_bytes'] / 1e6:.1f} MB |")
L.append("")
L += ["## Reading the results\n",
      "* **Backfill is the largest single win.** At offered load 1.0, EASY backfill cuts mean wait 3.5× against strict FIFO "
      "(5,121 s → 1,471 s) and bounded slowdown from 16.5 to 4.1, at the same utilisation. The reserved head job is never delayed "
      "(this is tested, not just measured).",
      "* **Fair share protects light tenants.** One tenant submits half the jobs. Under FIFO + backfill every tenant waits about the "
      "same (≈ 1,450–1,490 s at load 1.0). Under fair share the three light tenants wait 216 s and the heavy tenant 2,137 s: the heavy "
      "tenant pays for its own backlog. Mean wait over all jobs drops too (1,471 → 1,169 s).",
      "* **Topology-aware placement keeps jobs compact:** 76–91 % of starts on the minimum number of nodes, against 63–64 % for first "
      "fit, and 85–92 % of small jobs in one NVLink island, against 75–76 %. Whether that matters depends on the slow-down assumption.",
      "  * With it (1.1× / 1.3×), first fit wastes about 19 % of allocated GPU time (efficiency 80.7 %) and waits 1.5× longer. Its higher "
      "*util* figure only means GPUs are held longer by slower jobs.",
      "  * Without it, the two placements give identical waits, because a job that fits somewhere is placed by both. Compactness is "
      "then only worth what real interconnect behaviour makes it worth.",
      "* **Preemption buys latency for important work, and others pay for it.** High-priority jobs start immediately (0 s mean "
      "wait instead of 14–38 s). Everyone else's mean wait rises 9–47 % against fair share without preemption, and there are 123–382 "
      "preemptions per 4,000 jobs. Each preemption throws away the victim's progress, because jobs here do not checkpoint.",
      "* **Node failures** (MTBF 100 h) cost 18–34 job restarts per run and a few points of efficiency, with no stuck or lost jobs.",
      "* **Scheduler cost.** A pass takes 0.1–0.3 ms on 256 GPUs. On 2,048 GPUs with a deep queue it takes 1–4 ms on average and up to "
      "72 ms at worst. In the control plane the pass runs under the state lock, so API calls wait for it.",
      "* **Control plane.**",
      "  * At sustainable rates (50–100 submits/s, 64–256 nodes heartbeating every second) submit p99 is 13–35 ms and heartbeat "
      "p99 is 4–7 ms, with every state change fsynced.",
      "  * In the closed-loop flood the API accepts about 2,000 submits/s but the cluster cannot run them, so 39,000 jobs queue. "
      "Scheduling passes then take 46 ms, and p99 latency reaches about 1 s.",
      "  * Disabling snapshots did not remove the 1 s worst cases. The most likely cause is contention on the single state lock, "
      "which handlers wait for on Tokio worker threads; this was not diagnosed further.",
      "  * The obvious fixes are listed in [limitations](limitations.md): incremental scheduling, a lock that does not block the "
      "async runtime, and pruning finished jobs.",
      ""]
print("\n".join(L))
