//! Compare scheduling policies on a simulated cluster.
//!
//! nebula-sim [--jobs N] [--load X] [--seeds K] [--nodes N] [--mtbf-h H] [--no-penalty] [--out FILE]

use nebula::sched::{Order, PlacementPolicy, Policy};
use nebula::sim::{self, SimConfig, SimResult};
use serde::Serialize;

#[derive(Serialize)]
struct Report {
    config: SimConfig,
    seeds: Vec<u64>,
    /// one entry per policy; each field is the mean over seeds
    results: Vec<SimResult>,
    per_seed: Vec<Vec<SimResult>>,
}

pub fn policies() -> Vec<(&'static str, Policy)> {
    use Order::*;
    use PlacementPolicy::*;
    vec![
        (
            "fifo",
            Policy {
                order: Fifo,
                backfill: false,
                placement: Topology,
                preemption: false,
            },
        ),
        (
            "fifo+backfill",
            Policy {
                order: Fifo,
                backfill: true,
                placement: Topology,
                preemption: false,
            },
        ),
        (
            "fairshare+backfill",
            Policy {
                order: FairShare,
                backfill: true,
                placement: Topology,
                preemption: false,
            },
        ),
        (
            "fairshare+backfill, first-fit",
            Policy {
                order: FairShare,
                backfill: true,
                placement: FirstFit,
                preemption: false,
            },
        ),
        (
            "fairshare+backfill+preemption",
            Policy {
                order: FairShare,
                backfill: true,
                placement: Topology,
                preemption: true,
            },
        ),
    ]
}

fn mean(rs: &[SimResult]) -> SimResult {
    let n = rs.len() as f64;
    let m = |f: fn(&SimResult) -> f64| rs.iter().map(f).sum::<f64>() / n;
    let mut out = rs[0].clone();
    out.makespan_h = m(|r| r.makespan_h);
    out.utilisation = m(|r| r.utilisation);
    out.efficiency = m(|r| r.efficiency);
    out.mean_wait_s = m(|r| r.mean_wait_s);
    out.p50_wait_s = m(|r| r.p50_wait_s);
    out.p95_wait_s = m(|r| r.p95_wait_s);
    out.p99_wait_s = m(|r| r.p99_wait_s);
    out.mean_wait_high_prio_s = m(|r| r.mean_wait_high_prio_s);
    out.mean_jct_s = m(|r| r.mean_jct_s);
    out.mean_bounded_slowdown = m(|r| r.mean_bounded_slowdown);
    out.frac_node_compact = m(|r| r.frac_node_compact);
    out.frac_island_compact = m(|r| r.frac_island_compact);
    out.preemptions = (m(|r| r.preemptions as f64)).round() as u64;
    out.restarts_from_failures = (m(|r| r.restarts_from_failures as f64)).round() as u64;
    out.node_failures = (m(|r| r.node_failures as f64)).round() as u64;
    out.wait_heavy_tenant_s = m(|r| r.wait_heavy_tenant_s);
    out.wait_light_tenants_s = m(|r| r.wait_light_tenants_s);
    out.sched_mean_us = m(|r| r.sched_mean_us);
    out.sched_max_us = rs.iter().map(|r| r.sched_max_us).fold(0.0, f64::max);
    for (t, s) in out.tenants.iter_mut() {
        s.mean_wait_s = rs.iter().map(|r| r.tenants[t].mean_wait_s).sum::<f64>() / n;
        s.gpu_hours = rs.iter().map(|r| r.tenants[t].gpu_hours).sum::<f64>() / n;
    }
    out
}

fn main() {
    let mut cfg = SimConfig::default();
    let mut seeds = 3u64;
    let mut out = None;
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < args.len() {
        let v = || args.get(i + 1).expect("missing value").clone();
        match args[i].as_str() {
            "--jobs" => cfg.jobs = v().parse().unwrap(),
            "--load" => cfg.load = v().parse().unwrap(),
            "--nodes" => cfg.nodes = v().parse().unwrap(),
            "--seeds" => seeds = v().parse().unwrap(),
            "--mtbf-h" => cfg.node_mtbf_h = v().parse().unwrap(),
            "--out" => out = Some(v()),
            "--no-penalty" => {
                cfg.cross_island = 1.0;
                cfg.cross_node = 1.0;
                i -= 1;
            }
            a => panic!("unknown argument {a}"),
        }
        i += 2;
    }
    let seed_list: Vec<u64> = (1..=seeds).collect();
    let mut results = vec![];
    let mut per_seed = vec![];
    println!(
        "{} nodes x {} GPUs, {} jobs, load {:.2}, {} seeds{}",
        cfg.nodes,
        cfg.gpus_per_node,
        cfg.jobs,
        cfg.load,
        seeds,
        if cfg.node_mtbf_h > 0.0 {
            format!(", node MTBF {} h", cfg.node_mtbf_h)
        } else {
            String::new()
        }
    );
    println!(
        "{:<30} {:>6} {:>7} {:>7} {:>8} {:>7} {:>7} {:>7} {:>8} {:>6} {:>6} {:>7} {:>8}",
        "policy", "util", "effic.", "wait", "p95 wait", "hi-pri", "heavy", "light", "slowdown", "node%", "isl%", "preempt", "sched"
    );
    for (name, p) in policies() {
        let rs: Vec<SimResult> = seed_list
            .iter()
            .map(|&s| {
                let mut c = cfg.clone();
                c.seed = s;
                sim::run(&c, &p, name)
            })
            .collect();
        let r = mean(&rs);
        println!(
            "{:<30} {:>5.1}% {:>6.1}% {:>6.0}s {:>7.0}s {:>6.0}s {:>6.0}s {:>6.0}s {:>8.2} {:>5.1}% {:>5.1}% {:>7} {:>6.0}us",
            name,
            100.0 * r.utilisation,
            100.0 * r.efficiency,
            r.mean_wait_s,
            r.p95_wait_s,
            r.mean_wait_high_prio_s,
            r.wait_heavy_tenant_s,
            r.wait_light_tenants_s,
            r.mean_bounded_slowdown,
            100.0 * r.frac_node_compact,
            100.0 * r.frac_island_compact,
            r.preemptions,
            r.sched_mean_us
        );
        results.push(r);
        per_seed.push(rs);
    }
    if let Some(path) = out {
        let rep = Report {
            config: cfg,
            seeds: seed_list,
            results,
            per_seed,
        };
        std::fs::write(&path, serde_json::to_string_pretty(&rep).unwrap()).unwrap();
        println!("wrote {path}");
    }
}
