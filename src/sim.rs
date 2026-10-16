//! Discrete-event cluster simulator. Drives the same `sched` code the
//! control plane uses with a synthetic workload and reports utilisation,
//! waiting times, placement quality and fairness per policy.
//!
//! Workload model (all parameters in `SimConfig`):
//! * Poisson arrivals at a rate that offers `load` × cluster capacity;
//! * GPU counts 1/2/4/8/16/32 with probabilities 60/10/12/12/4/2 %;
//! * lognormal runtimes (median 10 min, σ = 1.2, capped at 24 h); the user's
//!   estimate is the runtime × U(1, 3) (users over-estimate);
//! * 4 tenants; tenant 0 submits half of all jobs; 10 % of jobs have high
//!   priority (and may preempt);
//! * a job placed less compactly than possible runs slower: × `cross_island`
//!   if it spans NVLink islands of one node, × `cross_node` if it uses more
//!   nodes than it needs. These factors are model assumptions, not
//!   measurements; set both to 1.0 to remove them.
//! * optional node failures (exponential, `node_mtbf_h`), repaired after
//!   `repair_min`; jobs on a failed node restart from scratch.

use crate::model::*;
use crate::sched::{self, Decision, Policy};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use rand_distr::{Distribution, Exp, LogNormal};
use serde::Serialize;
use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap};

#[derive(Clone, Debug, Serialize)]
pub struct SimConfig {
    pub nodes: u32,
    pub gpus_per_node: u32,
    pub islands_per_node: u32,
    pub jobs: usize,
    pub load: f64,
    pub seed: u64,
    pub cross_island: f64,
    pub cross_node: f64,
    pub node_mtbf_h: f64,
    pub repair_min: f64,
    pub high_priority_frac: f64,
}

impl Default for SimConfig {
    fn default() -> Self {
        SimConfig {
            nodes: 32,
            gpus_per_node: 8,
            islands_per_node: 2,
            jobs: 4000,
            load: 0.9,
            seed: 1,
            cross_island: 1.1,
            cross_node: 1.3,
            node_mtbf_h: 0.0,
            repair_min: 30.0,
            high_priority_frac: 0.1,
        }
    }
}

#[derive(Clone, Debug)]
pub struct WorkItem {
    pub at: f64,
    pub spec: JobSpec,
    /// runtime on a compact placement
    pub runtime: f64,
}

pub const GPU_MIX: [(u32, f64); 6] = [(1, 0.60), (2, 0.10), (4, 0.12), (8, 0.12), (16, 0.04), (32, 0.02)];
pub const TENANTS: [(&str, f64); 4] = [("t0", 0.5), ("t1", 0.2), ("t2", 0.15), ("t3", 0.15)];

pub fn workload(cfg: &SimConfig) -> Vec<WorkItem> {
    let mut rng = SmallRng::seed_from_u64(cfg.seed);
    let rt = LogNormal::new(600f64.ln(), 1.2).unwrap();
    let mean_gpus: f64 = GPU_MIX.iter().map(|(g, p)| *g as f64 * p).sum();
    let mean_rt = 600.0 * (1.2f64 * 1.2 / 2.0).exp();
    let capacity = (cfg.nodes * cfg.gpus_per_node) as f64;
    let rate = cfg.load * capacity / (mean_gpus * mean_rt);
    let gap = Exp::new(rate).unwrap();
    let pick = |rng: &mut SmallRng, items: &[(u32, f64)]| {
        let mut x: f64 = rng.r#gen();
        for (v, p) in items {
            if x < *p {
                return *v;
            }
            x -= p;
        }
        items.last().unwrap().0
    };
    let tenants: Vec<(u32, f64)> = TENANTS.iter().enumerate().map(|(i, (_, p))| (i as u32, *p)).collect();
    let mut t = 0.0;
    (0..cfg.jobs)
        .map(|i| {
            t += gap.sample(&mut rng);
            let gpus = pick(&mut rng, &GPU_MIX);
            let runtime: f64 = rt.sample(&mut rng).clamp(10.0, 86_400.0);
            let est = runtime * rng.gen_range(1.0..3.0);
            let tenant = TENANTS[pick(&mut rng, &tenants) as usize].0;
            let high = rng.r#gen::<f64>() < cfg.high_priority_frac;
            WorkItem {
                at: t,
                runtime,
                spec: JobSpec {
                    tenant: tenant.into(),
                    gpus,
                    gpu_model: None,
                    priority: if high { 10 } else { 0 },
                    preemptible: !high,
                    est_runtime: est,
                    command: vec![],
                    name: format!("job-{i}"),
                    image: None,
                    cpus: None,
                    memory_mb: None,
                },
            }
        })
        .collect()
}

pub fn build_cluster(cfg: &SimConfig) -> Cluster {
    let mut c = Cluster::default();
    let per = cfg.gpus_per_node / cfg.islands_per_node;
    for n in 0..cfg.nodes {
        let id = format!("node-{n:03}");
        c.nodes.insert(
            id.clone(),
            Node {
                id,
                gpus: (0..cfg.gpus_per_node)
                    .map(|i| Gpu {
                        index: i,
                        model: "h100".into(),
                        island: i / per,
                        healthy: true,
                        memory_mb: None,
                        uuid: None,
                    })
                    .collect(),
                state: NodeState::Up,
                used: BTreeMap::new(),
                last_heartbeat: 0.0,
            },
        );
    }
    for (t, _) in TENANTS {
        c.tenants.insert(
            t.into(),
            Tenant {
                name: t.into(),
                quota_gpus: 0,
                weight: 1.0,
            },
        );
    }
    c
}

/// Slowdown of a placement relative to the most compact one possible.
pub fn placement_factor(cfg: &SimConfig, gpus: u32, pl: &[Placement]) -> (f64, bool, bool) {
    let per_island = cfg.gpus_per_node / cfg.islands_per_node;
    let min_nodes = gpus.div_ceil(cfg.gpus_per_node) as usize;
    let node_ok = pl.len() <= min_nodes;
    let island_ok = if gpus <= per_island {
        pl.len() == 1
            && pl[0]
                .gpus
                .iter()
                .map(|g| g / per_island)
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                == 1
    } else {
        true
    };
    let f = if !node_ok {
        cfg.cross_node
    } else if !island_ok {
        cfg.cross_island
    } else {
        1.0
    };
    (f, node_ok, island_ok)
}

#[derive(Clone, Debug, Serialize, Default)]
pub struct TenantStats {
    pub jobs: usize,
    pub mean_wait_s: f64,
    pub gpu_hours: f64,
}

#[derive(Clone, Debug, Serialize, Default)]
pub struct SimResult {
    pub policy: String,
    pub jobs: usize,
    pub makespan_h: f64,
    /// mean fraction of GPUs allocated while jobs are still arriving
    pub utilisation: f64,
    /// useful GPU-time (compact runtime of each finished job) / all allocated
    /// GPU-time; the rest is slow-down from spread placements and work lost
    /// to preemption or node failure
    pub efficiency: f64,
    pub mean_wait_s: f64,
    pub p50_wait_s: f64,
    pub p95_wait_s: f64,
    pub p99_wait_s: f64,
    pub mean_wait_high_prio_s: f64,
    pub mean_jct_s: f64,
    /// mean of max(1, (wait + run) / max(run, 60 s))
    pub mean_bounded_slowdown: f64,
    /// share of starts on the minimum number of nodes / inside one island (when possible)
    pub frac_node_compact: f64,
    pub frac_island_compact: f64,
    pub preemptions: u64,
    pub restarts_from_failures: u64,
    pub node_failures: u64,
    /// mean wait of the heavy tenant (t0, half of all jobs) and of the
    /// three light tenants together
    pub wait_heavy_tenant_s: f64,
    pub wait_light_tenants_s: f64,
    pub tenants: BTreeMap<String, TenantStats>,
    pub sched_passes: u64,
    pub sched_mean_us: f64,
    pub sched_max_us: f64,
}

#[derive(PartialEq)]
struct T(f64);
impl Eq for T {}
impl PartialOrd for T {
    fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for T {
    fn cmp(&self, o: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&o.0)
    }
}

enum Ev {
    Done(JobId, u32),
    Fail(String),
    Repair(String),
}

pub fn run(cfg: &SimConfig, policy: &Policy, name: &str) -> SimResult {
    let work = workload(cfg);
    let mut c = build_cluster(cfg);
    let mut rng = SmallRng::seed_from_u64(cfg.seed ^ 0x9e37_79b9);
    let mut heap: BinaryHeap<Reverse<(T, u64)>> = BinaryHeap::new();
    let mut events: BTreeMap<u64, Ev> = BTreeMap::new();
    let mut seq = 0u64;
    let mut push = |heap: &mut BinaryHeap<Reverse<(T, u64)>>, events: &mut BTreeMap<u64, Ev>, t: f64, e: Ev| {
        seq += 1;
        heap.push(Reverse((T(t), seq)));
        events.insert(seq, e);
    };
    if cfg.node_mtbf_h > 0.0 {
        let ex = Exp::new(1.0 / (cfg.node_mtbf_h * 3600.0)).unwrap();
        for id in c.nodes.keys().cloned().collect::<Vec<_>>() {
            push(&mut heap, &mut events, ex.sample(&mut rng), Ev::Fail(id));
        }
    }
    let mut runtime: BTreeMap<JobId, f64> = BTreeMap::new();
    let mut wait: BTreeMap<JobId, f64> = BTreeMap::new();
    let mut first_submit: BTreeMap<JobId, f64> = BTreeMap::new();
    let (mut starts, mut node_ok, mut island_ok, mut island_eligible) = (0u64, 0u64, 0u64, 0u64);
    let (mut preemptions, mut failure_restarts, mut failures) = (0u64, 0u64, 0u64);
    let (mut passes, mut sched_total, mut sched_max) = (0u64, 0f64, 0f64);
    let per_island = cfg.gpus_per_node / cfg.islands_per_node;
    let mut next_arrival = 0usize;
    let mut done = 0usize;
    let mut now;
    let window = work.last().map(|w| w.at).unwrap_or(0.0);
    let (mut prev, mut busy_window) = (0.0f64, 0.0f64);
    while done < work.len() {
        let t_arr = work.get(next_arrival).map(|w| w.at).unwrap_or(f64::INFINITY);
        let t_ev = heap.peek().map(|Reverse((T(t), _))| *t).unwrap_or(f64::INFINITY);
        now = t_arr.min(t_ev);
        assert!(now.is_finite(), "simulation stalled with {} jobs unfinished", work.len() - done);
        busy_window += c.used_gpus() as f64 * (now.min(window) - prev.min(window));
        prev = now;
        while next_arrival < work.len() && work[next_arrival].at <= now {
            let w = &work[next_arrival];
            let id = sched::submit(&mut c, w.spec.clone(), w.at).expect("workload fits the cluster");
            runtime.insert(id, w.runtime);
            first_submit.insert(id, w.at);
            next_arrival += 1;
        }
        while heap.peek().is_some_and(|Reverse((T(t), _))| *t <= now) {
            let Reverse((_, s)) = heap.pop().unwrap();
            match events.remove(&s).unwrap() {
                Ev::Done(id, attempt) => {
                    if c.jobs[&id].restarts == attempt && c.jobs[&id].running_placement().is_some() {
                        sched::complete(&mut c, id, now, true, "");
                        done += 1;
                    }
                }
                Ev::Fail(n) => {
                    failures += 1;
                    failure_restarts += sched::node_down(&mut c, &n, now).len() as u64;
                    push(&mut heap, &mut events, now + cfg.repair_min * 60.0, Ev::Repair(n));
                }
                Ev::Repair(n) => {
                    c.nodes.get_mut(&n).unwrap().state = NodeState::Up;
                    let ex = Exp::new(1.0 / (cfg.node_mtbf_h * 3600.0)).unwrap();
                    push(&mut heap, &mut events, now + ex.sample(&mut rng), Ev::Fail(n));
                }
            }
        }
        let t0 = std::time::Instant::now();
        let ds = sched::schedule(&mut c, now, policy);
        let us = t0.elapsed().as_secs_f64() * 1e6;
        passes += 1;
        sched_total += us;
        sched_max = sched_max.max(us);
        for d in ds {
            match d {
                Decision::Preempt { .. } => preemptions += 1,
                Decision::Start { job, placement } => {
                    let j = &c.jobs[&job];
                    // the job may have been preempted again later in the same pass
                    if j.running_placement() != Some(&placement) {
                        continue;
                    }
                    let (f, nok, iok) = placement_factor(cfg, j.spec.gpus, &placement);
                    starts += 1;
                    node_ok += nok as u64;
                    if j.spec.gpus <= per_island {
                        island_eligible += 1;
                        island_ok += iok as u64;
                    }
                    *wait.entry(job).or_default() += now - j.queued_since;
                    let attempt = j.restarts;
                    let end = now + runtime[&job] * f;
                    push(&mut heap, &mut events, end, Ev::Done(job, attempt));
                }
            }
        }
    }
    let end = c
        .jobs
        .values()
        .map(|j| match j.state {
            JobState::Succeeded { end, .. } => end,
            _ => 0.0,
        })
        .fold(0.0, f64::max);
    let gpus = (cfg.nodes * cfg.gpus_per_node) as f64;
    let busy: f64 = c.jobs.values().map(|j| j.gpu_seconds).sum();
    let useful: f64 = c.jobs.values().map(|j| j.spec.gpus as f64 * runtime[&j.id]).sum();
    let mut waits: Vec<f64> = c.jobs.keys().map(|id| wait[id]).collect();
    waits.sort_by(f64::total_cmp);
    let pct = |p: f64| waits[((waits.len() - 1) as f64 * p).round() as usize];
    let mut jct = 0.0;
    let mut bsd: BTreeMap<String, (f64, usize, f64, f64)> = BTreeMap::new();
    let (mut hp, mut hp_n) = (0.0, 0usize);
    for j in c.jobs.values() {
        let JobState::Succeeded { end, .. } = j.state else { unreachable!() };
        let total = end - first_submit[&j.id];
        jct += total;
        let s = (total / runtime[&j.id].max(60.0)).max(1.0);
        let e = bsd.entry(j.spec.tenant.clone()).or_default();
        e.0 += s;
        e.1 += 1;
        e.2 += wait[&j.id];
        e.3 += j.gpu_seconds / 3600.0;
        if j.spec.priority > 0 {
            hp += wait[&j.id];
            hp_n += 1;
        }
    }
    let n = c.jobs.len() as f64;
    let heavy = &bsd["t0"];
    let light = bsd
        .iter()
        .filter(|(t, _)| t.as_str() != "t0")
        .fold((0.0, 0usize), |a, (_, v)| (a.0 + v.2, a.1 + v.1));
    SimResult {
        policy: name.into(),
        jobs: c.jobs.len(),
        makespan_h: end / 3600.0,
        utilisation: busy_window / (gpus * window),
        efficiency: useful / busy,
        mean_wait_s: waits.iter().sum::<f64>() / n,
        p50_wait_s: pct(0.5),
        p95_wait_s: pct(0.95),
        p99_wait_s: pct(0.99),
        mean_wait_high_prio_s: if hp_n > 0 { hp / hp_n as f64 } else { 0.0 },
        mean_jct_s: jct / n,
        mean_bounded_slowdown: bsd.values().map(|v| v.0).sum::<f64>() / n,
        frac_node_compact: node_ok as f64 / starts as f64,
        frac_island_compact: island_ok as f64 / island_eligible.max(1) as f64,
        preemptions,
        restarts_from_failures: failure_restarts,
        node_failures: failures,
        wait_heavy_tenant_s: heavy.2 / heavy.1 as f64,
        wait_light_tenants_s: light.0 / light.1 as f64,
        tenants: bsd
            .into_iter()
            .map(|(t, (_, k, w, g))| {
                (
                    t,
                    TenantStats {
                        jobs: k,
                        mean_wait_s: w / k as f64,
                        gpu_hours: g,
                    },
                )
            })
            .collect(),
        sched_passes: passes,
        sched_mean_us: sched_total / passes as f64,
        sched_max_us: sched_max,
    }
}
