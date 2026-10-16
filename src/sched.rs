//! The scheduler: pure, deterministic functions over a `Cluster` and the
//! current time. Used unchanged by the control plane and by the simulator.
//!
//! Each pass (`schedule`):
//! 1. orders queued jobs: priority (high first), then either submission time
//!    (`Order::Fifo`) or the tenant's weighted usage, GPUs held / weight
//!    (`Order::FairShare`, recomputed as jobs start in this pass), then
//!    submission time;
//! 2. skips jobs whose tenant would exceed its quota;
//! 3. starts every job it can place, in order. The first job that cannot
//!    be placed becomes the *head*:
//!    * with preemption, lower-priority preemptible jobs are evicted
//!      (lowest priority, then youngest, first) if that lets the head start;
//!    * otherwise, with backfill (EASY), the head gets a reservation at the
//!      "shadow time" when enough GPUs free up (from running jobs' estimated
//!      ends). Later jobs may start only if they fit now and either finish
//!      before the shadow time or use only GPUs the head will not need;
//!    * without backfill, scheduling stops at the head (strict order).
//!
//! Placement (`Placement` policy `Topology`): a job that fits on one node
//! goes to the node with the fewest free GPUs that still fits it (best fit),
//! and inside it to one NVLink island if possible (again best fit). Larger
//! jobs take whole free nodes first and best-fit the remainder, minimising
//! the number of nodes. `FirstFit` takes free GPUs in node/index order: the
//! topology-unaware baseline.

use crate::model::*;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Order {
    Fifo,
    FairShare,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlacementPolicy {
    Topology,
    FirstFit,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct Policy {
    pub order: Order,
    pub backfill: bool,
    pub placement: PlacementPolicy,
    pub preemption: bool,
}

impl Default for Policy {
    fn default() -> Self {
        Policy {
            order: Order::FairShare,
            backfill: true,
            placement: PlacementPolicy::Topology,
            preemption: true,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Decision {
    Start { job: JobId, placement: Vec<Placement> },
    Preempt { job: JobId },
}

#[derive(Debug, PartialEq)]
pub enum SubmitError {
    UnknownTenant,
    BadRequest(String),
    ExceedsCapacity,
}

// ------------------------------------------------------------------ lifecycle

pub fn submit(c: &mut Cluster, spec: JobSpec, now: f64) -> Result<JobId, SubmitError> {
    let t = c.tenants.get(&spec.tenant).ok_or(SubmitError::UnknownTenant)?;
    if spec.gpus == 0 || spec.gpus > 4096 {
        return Err(SubmitError::BadRequest("gpus must be 1..4096".into()));
    }
    if !(spec.est_runtime > 0.0 && spec.est_runtime.is_finite()) {
        return Err(SubmitError::BadRequest("est_runtime must be positive".into()));
    }
    if t.quota_gpus > 0 && spec.gpus > t.quota_gpus {
        return Err(SubmitError::BadRequest(format!(
            "job needs {} GPUs, tenant quota is {}",
            spec.gpus, t.quota_gpus
        )));
    }
    let capacity: u32 = c
        .nodes
        .values()
        .map(|n| {
            n.gpus
                .iter()
                .filter(|g| spec.gpu_model.as_deref().is_none_or(|m| g.model == m))
                .count() as u32
        })
        .sum();
    if spec.gpus > capacity {
        return Err(SubmitError::ExceedsCapacity);
    }
    c.next_job += 1;
    let id = c.next_job;
    c.jobs.insert(
        id,
        Job {
            id,
            spec,
            submitted: now,
            state: JobState::Queued,
            restarts: 0,
            gpu_seconds: 0.0,
            queued_since: now,
        },
    );
    Ok(id)
}

fn release(c: &mut Cluster, id: JobId, now: f64) -> Option<f64> {
    let job = c.jobs.get_mut(&id)?;
    let (start, placement) = match &job.state {
        JobState::Running { start, placement } => (*start, placement.clone()),
        _ => return None,
    };
    let n: usize = placement.iter().map(|p| p.gpus.len()).sum();
    job.gpu_seconds += n as f64 * (now - start).max(0.0);
    for p in &placement {
        if let Some(node) = c.nodes.get_mut(&p.node) {
            for g in &p.gpus {
                if node.used.get(g) == Some(&id) {
                    node.used.remove(g);
                }
            }
        }
    }
    Some(start)
}

/// A running job finished (reported by its node agent).
pub fn complete(c: &mut Cluster, id: JobId, now: f64, success: bool, reason: &str) -> bool {
    let Some(start) = release(c, id, now) else { return false };
    let j = c.jobs.get_mut(&id).unwrap();
    j.state = if success {
        JobState::Succeeded { start, end: now }
    } else {
        JobState::Failed {
            start,
            end: now,
            reason: reason.to_string(),
        }
    };
    true
}

pub fn cancel(c: &mut Cluster, id: JobId, now: f64) -> bool {
    let Some(j) = c.jobs.get(&id) else { return false };
    match j.state {
        JobState::Queued | JobState::Running { .. } => {
            release(c, id, now);
            c.jobs.get_mut(&id).unwrap().state = JobState::Cancelled { at: now };
            true
        }
        _ => false,
    }
}

/// Put a running job back in the queue (preemption or lost node).
pub fn requeue(c: &mut Cluster, id: JobId, now: f64) {
    if release(c, id, now).is_some() {
        let j = c.jobs.get_mut(&id).unwrap();
        j.state = JobState::Queued;
        j.restarts += 1;
        j.queued_since = now;
    }
}

/// Mark a node down and requeue everything that ran on it.
pub fn node_down(c: &mut Cluster, node: &str, now: f64) -> Vec<JobId> {
    let Some(n) = c.nodes.get_mut(node) else { return vec![] };
    n.state = NodeState::Down;
    let victims: BTreeSet<JobId> = n.used.values().copied().collect();
    for &j in &victims {
        requeue(c, j, now);
    }
    victims.into_iter().collect()
}

/// A GPU reported an unrecoverable error: take it out of service and
/// requeue the job using it.
pub fn gpu_failed(c: &mut Cluster, node: &str, gpu: u32, now: f64) -> Option<JobId> {
    let n = c.nodes.get_mut(node)?;
    let g = n.gpus.iter_mut().find(|g| g.index == gpu)?;
    g.healthy = false;
    let job = n.used.get(&gpu).copied();
    if let Some(j) = job {
        requeue(c, j, now);
    }
    job
}

// ------------------------------------------------------------------ placement

fn pick_in_node(node: &Node, k: usize, model: Option<&str>, topo: bool) -> Vec<u32> {
    let free: Vec<&Gpu> = node.free_gpus(model).collect();
    if !topo {
        return free.iter().take(k).map(|g| g.index).collect();
    }
    let mut islands: BTreeMap<u32, Vec<u32>> = BTreeMap::new();
    for g in &free {
        islands.entry(g.island).or_default().push(g.index);
    }
    // one island that fits, best fit
    if let Some((_, gs)) = islands
        .iter()
        .filter(|(_, gs)| gs.len() >= k)
        .min_by_key(|(id, gs)| (gs.len(), **id))
    {
        return gs[..k].to_vec();
    }
    // otherwise fewest islands: largest first
    let mut v: Vec<(u32, Vec<u32>)> = islands.into_iter().collect();
    v.sort_by(|a, b| b.1.len().cmp(&a.1.len()).then(a.0.cmp(&b.0)));
    let mut out = Vec::with_capacity(k);
    for (_, gs) in v {
        for g in gs {
            if out.len() < k {
                out.push(g);
            }
        }
    }
    out.sort_unstable();
    out
}

/// Find GPUs for a job without changing the cluster.
pub fn place(c: &Cluster, gpus: u32, model: Option<&str>, policy: PlacementPolicy) -> Option<Vec<Placement>> {
    place_on(&c.nodes, gpus, model, policy)
}

fn place_on(nodes: &BTreeMap<NodeId, Node>, gpus: u32, model: Option<&str>, policy: PlacementPolicy) -> Option<Vec<Placement>> {
    let k = gpus as usize;
    let nodes: Vec<&Node> = nodes.values().filter(|n| n.schedulable()).collect();
    let free = |n: &Node| n.free_gpus(model).count();
    let total: usize = nodes.iter().map(|n| free(n)).sum();
    if total < k {
        return None;
    }
    let topo = policy == PlacementPolicy::Topology;
    if !topo {
        let mut out = vec![];
        let mut need = k;
        for n in nodes {
            if need == 0 {
                break;
            }
            let g = pick_in_node(n, need.min(free(n)), model, false);
            if !g.is_empty() {
                need -= g.len();
                out.push(Placement {
                    node: n.id.clone(),
                    gpus: g,
                });
            }
        }
        return Some(out);
    }
    // single node, best fit
    if let Some(n) = nodes.iter().filter(|n| free(n) >= k).min_by_key(|n| (free(n), n.id.clone())) {
        return Some(vec![Placement {
            node: n.id.clone(),
            gpus: pick_in_node(n, k, model, true),
        }]);
    }
    // multi-node gang: whole free nodes first, then best fit for the rest
    let mut by_free: Vec<&Node> = nodes.iter().copied().filter(|n| free(n) > 0).collect();
    by_free.sort_by(|a, b| free(b).cmp(&free(a)).then(a.id.cmp(&b.id)));
    let mut out = vec![];
    let mut used: BTreeSet<&str> = BTreeSet::new();
    let mut need = k;
    while need > 0 {
        let rest: Vec<&&Node> = by_free.iter().filter(|n| !used.contains(n.id.as_str())).collect();
        let pick = rest
            .iter()
            .filter(|n| free(n) >= need)
            .min_by_key(|n| (free(n), n.id.clone()))
            .or_else(|| rest.first())?;
        let take = need.min(free(pick));
        used.insert(pick.id.as_str());
        out.push(Placement {
            node: pick.id.clone(),
            gpus: pick_in_node(pick, take, model, true),
        });
        need -= take;
    }
    out.sort();
    Some(out)
}

/// Allocate `placement` to a queued job (also used to replay the journal).
pub fn apply_start(c: &mut Cluster, id: JobId, placement: Vec<Placement>, now: f64) {
    for p in &placement {
        let n = c.nodes.get_mut(&p.node).expect("placement on a known node");
        for g in &p.gpus {
            let prev = n.used.insert(*g, id);
            assert!(prev.is_none(), "GPU {}/{} double-allocated", p.node, g);
        }
    }
    c.jobs.get_mut(&id).unwrap().state = JobState::Running { start: now, placement };
}

// ------------------------------------------------------------------ the pass

pub fn schedule(c: &mut Cluster, now: f64, policy: &Policy) -> Vec<Decision> {
    let mut decisions = vec![];
    let mut usage: BTreeMap<String, f64> = c.tenants.keys().map(|t| (t.clone(), c.gpus_in_use(t) as f64)).collect();
    let mut queue: Vec<JobId> = c.jobs.values().filter(|j| j.is_queued()).map(|j| j.id).collect();
    let mut reservation: Option<(f64, i64, Option<String>)> = None; // (shadow time, spare GPUs at shadow, model)
    let mut blocked_head = false;
    loop {
        // re-sort each round: fair-share keys change as jobs start
        let key = |c: &Cluster, usage: &BTreeMap<String, f64>, id: JobId| {
            let j = &c.jobs[&id];
            let share = match policy.order {
                Order::Fifo => 0.0,
                Order::FairShare => usage.get(&j.spec.tenant).copied().unwrap_or(0.0) / c.tenants[&j.spec.tenant].weight.max(1e-9),
            };
            (-(j.spec.priority as i64), ordered(share), ordered(j.queued_since), j.id)
        };
        queue.sort_by_cached_key(|&id| key(c, &usage, id));
        // free GPUs of any model: lets most unplaceable jobs skip `place`
        let free_any = |c: &Cluster| {
            c.nodes
                .values()
                .filter(|n| n.schedulable())
                .map(|n| n.free_gpus(None).count())
                .sum::<usize>()
        };
        let mut free_now = free_any(c);
        let mut progressed = false;
        let mut i = 0;
        while i < queue.len() {
            let id = queue[i];
            let (spec, tenant) = {
                let j = &c.jobs[&id];
                (j.spec.clone(), &c.tenants[&j.spec.tenant])
            };
            let held = usage[&spec.tenant] as u32;
            if tenant.quota_gpus > 0 && held + spec.gpus > tenant.quota_gpus {
                i += 1;
                continue; // over quota: does not block others
            }
            let fits = if spec.gpus as usize > free_now {
                None
            } else {
                place(c, spec.gpus, spec.gpu_model.as_deref(), policy.placement)
            };
            if let Some(p) = fits {
                let allowed = match &reservation {
                    None => true,
                    Some((shadow, spare, model)) => {
                        let same_pool = model.is_none() || spec.gpu_model.is_none() || model == &spec.gpu_model;
                        now + spec.est_runtime <= *shadow || !same_pool || (spec.gpus as i64) <= *spare
                    }
                };
                if allowed {
                    if let Some((shadow, spare, model)) = &mut reservation {
                        let same_pool = model.is_none() || spec.gpu_model.is_none() || *model == spec.gpu_model;
                        if same_pool && now + spec.est_runtime > *shadow {
                            *spare -= spec.gpus as i64;
                        }
                    }
                    apply_start(c, id, p.clone(), now);
                    free_now -= spec.gpus as usize;
                    *usage.get_mut(&spec.tenant).unwrap() += spec.gpus as f64;
                    decisions.push(Decision::Start { job: id, placement: p });
                    queue.remove(i);
                    progressed = true;
                    if policy.order == Order::FairShare && reservation.is_none() {
                        break; // re-sort with the new usage
                    }
                    continue;
                }
                i += 1;
                continue;
            }
            // cannot place: this is the head (if no reservation yet)
            if reservation.is_none() && !blocked_head {
                if policy.preemption
                    && let Some(victims) = preemption_set(c, &spec, policy.placement)
                {
                    for v in victims {
                        let t = c.jobs[&v].spec.tenant.clone();
                        let g = c.jobs[&v].spec.gpus as f64;
                        requeue(c, v, now);
                        *usage.get_mut(&t).unwrap() -= g;
                        decisions.push(Decision::Preempt { job: v });
                        queue.push(v);
                    }
                    progressed = true;
                    break; // retry: the head now fits
                }
                if !policy.backfill {
                    return decisions; // strict order
                }
                reservation = Some(shadow_time(c, &spec, now));
                blocked_head = true;
            }
            i += 1;
        }
        if !progressed || i >= queue.len() && reservation.is_some() {
            break;
        }
        if reservation.is_some() {
            break;
        }
    }
    decisions
}

fn ordered(x: f64) -> i64 {
    (x * 1e6).round() as i64
}

/// EASY backfill reservation for a blocked job: the earliest time at which
/// enough compatible GPUs are free (assuming running jobs end at
/// start + est_runtime), and how many spare GPUs remain then.
fn shadow_time(c: &Cluster, spec: &JobSpec, now: f64) -> (f64, i64, Option<String>) {
    let model = spec.gpu_model.as_deref();
    let mut free: i64 = c
        .nodes
        .values()
        .filter(|n| n.schedulable())
        .map(|n| n.free_gpus(model).count() as i64)
        .sum();
    let mut ends: Vec<(f64, i64)> = vec![];
    for j in c.jobs.values() {
        if let JobState::Running { start, placement } = &j.state {
            let n: i64 = placement
                .iter()
                .map(|p| {
                    let node = &c.nodes[&p.node];
                    p.gpus
                        .iter()
                        .filter(|g| node.gpus.iter().any(|x| x.index == **g && model.is_none_or(|m| x.model == m)))
                        .count() as i64
                })
                .sum();
            if n > 0 {
                ends.push(((start + j.spec.est_runtime).max(now), n));
            }
        }
    }
    ends.sort_by(|a, b| a.0.total_cmp(&b.0));
    let need = spec.gpus as i64;
    let mut shadow = now;
    for (t, n) in ends {
        if free >= need {
            break;
        }
        free += n;
        shadow = t;
    }
    (shadow, free - need, spec.gpu_model.clone())
}

/// Smallest prefix (by eviction order) of lower-priority preemptible jobs
/// whose removal lets `spec` be placed.
fn preemption_set(c: &Cluster, spec: &JobSpec, policy: PlacementPolicy) -> Option<Vec<JobId>> {
    let mut cands: Vec<&Job> = c
        .jobs
        .values()
        .filter(|j| j.spec.preemptible && j.spec.priority < spec.priority && j.running_placement().is_some())
        .collect();
    if cands.is_empty() {
        return None;
    }
    cands.sort_by(|a, b| {
        let sa = match a.state {
            JobState::Running { start, .. } => start,
            _ => 0.0,
        };
        let sb = match b.state {
            JobState::Running { start, .. } => start,
            _ => 0.0,
        };
        a.spec.priority.cmp(&b.spec.priority).then(sb.total_cmp(&sa)).then(b.id.cmp(&a.id))
    });
    // trial placements only need the node maps: free the victims' GPUs there
    let without = |victims: &[JobId]| {
        let mut nodes = c.nodes.clone();
        for v in victims {
            for p in c.jobs[v].running_placement().unwrap() {
                if let Some(n) = nodes.get_mut(&p.node) {
                    n.used.retain(|_, j| j != v);
                }
            }
        }
        nodes
    };
    let mut chosen = vec![];
    for v in cands {
        chosen.push(v.id);
        if place_on(&without(&chosen), spec.gpus, spec.gpu_model.as_deref(), policy).is_some() {
            // drop victims that turned out unnecessary (last-in first)
            let mut minimal = chosen.clone();
            for &x in chosen.iter().rev() {
                let fewer: Vec<JobId> = minimal.iter().copied().filter(|&y| y != x).collect();
                if place_on(&without(&fewer), spec.gpus, spec.gpu_model.as_deref(), policy).is_some() {
                    minimal = fewer;
                }
            }
            return Some(minimal);
        }
    }
    None
}

// ------------------------------------------------------------------ checks

/// Consistency of allocations; returns a description of the first violation.
pub fn check_invariants(c: &Cluster) -> Result<(), String> {
    let mut seen: BTreeMap<(String, u32), JobId> = BTreeMap::new();
    for j in c.jobs.values() {
        if let JobState::Running { placement, .. } = &j.state {
            let n: usize = placement.iter().map(|p| p.gpus.len()).sum();
            if n != j.spec.gpus as usize {
                return Err(format!("job {} holds {} GPUs, asked for {}", j.id, n, j.spec.gpus));
            }
            for p in placement {
                let node = c.nodes.get(&p.node).ok_or(format!("job {} on unknown node", j.id))?;
                for g in &p.gpus {
                    let gpu = node.gpus.iter().find(|x| x.index == *g).ok_or("unknown GPU")?;
                    if j.spec.gpu_model.as_deref().is_some_and(|m| gpu.model != m) {
                        return Err(format!("job {} got GPU model {}", j.id, gpu.model));
                    }
                    if let Some(o) = seen.insert((p.node.clone(), *g), j.id) {
                        return Err(format!("GPU {}/{} shared by jobs {} and {}", p.node, g, o, j.id));
                    }
                    if node.used.get(g) != Some(&j.id) {
                        return Err(format!("node map disagrees for {}/{}", p.node, g));
                    }
                }
            }
        }
    }
    for n in c.nodes.values() {
        for (g, j) in &n.used {
            if seen.get(&(n.id.clone(), *g)) != Some(j) {
                return Err(format!("stale allocation {}/{} -> {}", n.id, g, j));
            }
        }
    }
    for t in c.tenants.values() {
        if t.quota_gpus > 0 && c.gpus_in_use(&t.name) > t.quota_gpus {
            return Err(format!("tenant {} over quota", t.name));
        }
    }
    Ok(())
}
