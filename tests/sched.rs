use nebula::model::*;
use nebula::sched::*;
use proptest::prelude::*;

fn node(id: &str, gpus: u32, islands: u32, model: &str) -> Node {
    let per = gpus / islands;
    Node {
        id: id.into(),
        gpus: (0..gpus)
            .map(|i| Gpu {
                index: i,
                model: model.into(),
                island: i / per,
                healthy: true,
                memory_mb: None,
                uuid: None,
            })
            .collect(),
        state: NodeState::Up,
        used: Default::default(),
        last_heartbeat: 0.0,
    }
}

fn cluster(nodes: &[(&str, u32)]) -> Cluster {
    let mut c = Cluster::default();
    for (id, g) in nodes {
        c.nodes.insert(id.to_string(), node(id, *g, 2.min(*g), "h100"));
    }
    for (t, w) in [("a", 1.0), ("b", 1.0)] {
        c.tenants.insert(
            t.into(),
            Tenant {
                name: t.into(),
                quota_gpus: 0,
                weight: w,
            },
        );
    }
    c
}

fn spec(tenant: &str, gpus: u32, est: f64) -> JobSpec {
    JobSpec {
        tenant: tenant.into(),
        gpus,
        gpu_model: None,
        priority: 0,
        preemptible: true,
        est_runtime: est,
        command: vec![],
        name: String::new(),
        image: None,
        cpus: None,
        memory_mb: None,
    }
}

fn prio(mut s: JobSpec, p: i32) -> JobSpec {
    s.priority = p;
    s
}

fn running(c: &Cluster, id: JobId) -> Option<Vec<Placement>> {
    c.jobs[&id].running_placement().cloned()
}

const FIFO: Policy = Policy {
    order: Order::Fifo,
    backfill: false,
    placement: PlacementPolicy::Topology,
    preemption: false,
};

#[test]
fn submit_validation() {
    let mut c = cluster(&[("n1", 8)]);
    assert_eq!(submit(&mut c, spec("zz", 1, 1.0), 0.0), Err(SubmitError::UnknownTenant));
    assert!(matches!(submit(&mut c, spec("a", 0, 1.0), 0.0), Err(SubmitError::BadRequest(_))));
    assert!(matches!(submit(&mut c, spec("a", 1, 0.0), 0.0), Err(SubmitError::BadRequest(_))));
    assert!(matches!(
        submit(&mut c, spec("a", 1, f64::NAN), 0.0),
        Err(SubmitError::BadRequest(_))
    ));
    assert_eq!(submit(&mut c, spec("a", 9, 1.0), 0.0), Err(SubmitError::ExceedsCapacity));
    let mut s = spec("a", 1, 1.0);
    s.gpu_model = Some("a100".into());
    assert_eq!(submit(&mut c, s, 0.0), Err(SubmitError::ExceedsCapacity));
    c.tenants.get_mut("a").unwrap().quota_gpus = 4;
    assert!(matches!(submit(&mut c, spec("a", 5, 1.0), 0.0), Err(SubmitError::BadRequest(_))));
    assert_eq!(submit(&mut c, spec("a", 4, 1.0), 0.0), Ok(1));
}

#[test]
fn small_job_stays_inside_one_nvlink_island() {
    // 8 GPUs, islands {0..3} and {4..7}; island 0 has one GPU busy.
    let mut c = cluster(&[("n1", 8)]);
    let j0 = submit(&mut c, spec("a", 1, 10.0), 0.0).unwrap();
    schedule(&mut c, 0.0, &Policy::default());
    assert_eq!(running(&c, j0).unwrap()[0].gpus, vec![0]);
    // 3 GPUs: best-fit island is island 0 (3 free) rather than island 1 (4 free)
    let j1 = submit(&mut c, spec("a", 3, 10.0), 0.0).unwrap();
    // 4 GPUs: must take the whole of island 1
    let j2 = submit(&mut c, spec("a", 4, 10.0), 0.0).unwrap();
    schedule(&mut c, 0.0, &FIFO);
    assert_eq!(running(&c, j1).unwrap()[0].gpus, vec![1, 2, 3]);
    assert_eq!(running(&c, j2).unwrap()[0].gpus, vec![4, 5, 6, 7]);
    check_invariants(&c).unwrap();
}

#[test]
fn topology_avoids_splitting_where_first_fit_splits() {
    for (policy, want_nodes) in [(PlacementPolicy::Topology, 1), (PlacementPolicy::FirstFit, 2)] {
        let mut c = cluster(&[("n1", 8), ("n2", 8)]);
        let filler = submit(&mut c, spec("a", 5, 100.0), 0.0).unwrap();
        let p = Policy { placement: policy, ..FIFO };
        schedule(&mut c, 0.0, &p);
        assert_eq!(running(&c, filler).unwrap()[0].node, "n1");
        let j = submit(&mut c, spec("a", 4, 10.0), 0.0).unwrap();
        schedule(&mut c, 0.0, &p);
        let pl = running(&c, j).unwrap();
        assert_eq!(pl.len(), want_nodes, "{policy:?}: {pl:?}");
        if policy == PlacementPolicy::Topology {
            assert_eq!(
                pl[0],
                Placement {
                    node: "n2".into(),
                    gpus: vec![0, 1, 2, 3]
                }
            );
        }
        check_invariants(&c).unwrap();
    }
}

#[test]
fn single_node_best_fit_keeps_whole_nodes_free() {
    let mut c = cluster(&[("n1", 8), ("n2", 8), ("n3", 8)]);
    let a = submit(&mut c, spec("a", 6, 100.0), 0.0).unwrap();
    schedule(&mut c, 0.0, &FIFO);
    let n_a = running(&c, a).unwrap()[0].node.clone();
    // a 2-GPU job goes to the partly used node, leaving two empty nodes for big jobs
    let b = submit(&mut c, spec("a", 2, 100.0), 0.0).unwrap();
    schedule(&mut c, 0.0, &FIFO);
    assert_eq!(running(&c, b).unwrap()[0].node, n_a);
}

#[test]
fn gang_job_takes_whole_nodes_and_is_all_or_nothing() {
    let mut c = cluster(&[("n1", 8), ("n2", 8), ("n3", 8)]);
    let small = submit(&mut c, spec("a", 3, 100.0), 0.0).unwrap();
    schedule(&mut c, 0.0, &FIFO);
    let small_node = running(&c, small).unwrap()[0].node.clone();
    let big = submit(&mut c, spec("a", 12, 100.0), 0.0).unwrap();
    schedule(&mut c, 0.0, &FIFO);
    let pl = running(&c, big).unwrap();
    assert_eq!(pl.len(), 2);
    let sizes: Vec<usize> = pl.iter().map(|p| p.gpus.len()).collect();
    assert!(sizes.contains(&8) && sizes.contains(&4), "{pl:?}");
    // the 4-GPU remainder is best-fit onto the partly used node, in one island
    let rest = pl.iter().find(|p| p.gpus.len() == 4).unwrap();
    assert_eq!(rest.node, small_node);
    // a 10-GPU job does not fit in the remaining 9: nothing is allocated for it
    let huge = submit(&mut c, spec("a", 10, 100.0), 0.0).unwrap();
    let before = c.used_gpus();
    schedule(&mut c, 0.0, &FIFO);
    assert!(c.jobs[&huge].is_queued());
    assert_eq!(c.used_gpus(), before);
    check_invariants(&c).unwrap();
}

#[test]
fn strict_fifo_blocks_behind_the_head_but_backfill_does_not_delay_it() {
    // 8 GPUs. A holds 6 until t=100. Head B needs 8 -> reservation at t=100.
    let setup = || {
        let mut c = cluster(&[("n1", 8)]);
        let a = submit(&mut c, spec("a", 6, 100.0), 0.0).unwrap();
        schedule(&mut c, 0.0, &FIFO);
        let b = submit(&mut c, spec("a", 8, 50.0), 1.0).unwrap();
        let short = submit(&mut c, spec("b", 2, 50.0), 2.0).unwrap(); // ends at 60 < 100
        let long = submit(&mut c, spec("b", 2, 500.0), 3.0).unwrap(); // would delay B
        (c, a, b, short, long)
    };
    let (mut c, _, b, short, long) = setup();
    schedule(&mut c, 10.0, &FIFO);
    assert!(c.jobs[&short].is_queued() && c.jobs[&long].is_queued() && c.jobs[&b].is_queued());

    let (mut c, a, b, short, long) = setup();
    let easy = Policy {
        order: Order::Fifo,
        backfill: true,
        placement: PlacementPolicy::Topology,
        preemption: false,
    };
    schedule(&mut c, 10.0, &easy);
    assert!(running(&c, short).is_some(), "short job backfilled");
    assert!(c.jobs[&long].is_queued(), "long job would delay the head");
    // the head starts as soon as its reservation time comes
    complete(&mut c, short, 60.0, true, "");
    schedule(&mut c, 60.0, &easy);
    assert!(c.jobs[&b].is_queued());
    complete(&mut c, a, 100.0, true, "");
    schedule(&mut c, 100.0, &easy);
    assert!(running(&c, b).is_some());
    check_invariants(&c).unwrap();
}

#[test]
fn backfill_may_use_gpus_the_head_does_not_need() {
    // 16 GPUs; A holds 12 until t=100; head needs 10 -> at t=100, 16 free, 6 spare.
    let mut c = cluster(&[("n1", 8), ("n2", 8)]);
    let _a = submit(&mut c, spec("a", 12, 100.0), 0.0).unwrap();
    schedule(&mut c, 0.0, &FIFO);
    let head = submit(&mut c, spec("a", 10, 10.0), 1.0).unwrap();
    let long = submit(&mut c, spec("b", 4, 1000.0), 2.0).unwrap(); // 4 <= 6 spare
    let long2 = submit(&mut c, spec("b", 4, 1000.0), 3.0).unwrap(); // no GPUs left now anyway
    let easy = Policy {
        order: Order::Fifo,
        backfill: true,
        placement: PlacementPolicy::Topology,
        preemption: false,
    };
    schedule(&mut c, 5.0, &easy);
    assert!(c.jobs[&head].is_queued());
    assert!(running(&c, long).is_some());
    assert!(c.jobs[&long2].is_queued());
}

#[test]
fn preemption_evicts_a_minimal_set_of_lower_priority_jobs() {
    let mut c = cluster(&[("n1", 8)]);
    let low_old = submit(&mut c, spec("a", 4, 100.0), 0.0).unwrap();
    schedule(&mut c, 0.0, &FIFO);
    let low_young = submit(&mut c, spec("a", 2, 100.0), 1.0).unwrap();
    schedule(&mut c, 1.0, &FIFO);
    let mut s = spec("a", 2, 100.0);
    s.preemptible = false;
    let pinned = submit(&mut c, s, 2.0).unwrap();
    schedule(&mut c, 2.0, &FIFO);
    assert_eq!(c.used_gpus(), 8);

    // equal priority never preempts
    let same = submit(&mut c, spec("b", 2, 10.0), 3.0).unwrap();
    let ds = schedule(&mut c, 3.0, &Policy::default());
    assert!(ds.is_empty() && c.jobs[&same].is_queued());
    cancel(&mut c, same, 3.0);

    // a 2-GPU high-priority job evicts only the youngest low job
    let hi = submit(&mut c, prio(spec("b", 2, 10.0), 5), 4.0).unwrap();
    let ds = schedule(&mut c, 4.0, &Policy::default());
    assert_eq!(ds[0], Decision::Preempt { job: low_young });
    assert!(running(&c, hi).is_some());
    assert!(running(&c, low_old).is_some() && running(&c, pinned).is_some());
    assert_eq!(c.jobs[&low_young].restarts, 1);
    check_invariants(&c).unwrap();

    // a 6-GPU high-priority job cannot be helped (pinned + hi hold 4): nothing is evicted
    let hi2 = submit(&mut c, prio(spec("b", 6, 10.0), 5), 5.0).unwrap();
    let ds = schedule(&mut c, 5.0, &Policy::default());
    assert!(!ds.iter().any(|d| matches!(d, Decision::Preempt { .. })), "{ds:?}");
    assert!(c.jobs[&hi2].is_queued());
}

#[test]
fn preemption_set_is_minimal_not_just_a_prefix() {
    // n1: m (8 GPUs, older). n2: pinned (6, not preemptible) + y (2, youngest).
    // Eviction order is y, m; the prefix [y, m] frees enough for an 8-GPU job,
    // but evicting y does not help: only m is preempted.
    let mut c = cluster(&[("n1", 8), ("n2", 8)]);
    let m = submit(&mut c, spec("a", 8, 100.0), 0.0).unwrap();
    let mut s = spec("a", 6, 100.0);
    s.preemptible = false;
    let pinned = submit(&mut c, s, 1.0).unwrap();
    let y = submit(&mut c, spec("a", 2, 100.0), 2.0).unwrap();
    schedule(&mut c, 2.0, &FIFO);
    assert_eq!(c.used_gpus(), 16);
    let hi = submit(&mut c, prio(spec("b", 8, 10.0), 1), 3.0).unwrap();
    let ds = schedule(&mut c, 3.0, &Policy::default());
    let pre: Vec<JobId> = ds
        .iter()
        .filter_map(|d| if let Decision::Preempt { job } = d { Some(*job) } else { None })
        .collect();
    assert_eq!(pre, vec![m], "{ds:?}");
    assert!(running(&c, y).is_some() && running(&c, pinned).is_some() && running(&c, hi).is_some());
    check_invariants(&c).unwrap();
}

#[test]
fn quota_skips_without_blocking_others() {
    let mut c = cluster(&[("n1", 8)]);
    c.tenants.get_mut("a").unwrap().quota_gpus = 4;
    let a1 = submit(&mut c, spec("a", 4, 10.0), 0.0).unwrap();
    let a2 = submit(&mut c, spec("a", 2, 10.0), 1.0).unwrap();
    let b1 = submit(&mut c, spec("b", 2, 10.0), 2.0).unwrap();
    schedule(&mut c, 2.0, &FIFO);
    assert!(running(&c, a1).is_some() && c.jobs[&a2].is_queued() && running(&c, b1).is_some());
    complete(&mut c, a1, 5.0, true, "");
    schedule(&mut c, 5.0, &FIFO);
    assert!(running(&c, a2).is_some());
    check_invariants(&c).unwrap();
}

#[test]
fn fair_share_serves_the_tenant_with_less_usage_first() {
    let mut c = cluster(&[("n1", 8)]);
    let a0 = submit(&mut c, spec("a", 6, 100.0), 0.0).unwrap();
    schedule(&mut c, 0.0, &FIFO);
    assert!(running(&c, a0).is_some());
    let a1 = submit(&mut c, spec("a", 2, 10.0), 1.0).unwrap(); // older
    let b1 = submit(&mut c, spec("b", 2, 10.0), 2.0).unwrap();
    let fifo = schedule(&mut c.clone(), 3.0, &FIFO);
    assert_eq!(
        fifo,
        vec![Decision::Start {
            job: a1,
            placement: fifo_placement(&fifo)
        }]
    );
    let fs = Policy {
        order: Order::FairShare,
        ..FIFO
    };
    let ds = schedule(&mut c, 3.0, &fs);
    assert!(matches!(&ds[0], Decision::Start { job, .. } if *job == b1));
    // weights: b with weight 4 may hold more before a overtakes it
    let mut c = cluster(&[("n1", 8)]);
    c.tenants.get_mut("b").unwrap().weight = 4.0;
    for t in 0..4 {
        submit(&mut c, spec("a", 1, 10.0), t as f64).unwrap();
        submit(&mut c, spec("b", 1, 10.0), t as f64 + 0.5).unwrap();
    }
    schedule(&mut c, 10.0, &fs);
    // 8 GPUs, a has weight 1, b weight 4: shares equalise at roughly a=2, b=6 but b only has 4 jobs
    assert_eq!(c.gpus_in_use("b"), 4);
    assert_eq!(c.gpus_in_use("a"), 4);
    let mut c = cluster(&[("n1", 4)]);
    c.tenants.get_mut("b").unwrap().weight = 3.0;
    for t in 0..4 {
        submit(&mut c, spec("a", 1, 10.0), t as f64).unwrap();
        submit(&mut c, spec("b", 1, 10.0), t as f64 + 0.5).unwrap();
    }
    schedule(&mut c, 10.0, &fs);
    assert_eq!((c.gpus_in_use("a"), c.gpus_in_use("b")), (1, 3));
}

fn fifo_placement(ds: &[Decision]) -> Vec<Placement> {
    match &ds[0] {
        Decision::Start { placement, .. } => placement.clone(),
        _ => panic!(),
    }
}

#[test]
fn node_failure_and_gpu_failure_requeue_jobs() {
    let mut c = cluster(&[("n1", 8), ("n2", 8)]);
    let j = submit(&mut c, spec("a", 12, 100.0), 0.0).unwrap();
    let k = submit(&mut c, spec("a", 2, 100.0), 0.0).unwrap();
    schedule(&mut c, 0.0, &FIFO);
    assert!(running(&c, j).is_some() && running(&c, k).is_some());
    let lost = node_down(&mut c, "n1", 10.0);
    // the gang job lost part of its GPUs: the whole job is requeued and its
    // GPUs on the surviving node are released too
    assert!(lost.contains(&j));
    assert!(c.jobs[&j].is_queued() && c.jobs[&j].restarts == 1);
    check_invariants(&c).unwrap();
    schedule(&mut c, 10.0, &FIFO);
    assert!(c.jobs[&j].is_queued(), "12 GPUs do not fit on one node");
    c.nodes.get_mut("n1").unwrap().state = NodeState::Up;
    schedule(&mut c, 11.0, &FIFO);
    let pl = running(&c, j).unwrap();
    // GPU failure: the job using it is requeued, the GPU is never used again
    let (n, g) = (pl[0].node.clone(), pl[0].gpus[0]);
    assert_eq!(gpu_failed(&mut c, &n, g, 20.0), Some(j));
    assert!(c.jobs[&j].is_queued() && c.jobs[&j].restarts == 2);
    schedule(&mut c, 20.0, &FIFO);
    for job in c.jobs.values() {
        for p in job.running_placement().into_iter().flatten() {
            assert!(!(p.node == n && p.gpus.contains(&g)));
        }
    }
    check_invariants(&c).unwrap();
}

#[test]
fn metering_counts_gpu_seconds_across_attempts() {
    let mut c = cluster(&[("n1", 8)]);
    let j = submit(&mut c, spec("a", 4, 100.0), 0.0).unwrap();
    schedule(&mut c, 10.0, &FIFO);
    requeue(&mut c, j, 30.0); // 4 GPUs x 20 s
    schedule(&mut c, 40.0, &FIFO);
    complete(&mut c, j, 45.5, true, ""); // 4 x 5.5 s
    assert_eq!(c.jobs[&j].gpu_seconds, 102.0);
    assert!(matches!(c.jobs[&j].state, JobState::Succeeded { start, end } if start == 40.0 && end == 45.5));
    assert!(!complete(&mut c, j, 50.0, true, ""), "completing twice is a no-op");
    assert_eq!(c.used_gpus(), 0);
}

#[test]
fn gpu_model_constraints() {
    let mut c = cluster(&[("n1", 8)]);
    c.nodes.insert("m1".into(), node("m1", 4, 1, "a100"));
    let mut s = spec("a", 4, 10.0);
    s.gpu_model = Some("a100".into());
    let j = submit(&mut c, s.clone(), 0.0).unwrap();
    let j2 = submit(&mut c, s, 0.0).unwrap();
    schedule(&mut c, 0.0, &Policy::default());
    assert_eq!(running(&c, j).unwrap()[0].node, "m1");
    assert!(c.jobs[&j2].is_queued(), "no a100 left; h100s are not substituted");
    check_invariants(&c).unwrap();
}

// ------------------------------------------------------------------ properties

#[derive(Clone, Debug)]
enum Op {
    Submit {
        tenant: u8,
        gpus: u32,
        prio: i32,
        preemptible: bool,
        est: u16,
    },
    Complete(u16),
    Cancel(u16),
    NodeDown(u8),
    NodeUp(u8),
    GpuFail(u8, u8),
    Schedule(u8),
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (0u8..3, prop_oneof![Just(1u32), Just(2), Just(3), Just(4), Just(8), Just(12), Just(16)], 0i32..3, any::<bool>(), 1u16..500)
            .prop_map(|(tenant, gpus, prio, preemptible, est)| Op::Submit { tenant, gpus, prio, preemptible, est }),
        2 => any::<u16>().prop_map(Op::Complete),
        1 => any::<u16>().prop_map(Op::Cancel),
        1 => (0u8..4).prop_map(Op::NodeDown),
        1 => (0u8..4).prop_map(Op::NodeUp),
        1 => (0u8..4, 0u8..8).prop_map(|(n, g)| Op::GpuFail(n, g)),
        4 => (0u8..16).prop_map(Op::Schedule),
    ]
}

fn policy_from(bits: u8) -> Policy {
    Policy {
        order: if bits & 1 == 0 { Order::Fifo } else { Order::FairShare },
        backfill: bits & 2 != 0,
        placement: if bits & 4 == 0 {
            PlacementPolicy::Topology
        } else {
            PlacementPolicy::FirstFit
        },
        preemption: bits & 8 != 0,
    }
}

fn extra_checks(c: &Cluster) -> Result<(), String> {
    for j in c.jobs.values() {
        for p in j.running_placement().into_iter().flatten() {
            let n = &c.nodes[&p.node];
            if n.state == NodeState::Down {
                return Err(format!("job {} on down node {}", j.id, n.id));
            }
            for g in &p.gpus {
                if !n.gpus.iter().any(|x| x.index == *g && x.healthy) {
                    return Err(format!("job {} on failed GPU {}/{}", j.id, n.id, g));
                }
            }
        }
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(300))]

    /// Allocation invariants hold after every operation under every policy,
    /// and once all hardware is healthy every job eventually runs.
    #[test]
    fn invariants_and_eventual_progress(ops in proptest::collection::vec(op(), 1..120)) {
        let mut c = Cluster::default();
        for i in 0..4 {
            let id = format!("n{i}");
            c.nodes.insert(id.clone(), node(&id, 8, 2, "h100"));
        }
        for (t, q, w) in [("t0", 0, 1.0), ("t1", 16, 2.0), ("t2", 8, 1.0)] {
            c.tenants.insert(t.into(), Tenant { name: t.into(), quota_gpus: q, weight: w });
        }
        let mut now = 0.0;
        for o in ops {
            now += 1.0;
            match o {
                Op::Submit { tenant, gpus, prio, preemptible, est } => {
                    let mut s = spec(&format!("t{tenant}"), gpus, est as f64);
                    s.priority = prio;
                    s.preemptible = preemptible;
                    let _ = submit(&mut c, s, now);
                }
                Op::Complete(k) | Op::Cancel(k) => {
                    let run: Vec<JobId> = c.jobs.values().filter(|j| j.running_placement().is_some()).map(|j| j.id).collect();
                    if !run.is_empty() {
                        let id = run[k as usize % run.len()];
                        if matches!(o, Op::Complete(_)) { complete(&mut c, id, now, k % 3 != 0, "exit 1"); } else { cancel(&mut c, id, now); }
                    }
                }
                Op::NodeDown(n) => { node_down(&mut c, &format!("n{n}"), now); }
                Op::NodeUp(n) => { c.nodes.get_mut(&format!("n{n}")).unwrap().state = NodeState::Up; }
                Op::GpuFail(n, g) => { gpu_failed(&mut c, &format!("n{n}"), g as u32, now); }
                Op::Schedule(bits) => {
                    let before_used = c.used_gpus();
                    let ds = schedule(&mut c, now, &policy_from(bits));
                    let starts: u32 = ds.iter().map(|d| match d { Decision::Start { placement, .. } => placement.iter().map(|p| p.gpus.len() as u32).sum(), _ => 0 }).sum();
                    prop_assert!(c.used_gpus() <= before_used + starts);
                }
            }
            prop_assert_eq!(check_invariants(&c), Ok(()));
            prop_assert_eq!(extra_checks(&c), Ok(()));
            prop_assert!(c.used_gpus() <= 32);
        }
        // repair everything; then every queued job must finish within a bounded number of passes
        for n in c.nodes.values_mut() {
            n.state = NodeState::Up;
            for g in &mut n.gpus { g.healthy = true; }
        }
        for bits in [0u8, 3, 11, 15, 7] {
            let mut c = c.clone();
            let mut t = now;
            for _ in 0..500 {
                t += 1.0;
                schedule(&mut c, t, &policy_from(bits));
                prop_assert_eq!(check_invariants(&c), Ok(()));
                let run: Vec<JobId> = c.jobs.values().filter(|j| j.running_placement().is_some()).map(|j| j.id).collect();
                for id in run { complete(&mut c, id, t + 0.5, true, ""); }
                if !c.jobs.values().any(|j| j.is_queued()) { break; }
            }
            prop_assert!(!c.jobs.values().any(|j| j.is_queued()), "policy {:?} left jobs queued", policy_from(bits));
        }
    }

    /// Placement never splits a job that fits on one node (topology policy),
    /// and stays inside one island when one island can hold it.
    #[test]
    fn topology_placement_is_compact(busy in proptest::collection::vec(proptest::collection::vec(any::<bool>(), 8), 3), k in 1u32..9) {
        let mut c = Cluster::default();
        for (i, b) in busy.iter().enumerate() {
            let id = format!("n{i}");
            let mut n = node(&id, 8, 2, "h100");
            for (g, &x) in b.iter().enumerate() { if x { n.used.insert(g as u32, 999); } }
            c.nodes.insert(id, n);
        }
        let Some(pl) = place(&c, k, None, PlacementPolicy::Topology) else {
            prop_assert!(c.nodes.values().map(|n| n.free_gpus(None).count() as u32).sum::<u32>() < k);
            return Ok(());
        };
        prop_assert_eq!(pl.iter().map(|p| p.gpus.len() as u32).sum::<u32>(), k);
        let fits_one_node = c.nodes.values().any(|n| n.free_gpus(None).count() as u32 >= k);
        if fits_one_node {
            prop_assert_eq!(pl.len(), 1);
            let n = &c.nodes[&pl[0].node];
            let fits_island = (0..2).any(|is| n.free_gpus(None).filter(|g| g.island == is).count() as u32 >= k);
            if fits_island {
                let islands: std::collections::BTreeSet<u32> = pl[0].gpus.iter().map(|g| g / 4).collect();
                prop_assert_eq!(islands.len(), 1);
            }
        }
        for p in &pl {
            for g in &p.gpus { prop_assert!(!c.nodes[&p.node].used.contains_key(g)); }
        }
    }
}
