use nebula::sched::{Order, PlacementPolicy, Policy};
use nebula::sim::{self, SimConfig};

fn cfg() -> SimConfig {
    SimConfig {
        nodes: 8,
        jobs: 600,
        load: 1.0,
        ..Default::default()
    }
}

const FIFO: Policy = Policy {
    order: Order::Fifo,
    backfill: false,
    placement: PlacementPolicy::Topology,
    preemption: false,
};

#[test]
fn simulation_completes_every_job_and_is_deterministic() {
    let mut c = cfg();
    c.node_mtbf_h = 20.0;
    c.repair_min = 10.0;
    let a = sim::run(&c, &Policy::default(), "default");
    let b = sim::run(&c, &Policy::default(), "default");
    assert_eq!(a.jobs, 600);
    assert!(a.node_failures > 0 && a.restarts_from_failures > 0, "{a:?}");
    assert!(a.utilisation > 0.3 && a.utilisation <= 1.0 + 1e-9, "{}", a.utilisation);
    assert!(a.efficiency > 0.0 && a.efficiency <= 1.0 + 1e-9);
    assert_eq!(
        serde_json::to_string(&a.tenants).unwrap(),
        serde_json::to_string(&b.tenants).unwrap()
    );
    assert_eq!(a.mean_wait_s, b.mean_wait_s);
}

#[test]
fn placement_factor_matches_the_model() {
    use nebula::model::Placement;
    let c = SimConfig::default();
    let p = |n: &str, g: &[u32]| Placement {
        node: n.into(),
        gpus: g.to_vec(),
    };
    assert_eq!(sim::placement_factor(&c, 4, &[p("a", &[0, 1, 2, 3])]), (1.0, true, true));
    assert_eq!(sim::placement_factor(&c, 4, &[p("a", &[2, 3, 4, 5])]), (1.1, true, false));
    assert_eq!(
        sim::placement_factor(&c, 4, &[p("a", &[0, 1]), p("b", &[0, 1])]),
        (1.3, false, false)
    );
    assert_eq!(
        sim::placement_factor(&c, 16, &[p("a", &(0..8).collect::<Vec<_>>()), p("b", &(0..8).collect::<Vec<_>>())]),
        (1.0, true, true)
    );
}

#[test]
fn policies_rank_as_expected_on_a_loaded_cluster() {
    let c = cfg();
    let fifo = sim::run(&c, &FIFO, "fifo");
    let easy = sim::run(&c, &Policy { backfill: true, ..FIFO }, "fifo+backfill");
    let ff = sim::run(
        &c,
        &Policy {
            backfill: true,
            placement: PlacementPolicy::FirstFit,
            ..FIFO
        },
        "first-fit",
    );
    let fs = sim::run(
        &c,
        &Policy {
            backfill: true,
            order: Order::FairShare,
            ..FIFO
        },
        "fairshare",
    );
    let pre = sim::run(&c, &Policy::default(), "preempt");
    assert!(
        easy.mean_wait_s < fifo.mean_wait_s,
        "backfill {} vs fifo {}",
        easy.mean_wait_s,
        fifo.mean_wait_s
    );
    assert!(easy.frac_island_compact > ff.frac_island_compact && easy.frac_node_compact > ff.frac_node_compact);
    assert!(
        fs.wait_light_tenants_s < easy.wait_light_tenants_s,
        "fair share favours light tenants"
    );
    assert!(pre.mean_wait_high_prio_s <= fs.mean_wait_high_prio_s);
    assert!(pre.preemptions > 0);
}
