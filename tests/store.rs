use nebula::model::*;
use nebula::sched::{self, Policy};
use nebula::store::{self, Cmd, Store};
use proptest::prelude::*;

fn node(id: &str) -> Node {
    Node {
        id: id.into(),
        gpus: (0..8)
            .map(|i| Gpu {
                index: i,
                model: "h100".into(),
                island: i / 4,
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

fn json(c: &Cluster) -> serde_json::Value {
    serde_json::to_value(c).unwrap()
}

/// Drive the state the way the server does: commands through `apply`,
/// scheduling passes journaled as their decisions. Returns the cluster after
/// each journaled record.
fn drive(st: &mut Store, c: &mut Cluster, ops: &[(u8, u16)]) -> Vec<serde_json::Value> {
    let mut states = vec![];
    let exec = |st: &mut Store, c: &mut Cluster, cmd: Cmd, states: &mut Vec<serde_json::Value>| {
        let r = store::apply(c, &cmd);
        if !matches!(r, store::Applied::Rejected(_) | store::Applied::NoOp) {
            st.append(&cmd).unwrap();
            if st.needs_compaction() {
                st.compact(c).unwrap();
            }
            states.push(json(c));
        }
    };
    exec(
        st,
        c,
        Cmd::AddTenant {
            tenant: Tenant {
                name: "a".into(),
                quota_gpus: 0,
                weight: 1.0,
            },
        },
        &mut states,
    );
    exec(
        st,
        c,
        Cmd::AddTenant {
            tenant: Tenant {
                name: "b".into(),
                quota_gpus: 12,
                weight: 2.0,
            },
        },
        &mut states,
    );
    for n in ["n0", "n1", "n2"] {
        exec(st, c, Cmd::Register { node: node(n), now: 0.0 }, &mut states);
    }
    let mut now = 0.0;
    for &(op, x) in ops {
        now += 1.5;
        let running: Vec<JobId> = c.jobs.values().filter(|j| j.running_placement().is_some()).map(|j| j.id).collect();
        let pick = |v: &[JobId]| v.get(x as usize % v.len().max(1)).copied();
        let n = format!("n{}", x % 3);
        let cmd = match op % 9 {
            0 | 1 => Cmd::Submit {
                spec: JobSpec {
                    tenant: if x % 2 == 0 { "a" } else { "b" }.into(),
                    gpus: [1, 2, 4, 8, 12][x as usize % 5],
                    gpu_model: None,
                    priority: (x % 3) as i32,
                    preemptible: x % 7 != 0,
                    est_runtime: 10.0 + x as f64,
                    command: vec!["true".into()],
                    name: format!("j{x}"),
                    image: None,
                    cpus: None,
                    memory_mb: None,
                },
                now,
            },
            2 => match pick(&running) {
                Some(job) => Cmd::Complete {
                    job,
                    success: x % 4 != 0,
                    reason: "exit".into(),
                    now,
                },
                None => continue,
            },
            3 => match pick(&c.jobs.keys().copied().collect::<Vec<_>>()) {
                Some(job) => Cmd::Cancel { job, now },
                None => continue,
            },
            4 => Cmd::NodeDown { node: n, now },
            5 => Cmd::NodeUp { node: n },
            6 => Cmd::GpuFailed {
                node: n,
                gpu: (x % 8) as u32,
                now,
            },
            7 => Cmd::Drain {
                node: n,
                drain: x % 2 == 0,
            },
            _ => Cmd::Register { node: node(&n), now },
        };
        exec(st, c, cmd, &mut states);
        let ds = sched::schedule(c, now, &Policy::default());
        if !ds.is_empty() {
            st.append(&Cmd::Decisions { decisions: ds, now }).unwrap();
            if st.needs_compaction() {
                st.compact(c).unwrap();
            }
            states.push(json(c));
        }
        sched::check_invariants(c).unwrap();
    }
    states
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// Recovery reproduces the live state exactly, with and without compaction.
    #[test]
    fn replay_reproduces_live_state(ops in proptest::collection::vec((any::<u8>(), any::<u16>()), 1..80), compact_every in 0u64..20) {
        let dir = tempfile::tempdir().unwrap();
        let (mut st, rec) = Store::open(dir.path()).unwrap();
        st.compact_every = if compact_every == 0 { u64::MAX } else { compact_every };
        let mut c = rec.cluster;
        let states = drive(&mut st, &mut c, &ops);
        drop(st);
        let (_, rec) = Store::open(dir.path()).unwrap();
        prop_assert_eq!(json(&rec.cluster), states.last().cloned().unwrap());
    }
}

#[test]
fn crash_at_every_byte_recovers_a_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let (mut st, rec) = Store::open(dir.path()).unwrap();
    let mut c = rec.cluster;
    let ops: Vec<(u8, u16)> = (0..40u16).map(|i| ((i * 7 % 9) as u8, i * 13 + 1)).collect();
    let states = drive(&mut st, &mut c, &ops);
    drop(st);
    let journal = std::fs::read(dir.path().join("journal.bin")).unwrap();
    let mut prefixes = std::collections::BTreeSet::new();
    for cut in 0..=journal.len() {
        let d2 = tempfile::tempdir().unwrap();
        std::fs::write(d2.path().join("journal.bin"), &journal[..cut]).unwrap();
        let (_, rec) = Store::open(d2.path()).unwrap();
        let got = json(&rec.cluster);
        let k = rec.replayed as usize;
        let want = if k == 0 { json(&Cluster::default()) } else { states[k - 1].clone() };
        assert_eq!(got, want, "cut at byte {cut}");
        prefixes.insert(k);
        // the torn tail was truncated: reopening again is a no-op
        let len = std::fs::metadata(d2.path().join("journal.bin")).unwrap().len();
        assert_eq!(len as usize, cut - rec.truncated_bytes as usize);
    }
    assert_eq!(prefixes.len(), states.len() + 1, "every record boundary was hit");
}

#[test]
fn crash_between_snapshot_and_new_journal_does_not_double_apply() {
    let dir = tempfile::tempdir().unwrap();
    let (mut st, rec) = Store::open(dir.path()).unwrap();
    let mut c = rec.cluster;
    let ops: Vec<(u8, u16)> = (0..30u16).map(|i| ((i * 5 % 9) as u8, i * 11 + 3)).collect();
    let states = drive(&mut st, &mut c, &ops);
    let old_journal = std::fs::read(dir.path().join("journal.bin")).unwrap();
    st.compact(&c).unwrap();
    drop(st);
    // as if we crashed after renaming the snapshot but before replacing the journal
    std::fs::write(dir.path().join("journal.bin"), &old_journal).unwrap();
    let (_, rec) = Store::open(dir.path()).unwrap();
    assert_eq!(rec.replayed, 0);
    assert_eq!(json(&rec.cluster), *states.last().unwrap());
}
