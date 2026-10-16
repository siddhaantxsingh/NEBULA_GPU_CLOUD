//! Durable control-plane state: a snapshot of the `Cluster` plus a journal
//! of the commands applied since. Every record is
//! `[len u32 LE][crc32c u32 LE][seq u64 LE][json]`, appended and fsynced
//! before the API call that caused it returns. Scheduling passes are
//! journaled as their decisions (not as "run the scheduler"), so replay does
//! not depend on the policy in the current configuration.
//!
//! Recovery loads the snapshot, replays journal records with a larger
//! sequence number than the snapshot's, and truncates a torn or corrupt
//! tail. Compaction writes the snapshot to a temporary file, fsyncs it,
//! renames it over the old one, fsyncs the directory and only then starts
//! a new journal; a crash between those steps leaves records that replay
//! skips by sequence number.

use crate::model::*;
use crate::sched::{self, Decision, SubmitError};
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Cmd {
    AddTenant {
        tenant: Tenant,
    },
    Submit {
        spec: JobSpec,
        now: f64,
    },
    Cancel {
        job: JobId,
        now: f64,
    },
    /// node (re)registration: replaces its GPU inventory; jobs that were on
    /// it are requeued first (the agent restarted and lost its processes)
    Register {
        node: Node,
        now: f64,
    },
    NodeDown {
        node: NodeId,
        now: f64,
    },
    NodeUp {
        node: NodeId,
    },
    Drain {
        node: NodeId,
        drain: bool,
    },
    Complete {
        job: JobId,
        success: bool,
        reason: String,
        now: f64,
    },
    GpuFailed {
        node: NodeId,
        gpu: u32,
        now: f64,
    },
    Decisions {
        decisions: Vec<Decision>,
        now: f64,
    },
}

#[derive(Debug, PartialEq)]
pub enum Applied {
    Ok,
    Job(JobId),
    Requeued(Vec<JobId>),
    Rejected(SubmitError),
    NoOp,
}

/// Apply a command to the state. Deterministic: the live server and replay
/// both go through here.
pub fn apply(c: &mut Cluster, cmd: &Cmd) -> Applied {
    match cmd {
        Cmd::AddTenant { tenant } => {
            c.tenants.insert(tenant.name.clone(), tenant.clone());
            Applied::Ok
        }
        Cmd::Submit { spec, now } => match sched::submit(c, spec.clone(), *now) {
            Ok(id) => Applied::Job(id),
            Err(e) => Applied::Rejected(e),
        },
        Cmd::Cancel { job, now } => {
            if sched::cancel(c, *job, *now) {
                Applied::Ok
            } else {
                Applied::NoOp
            }
        }
        Cmd::Register { node, now } => {
            let draining = c.nodes.get(&node.id).is_some_and(|n| n.state == NodeState::Draining);
            let lost = if c.nodes.contains_key(&node.id) {
                sched::node_down(c, &node.id, *now)
            } else {
                vec![]
            };
            let mut n = node.clone();
            n.used.clear();
            n.state = if draining { NodeState::Draining } else { NodeState::Up };
            n.last_heartbeat = *now;
            c.nodes.insert(n.id.clone(), n);
            Applied::Requeued(lost)
        }
        Cmd::NodeDown { node, now } => Applied::Requeued(sched::node_down(c, node, *now)),
        Cmd::NodeUp { node } => match c.nodes.get_mut(node) {
            Some(n) if n.state == NodeState::Down => {
                n.state = NodeState::Up;
                Applied::Ok
            }
            _ => Applied::NoOp,
        },
        Cmd::Drain { node, drain } => match c.nodes.get_mut(node) {
            Some(n) if n.state != NodeState::Down => {
                n.state = if *drain { NodeState::Draining } else { NodeState::Up };
                Applied::Ok
            }
            _ => Applied::NoOp,
        },
        Cmd::Complete { job, success, reason, now } => {
            if sched::complete(c, *job, *now, *success, reason) {
                Applied::Ok
            } else {
                Applied::NoOp
            }
        }
        Cmd::GpuFailed { node, gpu, now } => match sched::gpu_failed(c, node, *gpu, *now) {
            Some(j) => Applied::Requeued(vec![j]),
            None => Applied::Ok,
        },
        Cmd::Decisions { decisions, now } => {
            for d in decisions {
                match d {
                    Decision::Start { job, placement } => sched::apply_start(c, *job, placement.clone(), *now),
                    Decision::Preempt { job } => sched::requeue(c, *job, *now),
                }
            }
            Applied::Ok
        }
    }
}

// ------------------------------------------------------------------ crc32c

fn crc32c(data: &[u8]) -> u32 {
    static TABLE: std::sync::OnceLock<[u32; 256]> = std::sync::OnceLock::new();
    let t = TABLE.get_or_init(|| {
        let mut t = [0u32; 256];
        for (i, e) in t.iter_mut().enumerate() {
            let mut c = i as u32;
            for _ in 0..8 {
                c = if c & 1 != 0 { 0x82F6_3B78 ^ (c >> 1) } else { c >> 1 };
            }
            *e = c;
        }
        t
    });
    !data.iter().fold(!0u32, |c, &b| t[((c ^ b as u32) & 0xff) as usize] ^ (c >> 8))
}

// ------------------------------------------------------------------ store

#[derive(Serialize, Deserialize)]
struct Snapshot {
    seq: u64,
    cluster: Cluster,
}

pub struct Store {
    dir: PathBuf,
    journal: File,
    pub seq: u64,
    pub records_since_snapshot: u64,
    pub compact_every: u64,
}

pub struct Recovered {
    pub cluster: Cluster,
    pub replayed: u64,
    pub truncated_bytes: u64,
}

fn encode(seq: u64, cmd: &Cmd) -> Vec<u8> {
    let body = serde_json::to_vec(cmd).expect("serialise command");
    let mut payload = seq.to_le_bytes().to_vec();
    payload.extend_from_slice(&body);
    let mut out = (payload.len() as u32).to_le_bytes().to_vec();
    out.extend_from_slice(&crc32c(&payload).to_le_bytes());
    out.extend_from_slice(&payload);
    out
}

/// Parse records; returns (seq, cmd) pairs and the length of the valid prefix.
fn decode_all(buf: &[u8]) -> (Vec<(u64, Cmd)>, usize) {
    let mut out = vec![];
    let mut off = 0;
    while off + 8 <= buf.len() {
        let len = u32::from_le_bytes(buf[off..off + 4].try_into().unwrap()) as usize;
        let crc = u32::from_le_bytes(buf[off + 4..off + 8].try_into().unwrap());
        if len < 8 || off + 8 + len > buf.len() {
            break;
        }
        let payload = &buf[off + 8..off + 8 + len];
        if crc32c(payload) != crc {
            break;
        }
        let seq = u64::from_le_bytes(payload[..8].try_into().unwrap());
        let Ok(cmd) = serde_json::from_slice::<Cmd>(&payload[8..]) else {
            break;
        };
        out.push((seq, cmd));
        off += 8 + len;
    }
    (out, off)
}

fn fsync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

impl Store {
    pub fn open(dir: &Path) -> io::Result<(Store, Recovered)> {
        fs::create_dir_all(dir)?;
        let snap_path = dir.join("snapshot.json");
        let (mut cluster, snap_seq) = match fs::read(&snap_path) {
            Ok(b) => {
                let s: Snapshot = serde_json::from_slice(&b).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                (s.cluster, s.seq)
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => (Cluster::default(), 0),
            Err(e) => return Err(e),
        };
        let jpath = dir.join("journal.bin");
        let mut buf = vec![];
        if let Ok(mut f) = File::open(&jpath) {
            f.read_to_end(&mut buf)?;
        }
        let (recs, valid) = decode_all(&buf);
        let truncated = (buf.len() - valid) as u64;
        let mut seq = snap_seq;
        let mut replayed = 0;
        for (s, cmd) in recs {
            if s <= snap_seq {
                continue;
            }
            apply(&mut cluster, &cmd);
            seq = s;
            replayed += 1;
        }
        let journal = OpenOptions::new().create(true).append(true).open(&jpath)?;
        if truncated > 0 {
            journal.set_len(valid as u64)?;
            journal.sync_all()?;
        }
        fsync_dir(dir)?;
        let store = Store {
            dir: dir.to_path_buf(),
            journal,
            seq,
            records_since_snapshot: replayed,
            compact_every: 10_000,
        };
        Ok((
            store,
            Recovered {
                cluster,
                replayed,
                truncated_bytes: truncated,
            },
        ))
    }

    /// Append and fsync one command.
    pub fn append(&mut self, cmd: &Cmd) -> io::Result<()> {
        self.seq += 1;
        self.journal.write_all(&encode(self.seq, cmd))?;
        self.journal.sync_data()?;
        self.records_since_snapshot += 1;
        Ok(())
    }

    pub fn needs_compaction(&self) -> bool {
        self.records_since_snapshot >= self.compact_every
    }

    pub fn compact(&mut self, c: &Cluster) -> io::Result<()> {
        let tmp = self.dir.join("snapshot.json.tmp");
        {
            let mut f = File::create(&tmp)?;
            f.write_all(
                &serde_json::to_vec(&Snapshot {
                    seq: self.seq,
                    cluster: c.clone(),
                })
                .unwrap(),
            )?;
            f.sync_all()?;
        }
        fs::rename(&tmp, self.dir.join("snapshot.json"))?;
        fsync_dir(&self.dir)?;
        let jpath = self.dir.join("journal.bin");
        let ntmp = self.dir.join("journal.bin.tmp");
        File::create(&ntmp)?.sync_all()?;
        fs::rename(&ntmp, &jpath)?;
        fsync_dir(&self.dir)?;
        self.journal = OpenOptions::new().append(true).open(&jpath)?;
        self.records_since_snapshot = 0;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc32c_known_value() {
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
    }

    #[test]
    fn decode_stops_at_torn_or_corrupt_records() {
        let a = encode(1, &Cmd::NodeUp { node: "a".into() });
        let b = encode(2, &Cmd::NodeUp { node: "b".into() });
        let mut buf = [a.clone(), b.clone()].concat();
        assert_eq!(decode_all(&buf).0.len(), 2);
        for cut in 0..buf.len() {
            let (r, valid) = decode_all(&buf[..cut]);
            let want = if cut >= a.len() + b.len() {
                2
            } else if cut >= a.len() {
                1
            } else {
                0
            };
            assert_eq!(r.len(), want, "cut at {cut}");
            assert_eq!(valid, [0, a.len(), a.len() + b.len()][want]);
        }
        let n = buf.len();
        buf[n - 2] ^= 1;
        assert_eq!(decode_all(&buf).0.len(), 1);
    }
}
