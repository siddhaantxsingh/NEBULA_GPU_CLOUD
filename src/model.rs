//! Cluster model: nodes with GPUs (grouped into NVLink islands), tenants,
//! jobs and allocations. Plain data; all policy lives in `sched`.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub type JobId = u64;
pub type NodeId = String;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Gpu {
    pub index: u32,
    pub model: String,
    /// GPUs with the same island id are connected by NVLink/NVSwitch.
    pub island: u32,
    pub healthy: bool,
    /// reported by real-GPU agents (from nvidia-smi); absent for simulated GPUs
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_mb: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uuid: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeState {
    Up,
    /// no new work is placed; running jobs continue
    Draining,
    /// missed heartbeats or reported failure; its jobs were requeued
    Down,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Node {
    pub id: NodeId,
    pub gpus: Vec<Gpu>,
    pub state: NodeState,
    /// gpu index -> job using it
    #[serde(default)]
    pub used: BTreeMap<u32, JobId>,
    #[serde(default)]
    pub last_heartbeat: f64,
}

impl Node {
    pub fn free_gpus<'a>(&'a self, model: Option<&'a str>) -> impl Iterator<Item = &'a Gpu> + 'a {
        self.gpus
            .iter()
            .filter(move |g| g.healthy && !self.used.contains_key(&g.index) && model.is_none_or(|m| g.model == m))
    }
    pub fn schedulable(&self) -> bool {
        self.state == NodeState::Up
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Tenant {
    pub name: String,
    /// maximum GPUs the tenant may hold at once (0 = unlimited)
    pub quota_gpus: u32,
    /// fair-share weight
    pub weight: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct JobSpec {
    pub tenant: String,
    pub gpus: u32,
    #[serde(default)]
    pub gpu_model: Option<String>,
    /// higher runs first and may preempt lower (if preemptible)
    #[serde(default)]
    pub priority: i32,
    #[serde(default = "yes")]
    pub preemptible: bool,
    /// user estimate, used for backfill reservations (seconds)
    pub est_runtime: f64,
    /// command run by the node agent (CUDA_VISIBLE_DEVICES is set)
    #[serde(default)]
    pub command: Vec<String>,
    #[serde(default)]
    pub name: String,
    /// container runtime only: image, CPU and memory limits (agent defaults
    /// apply when absent; the process runtime ignores them)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpus: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_mb: Option<u64>,
}

fn yes() -> bool {
    true
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Placement {
    pub node: NodeId,
    pub gpus: Vec<u32>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum JobState {
    Queued,
    Running { start: f64, placement: Vec<Placement> },
    Succeeded { start: f64, end: f64 },
    Failed { start: f64, end: f64, reason: String },
    Cancelled { at: f64 },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Job {
    pub id: JobId,
    pub spec: JobSpec,
    pub submitted: f64,
    pub state: JobState,
    /// times the job was preempted or lost with a node
    pub restarts: u32,
    /// GPU-seconds consumed across all attempts (metering)
    pub gpu_seconds: f64,
    /// when the current queueing period started (submit or requeue)
    pub queued_since: f64,
}

impl Job {
    pub fn running_placement(&self) -> Option<&Vec<Placement>> {
        match &self.state {
            JobState::Running { placement, .. } => Some(placement),
            _ => None,
        }
    }
    pub fn is_queued(&self) -> bool {
        matches!(self.state, JobState::Queued)
    }
}

/// The complete scheduler state.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Cluster {
    pub nodes: BTreeMap<NodeId, Node>,
    pub tenants: BTreeMap<String, Tenant>,
    pub jobs: BTreeMap<JobId, Job>,
    pub next_job: JobId,
}

impl Cluster {
    pub fn gpus_in_use(&self, tenant: &str) -> u32 {
        self.jobs
            .values()
            .filter(|j| j.spec.tenant == tenant)
            .filter_map(|j| j.running_placement())
            .map(|p| p.iter().map(|x| x.gpus.len() as u32).sum::<u32>())
            .sum()
    }
    pub fn total_gpus(&self) -> u32 {
        self.nodes
            .values()
            .filter(|n| n.state != NodeState::Down)
            .map(|n| n.gpus.iter().filter(|g| g.healthy).count() as u32)
            .sum()
    }
    pub fn used_gpus(&self) -> u32 {
        self.nodes.values().map(|n| n.used.len() as u32).sum()
    }
}
