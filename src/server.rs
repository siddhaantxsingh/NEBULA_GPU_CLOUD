//! `nebulad`: the control plane. HTTP API for tenants, node agents and
//! operators; a background loop that detects dead nodes and runs scheduling
//! passes. All state changes go through `store::apply` and are journaled
//! before the request that caused them returns.
//!
//! Node agents are level-triggered: every heartbeat returns the complete
//! list of job attempts the node should be running. The agent starts what is
//! missing and kills what is not listed, so lost or repeated messages cannot
//! leave a stray process or a double start.

use crate::model::*;
use crate::sched::{self, Decision, Policy};
use crate::store::{self, Applied, Cmd, Store};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug, Deserialize)]
pub struct TenantConfig {
    pub name: String,
    pub api_key: String,
    #[serde(default)]
    pub quota_gpus: u32,
    #[serde(default = "one")]
    pub weight: f64,
    /// highest job priority this tenant may request
    #[serde(default)]
    pub max_priority: i32,
}

fn one() -> f64 {
    1.0
}

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    pub listen: String,
    pub data_dir: PathBuf,
    pub admin_token: String,
    pub node_token: String,
    #[serde(default = "default_timeout")]
    pub heartbeat_timeout_s: f64,
    #[serde(default = "default_tick")]
    pub tick_ms: u64,
    #[serde(default)]
    pub policy: Policy,
    pub tenants: Vec<TenantConfig>,
}

fn default_timeout() -> f64 {
    10.0
}
fn default_tick() -> u64 {
    200
}

#[derive(Default, Serialize)]
pub struct Metrics {
    pub sched_passes: u64,
    pub sched_seconds_total: f64,
    pub starts: u64,
    pub preemptions: u64,
    pub node_failures: u64,
    pub gpu_failures: u64,
    pub requeues: u64,
}

pub struct Inner {
    pub cluster: Cluster,
    store: Store,
    /// last heartbeat per node (not journaled: after a restart every node
    /// gets a fresh timeout)
    last_seen: HashMap<NodeId, f64>,
    pub metrics: Metrics,
}

#[derive(Clone)]
pub struct App {
    pub inner: Arc<Mutex<Inner>>,
    cfg: Arc<Config>,
    keys: Arc<HashMap<String, TenantConfig>>,
}

pub fn now() -> f64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs_f64()
}

impl Inner {
    /// Apply a command and make it durable. A journal write failure aborts
    /// the process: memory must never run ahead of disk.
    fn exec(&mut self, cmd: Cmd) -> Applied {
        let r = store::apply(&mut self.cluster, &cmd);
        if !matches!(r, Applied::Rejected(_) | Applied::NoOp) {
            self.journal(&cmd);
        }
        if let Applied::Requeued(v) = &r {
            self.metrics.requeues += v.len() as u64;
        }
        r
    }

    /// Make an already-applied command durable.
    fn journal(&mut self, cmd: &Cmd) {
        if let Err(e) = self.store.append(cmd) {
            tracing::error!("journal append failed: {e}; aborting");
            std::process::abort();
        }
        if self.store.needs_compaction()
            && let Err(e) = self.store.compact(&self.cluster)
        {
            tracing::error!("snapshot failed: {e}; aborting");
            std::process::abort();
        }
    }
}

impl App {
    pub fn open(cfg: Config) -> std::io::Result<App> {
        let (store, rec) = Store::open(&cfg.data_dir)?;
        tracing::info!(replayed = rec.replayed, truncated = rec.truncated_bytes, "state recovered");
        let t = now();
        let last_seen = rec.cluster.nodes.keys().map(|n| (n.clone(), t)).collect();
        let mut inner = Inner {
            cluster: rec.cluster,
            store,
            last_seen,
            metrics: Metrics::default(),
        };
        for tc in &cfg.tenants {
            let want = Tenant {
                name: tc.name.clone(),
                quota_gpus: tc.quota_gpus,
                weight: tc.weight,
            };
            if inner.cluster.tenants.get(&tc.name) != Some(&want) {
                inner.exec(Cmd::AddTenant { tenant: want });
            }
        }
        let keys = cfg.tenants.iter().map(|t| (t.api_key.clone(), t.clone())).collect();
        Ok(App {
            inner: Arc::new(Mutex::new(inner)),
            cfg: Arc::new(cfg),
            keys: Arc::new(keys),
        })
    }

    /// One control-loop iteration: expire silent nodes, then schedule.
    pub fn tick(&self) {
        let mut g = self.inner.lock();
        let t = now();
        let dead: Vec<NodeId> = g
            .cluster
            .nodes
            .values()
            .filter(|n| n.state != NodeState::Down)
            .filter(|n| t - g.last_seen.get(&n.id).copied().unwrap_or(t) > self.cfg.heartbeat_timeout_s)
            .map(|n| n.id.clone())
            .collect();
        for n in dead {
            tracing::warn!(node = %n, "heartbeat timeout; requeueing its jobs");
            g.metrics.node_failures += 1;
            g.exec(Cmd::NodeDown { node: n, now: t });
        }
        let t0 = Instant::now();
        let decisions = sched::schedule(&mut g.cluster, t, &self.cfg.policy);
        g.metrics.sched_passes += 1;
        g.metrics.sched_seconds_total += t0.elapsed().as_secs_f64();
        if !decisions.is_empty() {
            for d in &decisions {
                match d {
                    Decision::Start { job, placement } => {
                        g.metrics.starts += 1;
                        tracing::info!(job, ?placement, "start");
                    }
                    Decision::Preempt { job } => {
                        g.metrics.preemptions += 1;
                        tracing::info!(job, "preempt");
                    }
                }
            }
            // already applied by `schedule`; replay applies them the same way
            g.journal(&Cmd::Decisions { decisions, now: t });
        }
    }

    pub fn router(&self) -> Router {
        Router::new()
            .route("/healthz", get(|| async { "ok" }))
            .route("/metrics", get(metrics))
            .route("/v1/jobs", post(submit).get(list_jobs))
            .route("/v1/jobs/:id", get(get_job).delete(cancel_job))
            .route("/v1/usage", get(usage))
            .route("/v1/admin/cluster", get(cluster))
            .route("/v1/admin/usage", get(usage_all))
            .route("/v1/admin/nodes/:id/drain", post(drain).delete(undrain))
            .route("/v1/nodes/register", post(register))
            .route("/v1/nodes/:id/heartbeat", post(heartbeat))
            .route("/v1/nodes/:id", delete(deregister))
            .with_state(self.clone())
    }

    pub async fn serve(self, listener: tokio::net::TcpListener) -> std::io::Result<()> {
        let app = self.clone();
        let tick = Duration::from_millis(self.cfg.tick_ms);
        let ticker = tokio::spawn(async move {
            let mut iv = tokio::time::interval(tick);
            iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                iv.tick().await;
                let a = app.clone();
                tokio::task::spawn_blocking(move || a.tick()).await.ok();
            }
        });
        let r = axum::serve(listener, self.router())
            .with_graceful_shutdown(async {
                let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
                tokio::select! {
                    _ = term.recv() => {}
                    _ = tokio::signal::ctrl_c() => {}
                }
            })
            .await;
        ticker.abort();
        r
    }
}

// ------------------------------------------------------------------ errors and auth

pub struct ApiError(StatusCode, String);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({ "error": self.1 }))).into_response()
    }
}

fn err(code: StatusCode, msg: impl Into<String>) -> ApiError {
    ApiError(code, msg.into())
}

fn bearer(h: &HeaderMap) -> Option<&str> {
    h.get("authorization")?.to_str().ok()?.strip_prefix("Bearer ")
}

/// Constant-time comparison for secrets.
fn eq_secret(a: &str, b: &str) -> bool {
    a.len() == b.len() && a.bytes().zip(b.bytes()).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

fn tenant(app: &App, h: &HeaderMap) -> Result<TenantConfig, ApiError> {
    let k = bearer(h).ok_or(err(StatusCode::UNAUTHORIZED, "missing bearer token"))?;
    app.keys
        .iter()
        .find(|(key, _)| eq_secret(key, k))
        .map(|(_, t)| t.clone())
        .ok_or(err(StatusCode::UNAUTHORIZED, "unknown API key"))
}

fn admin(app: &App, h: &HeaderMap) -> Result<(), ApiError> {
    match bearer(h) {
        Some(k) if eq_secret(k, &app.cfg.admin_token) => Ok(()),
        _ => Err(err(StatusCode::UNAUTHORIZED, "admin token required")),
    }
}

fn node_auth(app: &App, h: &HeaderMap) -> Result<(), ApiError> {
    match bearer(h) {
        Some(k) if eq_secret(k, &app.cfg.node_token) => Ok(()),
        _ => Err(err(StatusCode::UNAUTHORIZED, "node token required")),
    }
}

// ------------------------------------------------------------------ tenant API

#[derive(Deserialize)]
pub struct JobRequest {
    pub gpus: u32,
    #[serde(default)]
    pub gpu_model: Option<String>,
    #[serde(default)]
    pub priority: i32,
    #[serde(default = "yes")]
    pub preemptible: bool,
    pub est_runtime: f64,
    #[serde(default)]
    pub command: Vec<String>,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub image: Option<String>,
    #[serde(default)]
    pub cpus: Option<f64>,
    #[serde(default)]
    pub memory_mb: Option<u64>,
}

fn yes() -> bool {
    true
}

async fn submit(
    State(app): State<App>,
    h: HeaderMap,
    Json(r): Json<JobRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), ApiError> {
    let t = tenant(&app, &h)?;
    if r.priority > t.max_priority {
        return Err(err(
            StatusCode::FORBIDDEN,
            format!("priority {} above tenant maximum {}", r.priority, t.max_priority),
        ));
    }
    if r.command.is_empty() {
        return Err(err(StatusCode::BAD_REQUEST, "command must not be empty"));
    }
    if r.cpus.is_some_and(|c| !(c > 0.0 && c <= 1024.0)) {
        return Err(err(StatusCode::BAD_REQUEST, "cpus must be in (0, 1024]"));
    }
    // Docker refuses memory limits below 6 MiB
    if r.memory_mb.is_some_and(|m| !(6..=16 << 20).contains(&m)) {
        return Err(err(StatusCode::BAD_REQUEST, "memory_mb must be in [6, 16777216]"));
    }
    if r.image
        .as_deref()
        .is_some_and(|i| i.is_empty() || i.starts_with('-') || i.chars().any(char::is_whitespace))
    {
        return Err(err(StatusCode::BAD_REQUEST, "invalid image name"));
    }
    let spec = JobSpec {
        tenant: t.name.clone(),
        gpus: r.gpus,
        gpu_model: r.gpu_model,
        priority: r.priority,
        preemptible: r.preemptible,
        est_runtime: r.est_runtime,
        command: r.command,
        name: r.name,
        image: r.image,
        cpus: r.cpus,
        memory_mb: r.memory_mb,
    };
    match app.inner.lock().exec(Cmd::Submit { spec, now: now() }) {
        Applied::Job(id) => Ok((StatusCode::CREATED, Json(json!({ "id": id })))),
        Applied::Rejected(sched::SubmitError::BadRequest(m)) => Err(err(StatusCode::BAD_REQUEST, m)),
        Applied::Rejected(sched::SubmitError::ExceedsCapacity) => {
            Err(err(StatusCode::UNPROCESSABLE_ENTITY, "more GPUs than the cluster has"))
        }
        Applied::Rejected(sched::SubmitError::UnknownTenant) => Err(err(StatusCode::UNAUTHORIZED, "unknown tenant")),
        other => Err(err(StatusCode::INTERNAL_SERVER_ERROR, format!("{other:?}"))),
    }
}

async fn list_jobs(State(app): State<App>, h: HeaderMap) -> Result<Json<Vec<Job>>, ApiError> {
    let t = tenant(&app, &h)?;
    let g = app.inner.lock();
    Ok(Json(g.cluster.jobs.values().filter(|j| j.spec.tenant == t.name).cloned().collect()))
}

async fn get_job(State(app): State<App>, h: HeaderMap, Path(id): Path<JobId>) -> Result<Json<Job>, ApiError> {
    let t = tenant(&app, &h)?;
    let g = app.inner.lock();
    match g.cluster.jobs.get(&id) {
        Some(j) if j.spec.tenant == t.name => Ok(Json(j.clone())),
        _ => Err(err(StatusCode::NOT_FOUND, "no such job")),
    }
}

async fn cancel_job(State(app): State<App>, h: HeaderMap, Path(id): Path<JobId>) -> Result<StatusCode, ApiError> {
    let t = tenant(&app, &h)?;
    let mut g = app.inner.lock();
    match g.cluster.jobs.get(&id) {
        Some(j) if j.spec.tenant == t.name => {}
        _ => return Err(err(StatusCode::NOT_FOUND, "no such job")),
    }
    match g.exec(Cmd::Cancel { job: id, now: now() }) {
        Applied::Ok => Ok(StatusCode::NO_CONTENT),
        _ => Err(err(StatusCode::CONFLICT, "job already finished")),
    }
}

#[derive(Serialize, Default)]
pub struct Usage {
    pub tenant: String,
    /// GPU-hours of finished attempts plus running jobs so far
    pub gpu_hours: f64,
    pub gpus_in_use: u32,
    pub quota_gpus: u32,
    pub jobs: BTreeMap<String, u32>,
}

fn usage_of(c: &Cluster, tenant: &str, t: f64) -> Usage {
    let mut u = Usage {
        tenant: tenant.into(),
        quota_gpus: c.tenants.get(tenant).map(|x| x.quota_gpus).unwrap_or(0),
        ..Default::default()
    };
    for j in c.jobs.values().filter(|j| j.spec.tenant == tenant) {
        let mut s = j.gpu_seconds;
        let state = match &j.state {
            JobState::Queued => "queued",
            JobState::Running { start, placement } => {
                let n: usize = placement.iter().map(|p| p.gpus.len()).sum();
                s += n as f64 * (t - start).max(0.0);
                u.gpus_in_use += n as u32;
                "running"
            }
            JobState::Succeeded { .. } => "succeeded",
            JobState::Failed { .. } => "failed",
            JobState::Cancelled { .. } => "cancelled",
        };
        *u.jobs.entry(state.into()).or_default() += 1;
        u.gpu_hours += s / 3600.0;
    }
    u
}

async fn usage(State(app): State<App>, h: HeaderMap) -> Result<Json<Usage>, ApiError> {
    let t = tenant(&app, &h)?;
    Ok(Json(usage_of(&app.inner.lock().cluster, &t.name, now())))
}

// ------------------------------------------------------------------ operator API

async fn usage_all(State(app): State<App>, h: HeaderMap) -> Result<Json<Vec<Usage>>, ApiError> {
    admin(&app, &h)?;
    let g = app.inner.lock();
    let t = now();
    Ok(Json(g.cluster.tenants.keys().map(|n| usage_of(&g.cluster, n, t)).collect()))
}

async fn cluster(State(app): State<App>, h: HeaderMap) -> Result<Json<serde_json::Value>, ApiError> {
    admin(&app, &h)?;
    let g = app.inner.lock();
    let c = &g.cluster;
    Ok(Json(json!({
        "nodes": c.nodes.values().collect::<Vec<_>>(),
        "gpus_total": c.total_gpus(),
        "gpus_used": c.used_gpus(),
        "queued": c.jobs.values().filter(|j| j.is_queued()).count(),
        "running": c.jobs.values().filter(|j| j.running_placement().is_some()).count(),
        "metrics": &g.metrics,
    })))
}

async fn drain(State(app): State<App>, h: HeaderMap, Path(id): Path<NodeId>) -> Result<StatusCode, ApiError> {
    admin(&app, &h)?;
    match app.inner.lock().exec(Cmd::Drain { node: id, drain: true }) {
        Applied::Ok => Ok(StatusCode::NO_CONTENT),
        _ => Err(err(StatusCode::NOT_FOUND, "no such node, or node is down")),
    }
}

async fn undrain(State(app): State<App>, h: HeaderMap, Path(id): Path<NodeId>) -> Result<StatusCode, ApiError> {
    admin(&app, &h)?;
    match app.inner.lock().exec(Cmd::Drain { node: id, drain: false }) {
        Applied::Ok => Ok(StatusCode::NO_CONTENT),
        _ => Err(err(StatusCode::NOT_FOUND, "no such node, or node is down")),
    }
}

async fn metrics(State(app): State<App>) -> String {
    let g = app.inner.lock();
    let c = &g.cluster;
    let m = &g.metrics;
    let mut s = String::new();
    let mut put = |name: &str, kind: &str, v: f64| {
        s.push_str(&format!("# TYPE nebula_{name} {kind}\nnebula_{name} {v}\n"));
    };
    put("gpus_total", "gauge", c.total_gpus() as f64);
    put("gpus_used", "gauge", c.used_gpus() as f64);
    put("jobs_queued", "gauge", c.jobs.values().filter(|j| j.is_queued()).count() as f64);
    put(
        "jobs_running",
        "gauge",
        c.jobs.values().filter(|j| j.running_placement().is_some()).count() as f64,
    );
    put(
        "nodes_up",
        "gauge",
        c.nodes.values().filter(|n| n.state == NodeState::Up).count() as f64,
    );
    put(
        "nodes_down",
        "gauge",
        c.nodes.values().filter(|n| n.state == NodeState::Down).count() as f64,
    );
    put("sched_passes_total", "counter", m.sched_passes as f64);
    put("sched_seconds_total", "counter", m.sched_seconds_total);
    put("job_starts_total", "counter", m.starts as f64);
    put("preemptions_total", "counter", m.preemptions as f64);
    put("node_failures_total", "counter", m.node_failures as f64);
    put("gpu_failures_total", "counter", m.gpu_failures as f64);
    put("requeues_total", "counter", m.requeues as f64);
    s
}

// ------------------------------------------------------------------ node API

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct GpuInfo {
    pub index: u32,
    pub model: String,
    pub island: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_mb: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uuid: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RegisterRequest {
    pub id: NodeId,
    pub gpus: Vec<GpuInfo>,
}

async fn register(State(app): State<App>, h: HeaderMap, Json(r): Json<RegisterRequest>) -> Result<Json<serde_json::Value>, ApiError> {
    node_auth(&app, &h)?;
    if r.id.is_empty() || r.gpus.is_empty() {
        return Err(err(StatusCode::BAD_REQUEST, "id and gpus are required"));
    }
    let t = now();
    let node = Node {
        id: r.id.clone(),
        gpus: r
            .gpus
            .iter()
            .map(|g| Gpu {
                index: g.index,
                model: g.model.clone(),
                island: g.island,
                healthy: true,
                memory_mb: g.memory_mb,
                uuid: g.uuid.clone(),
            })
            .collect(),
        state: NodeState::Up,
        used: BTreeMap::new(),
        last_heartbeat: t,
    };
    let mut g = app.inner.lock();
    let lost = match g.exec(Cmd::Register { node, now: t }) {
        Applied::Requeued(v) => v,
        _ => vec![],
    };
    g.last_seen.insert(r.id.clone(), t);
    tracing::info!(node = %r.id, gpus = r.gpus.len(), requeued = ?lost, "node registered");
    Ok(Json(json!({ "requeued": lost })))
}

async fn deregister(State(app): State<App>, h: HeaderMap, Path(id): Path<NodeId>) -> Result<StatusCode, ApiError> {
    node_auth(&app, &h)?;
    let mut g = app.inner.lock();
    if !g.cluster.nodes.contains_key(&id) {
        return Err(err(StatusCode::NOT_FOUND, "unknown node"));
    }
    g.exec(Cmd::NodeDown { node: id, now: now() });
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct AttemptRef {
    pub job: JobId,
    pub attempt: u32,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Finished {
    pub job: JobId,
    pub attempt: u32,
    pub exit_code: i32,
    #[serde(default)]
    pub reason: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct HeartbeatRequest {
    #[serde(default)]
    pub running: Vec<AttemptRef>,
    #[serde(default)]
    pub finished: Vec<Finished>,
    #[serde(default)]
    pub failed_gpus: Vec<u32>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Assignment {
    pub job: JobId,
    pub attempt: u32,
    pub gpus: Vec<u32>,
    pub command: Vec<String>,
    pub env: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpus: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_mb: Option<u64>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct HeartbeatResponse {
    pub assignments: Vec<Assignment>,
}

async fn heartbeat(
    State(app): State<App>,
    h: HeaderMap,
    Path(id): Path<NodeId>,
    Json(r): Json<HeartbeatRequest>,
) -> Result<Json<HeartbeatResponse>, ApiError> {
    node_auth(&app, &h)?;
    let t = now();
    let mut g = app.inner.lock();
    let Some(node) = g.cluster.nodes.get(&id) else {
        return Err(err(StatusCode::NOT_FOUND, "unknown node; register first"));
    };
    if node.state == NodeState::Down {
        g.exec(Cmd::NodeUp { node: id.clone() });
    }
    g.last_seen.insert(id.clone(), t);
    for f in &r.finished {
        // only the current attempt, and only if it runs on this node
        let Some(j) = g.cluster.jobs.get(&f.job) else { continue };
        let Some(pl) = j.running_placement() else { continue };
        if j.restarts != f.attempt || !pl.iter().any(|p| p.node == id) {
            continue;
        }
        let rank0 = pl[0].node == id;
        if f.exit_code != 0 {
            let reason = format!(
                "exit code {} on {}{}",
                f.exit_code,
                id,
                if f.reason.is_empty() {
                    String::new()
                } else {
                    format!(": {}", f.reason)
                }
            );
            g.exec(Cmd::Complete {
                job: f.job,
                success: false,
                reason,
                now: t,
            });
        } else if rank0 {
            g.exec(Cmd::Complete {
                job: f.job,
                success: true,
                reason: String::new(),
                now: t,
            });
        }
    }
    for &gpu in &r.failed_gpus {
        let healthy = g.cluster.nodes[&id].gpus.iter().any(|x| x.index == gpu && x.healthy);
        if healthy {
            tracing::warn!(node = %id, gpu, "GPU failed");
            g.metrics.gpu_failures += 1;
            g.exec(Cmd::GpuFailed {
                node: id.clone(),
                gpu,
                now: t,
            });
        }
    }
    Ok(Json(HeartbeatResponse {
        assignments: assignments_for(&g.cluster, &id),
    }))
}

pub fn assignments_for(c: &Cluster, node: &str) -> Vec<Assignment> {
    let mut out = vec![];
    for j in c.jobs.values() {
        let Some(pl) = j.running_placement() else { continue };
        let Some(rank) = pl.iter().position(|p| p.node == node) else {
            continue;
        };
        let gpus = pl[rank].gpus.clone();
        let mut env = BTreeMap::new();
        env.insert(
            "CUDA_VISIBLE_DEVICES".into(),
            gpus.iter().map(|g| g.to_string()).collect::<Vec<_>>().join(","),
        );
        env.insert("NEBULA_JOB_ID".into(), j.id.to_string());
        env.insert("NEBULA_ATTEMPT".into(), j.restarts.to_string());
        env.insert("NEBULA_TENANT".into(), j.spec.tenant.clone());
        env.insert("NEBULA_NODE_RANK".into(), rank.to_string());
        env.insert("NEBULA_NUM_NODES".into(), pl.len().to_string());
        env.insert("NEBULA_MASTER_NODE".into(), pl[0].node.clone());
        out.push(Assignment {
            job: j.id,
            attempt: j.restarts,
            gpus,
            command: j.spec.command.clone(),
            env,
            image: j.spec.image.clone(),
            cpus: j.spec.cpus,
            memory_mb: j.spec.memory_mb,
        });
    }
    out
}
