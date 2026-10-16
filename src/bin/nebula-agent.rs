//! Node agent. Registers its GPUs, heartbeats, and reconciles local
//! processes with the assignment list in every heartbeat reply: starts
//! missing job attempts (with CUDA_VISIBLE_DEVICES and NEBULA_* set) and
//! kills processes that are no longer assigned.
//!
//! GPUs: with `--gpu-source auto` (default) the agent reports the devices
//! `nvidia-smi` lists, if it is installed; otherwise it reports `--gpus`
//! simulated devices. A GPU failure is injected by creating
//! `<work-dir>/fail-gpu-<index>`.
//!
//! Runtimes: `--runtime process` (default) runs the command as a plain
//! process group; `--runtime docker` runs each attempt as a hardened container
//! (see `nebula::runtime`).
//!
//! nebula-agent --server URL --token T --id NODE [--gpus 8] [--islands 2]
//!              [--model h100] [--work-dir DIR] [--interval-ms 500]
//!              [--gpu-source auto|nvidia|sim] [--nvidia-smi PATH]
//!              [--runtime process|docker] [--image IMG] [--default-cpus N]
//!              [--default-memory-mb N] [--container-network none]
//!              [--container-user UID:GID] [--stop-grace-s 3] [--docker PATH]

use nebula::gpu::{self, Source};
use nebula::runtime::{self, DockerConfig};
use nebula::server::{Assignment, AttemptRef, Finished, GpuInfo, HeartbeatRequest, HeartbeatResponse, RegisterRequest};
use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use std::process::Stdio;
use std::time::{Duration, Instant};
use tokio::process::{Child, Command};

struct Args {
    server: String,
    token: String,
    id: String,
    gpus: u32,
    islands: u32,
    model: String,
    work_dir: PathBuf,
    interval: Duration,
    gpu_source: String,
    nvidia_smi: String,
    runtime: String,
    grace: Duration,
    docker: DockerConfig,
}

fn parse() -> Args {
    let mut a = Args {
        server: "http://127.0.0.1:7070".into(),
        token: String::new(),
        id: String::new(),
        gpus: 8,
        islands: 2,
        model: "h100".into(),
        work_dir: PathBuf::from("agent-work"),
        interval: Duration::from_millis(500),
        gpu_source: "auto".into(),
        nvidia_smi: "nvidia-smi".into(),
        runtime: "process".into(),
        grace: Duration::from_secs(3),
        docker: DockerConfig::new(""),
    };
    let v: Vec<String> = std::env::args().skip(1).collect();
    for p in v.chunks(2) {
        let val = p.get(1).cloned().unwrap_or_else(|| panic!("missing value for {}", p[0]));
        match p[0].as_str() {
            "--server" => a.server = val,
            "--token" => a.token = val,
            "--id" => a.id = val,
            "--gpus" => a.gpus = val.parse().unwrap(),
            "--islands" => a.islands = val.parse().unwrap(),
            "--model" => a.model = val,
            "--work-dir" => a.work_dir = val.into(),
            "--interval-ms" => a.interval = Duration::from_millis(val.parse().unwrap()),
            "--gpu-source" => a.gpu_source = val,
            "--nvidia-smi" => a.nvidia_smi = val,
            "--runtime" => a.runtime = val,
            "--stop-grace-s" => a.grace = Duration::from_secs_f64(val.parse().unwrap()),
            "--docker" => a.docker.docker = val,
            "--image" => a.docker.default_image = val,
            "--default-cpus" => a.docker.default_cpus = val.parse().unwrap(),
            "--default-memory-mb" => a.docker.default_memory_mb = val.parse().unwrap(),
            "--container-network" => a.docker.network = val,
            "--container-user" => a.docker.user = val,
            x => panic!("unknown argument {x}"),
        }
    }
    assert!(!a.id.is_empty(), "--id is required");
    assert!(
        a.runtime == "process" || a.runtime == "docker",
        "--runtime must be process or docker"
    );
    a.docker.node = a.id.clone();
    a
}

enum Handle {
    Process { child: Child, pid: u32 },
    Container { name: String },
}

struct Proc {
    h: Handle,
    log: PathBuf,
    memory_mb: u64,
    /// set when we asked it to stop; escalated to SIGKILL after a grace period
    stopping: Option<Instant>,
}

fn signal_group(pid: u32, sig: &str) {
    // each job runs in its own process group so its children die with it
    let _ = std::process::Command::new("kill")
        .arg(format!("-{sig}"))
        .arg("--")
        .arg(format!("-{pid}"))
        .stderr(Stdio::null())
        .status();
}

struct Agent {
    a: Args,
    source: Source,
    http: reqwest::Client,
    procs: HashMap<AttemptRef, Proc>,
    /// attempts that exited here; never restarted even if still assigned
    /// (a non-rank-0 member of a gang that finished before rank 0)
    done: BTreeSet<AttemptRef>,
    pending_finished: Vec<Finished>,
    pending_failed: Vec<u32>,
}

impl Agent {
    async fn register(&mut self) {
        let gpus = match &self.source {
            Source::Nvidia(g) => {
                let per = (g.len() as u32 / self.a.islands.max(1)).max(1);
                g.iter()
                    .map(|d| GpuInfo {
                        index: d.index,
                        model: d.name.clone(),
                        island: d.index / per,
                        memory_mb: d.memory_mb,
                        uuid: Some(d.uuid.clone()),
                    })
                    .collect()
            }
            Source::Simulated => {
                let per = (self.a.gpus / self.a.islands.max(1)).max(1);
                (0..self.a.gpus)
                    .map(|i| GpuInfo {
                        index: i,
                        model: self.a.model.clone(),
                        island: i / per,
                        memory_mb: None,
                        uuid: None,
                    })
                    .collect()
            }
        };
        let req = RegisterRequest {
            id: self.a.id.clone(),
            gpus,
        };
        loop {
            let r = self
                .http
                .post(format!("{}/v1/nodes/register", self.a.server))
                .bearer_auth(&self.a.token)
                .json(&req)
                .send()
                .await;
            match r {
                Ok(r) if r.status().is_success() => {
                    tracing::info!("registered as {}", self.a.id);
                    // the control plane requeued whatever ran here before
                    self.stop_all().await;
                    self.done.clear();
                    self.pending_finished.clear();
                    return;
                }
                Ok(r) => tracing::warn!("register: HTTP {}", r.status()),
                Err(e) => tracing::warn!("register: {e}"),
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    /// Kill everything this agent runs. With the docker runtime this also
    /// removes containers labelled with this node that a previous agent
    /// process left behind (after a crash or `kill -9`): the control plane
    /// requeues all of a node's jobs when it registers, so they are orphans.
    async fn stop_all(&mut self) {
        for p in self.procs.values_mut() {
            match &mut p.h {
                Handle::Process { child, pid } => {
                    signal_group(*pid, "KILL");
                    let _ = child.wait().await;
                }
                Handle::Container { name } => runtime::remove(&self.a.docker, name),
            }
        }
        self.procs.clear();
        if self.a.runtime == "docker" {
            match runtime::list(&self.a.docker) {
                Ok(v) => {
                    for c in v {
                        tracing::warn!(container = %c.name, state = %c.state, "removing orphaned container");
                        runtime::remove(&self.a.docker, &c.name);
                    }
                }
                Err(e) => tracing::warn!("listing containers: {e}"),
            }
        }
    }

    fn finish(&mut self, k: AttemptRef, code: i32, reason: String) {
        tracing::info!(job = k.job, attempt = k.attempt, code, reason, "exited");
        self.done.insert(k.clone());
        self.pending_finished.push(Finished {
            job: k.job,
            attempt: k.attempt,
            exit_code: code,
            reason,
        });
    }

    fn reap_containers(&mut self) {
        let cfg = self.a.docker.clone();
        let listed = match runtime::list(&cfg) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!("listing containers: {e}; will retry");
                return;
            }
        };
        let by_name: HashMap<&str, &runtime::Listed> = listed.iter().map(|l| (l.name.as_str(), l)).collect();
        let mut exited = vec![];
        for (k, p) in self.procs.iter_mut() {
            let Handle::Container { name } = &p.h else { continue };
            match by_name.get(name.as_str()) {
                Some(l) if l.is_live() => {
                    if let Some(t) = p.stopping
                        && t.elapsed() > self.a.grace
                    {
                        runtime::signal(&cfg, name, "KILL");
                    }
                }
                Some(_) => {
                    let info = runtime::exit_info(&cfg, name);
                    runtime::save_logs(&cfg, name, &p.log);
                    runtime::remove(&cfg, name);
                    let (code, reason) = match info {
                        Ok(i) if i.oom_killed => (
                            if i.code == 0 { 137 } else { i.code },
                            format!("OOM-killed (memory limit {} MiB)", p.memory_mb),
                        ),
                        // Some Docker/cgroup-v2 setups (e.g. GitHub's runners)
                        // report OOMKilled=false for a cgroup OOM kill. An
                        // unrequested SIGKILL is then the best evidence left.
                        Ok(i) if i.code == 137 && p.stopping.is_none() => (
                            137,
                            format!(
                                "killed by SIGKILL; probable OOM (memory limit {} MiB; Docker did not report OOMKilled)",
                                p.memory_mb
                            ),
                        ),
                        Ok(i) => (i.code, String::new()),
                        Err(e) => (128 + 9, format!("inspect: {e}")),
                    };
                    exited.push((k.clone(), code, reason, p.stopping.is_some()));
                }
                None => exited.push((k.clone(), 128 + 9, "container disappeared".into(), p.stopping.is_some())),
            }
        }
        for (k, code, reason, stopped) in exited {
            self.procs.remove(&k);
            if !stopped {
                self.finish(k, code, reason);
            }
        }
        // labelled containers this agent does not track (e.g. a stop that
        // raced with a crash) are removed
        let ours: BTreeSet<String> = self
            .procs
            .values()
            .filter_map(|p| match &p.h {
                Handle::Container { name } => Some(name.clone()),
                _ => None,
            })
            .collect();
        for l in &listed {
            if !ours.contains(&l.name) {
                tracing::warn!(container = %l.name, "removing untracked container");
                runtime::remove(&cfg, &l.name);
            }
        }
    }

    fn reap(&mut self) {
        if self.a.runtime == "docker" {
            self.reap_containers();
        }
        let mut exited = vec![];
        for (k, p) in self.procs.iter_mut() {
            let Handle::Process { child, pid } = &mut p.h else { continue };
            if let Ok(Some(st)) = child.try_wait() {
                exited.push((k.clone(), st, p.stopping.is_some()));
            } else if let Some(t) = p.stopping
                && t.elapsed() > self.a.grace
            {
                signal_group(*pid, "KILL");
            }
        }
        for (k, st, stopped) in exited {
            self.procs.remove(&k);
            if stopped {
                continue; // we killed it because it is no longer assigned
            }
            self.finish(k, st.code().unwrap_or(128 + 9), String::new());
        }
        if let Ok(rd) = std::fs::read_dir(&self.a.work_dir) {
            for e in rd.flatten() {
                let name = e.file_name().to_string_lossy().to_string();
                if let Some(g) = name.strip_prefix("fail-gpu-").and_then(|x| x.parse::<u32>().ok()) {
                    tracing::warn!(gpu = g, "injected GPU failure");
                    self.pending_failed.push(g);
                    let _ = std::fs::remove_file(e.path());
                }
            }
        }
    }

    fn reconcile(&mut self, resp: HeartbeatResponse) {
        let want: HashMap<AttemptRef, _> = resp
            .assignments
            .into_iter()
            .map(|a| {
                (
                    AttemptRef {
                        job: a.job,
                        attempt: a.attempt,
                    },
                    a,
                )
            })
            .collect();
        for (k, p) in self.procs.iter_mut() {
            if !want.contains_key(k) && p.stopping.is_none() {
                tracing::info!(job = k.job, attempt = k.attempt, "no longer assigned; stopping");
                match &p.h {
                    Handle::Process { pid, .. } => signal_group(*pid, "TERM"),
                    Handle::Container { name } => runtime::signal(&self.a.docker, name, "TERM"),
                }
                p.stopping = Some(Instant::now());
            }
        }
        self.done.retain(|k| want.contains_key(k));
        for (k, a) in want {
            if self.procs.contains_key(&k) || self.done.contains(&k) {
                continue;
            }
            let log = self.a.work_dir.join(format!("job-{}-a{}.log", k.job, k.attempt));
            let spawned = if self.a.runtime == "docker" {
                let _ = std::fs::File::create(&log);
                runtime::start(&self.a.docker, &a).map(|name| Handle::Container { name })
            } else {
                spawn_process(&a, &log).map_err(|e| e.to_string())
            };
            match spawned {
                Ok(h) => {
                    match &h {
                        Handle::Process { pid, .. } => {
                            tracing::info!(job = k.job, attempt = k.attempt, gpus = ?a.gpus, pid, "started")
                        }
                        Handle::Container { name } => {
                            tracing::info!(job = k.job, attempt = k.attempt, gpus = ?a.gpus, container = %name, "started")
                        }
                    }
                    self.procs.insert(
                        k,
                        Proc {
                            h,
                            log,
                            memory_mb: a.memory_mb.unwrap_or(self.a.docker.default_memory_mb),
                            stopping: None,
                        },
                    );
                }
                Err(e) => {
                    tracing::warn!(job = k.job, "spawn failed: {e}");
                    self.finish(k, 127, format!("spawn: {e}"));
                }
            }
        }
    }

    async fn heartbeat(&mut self) -> Result<Option<HeartbeatResponse>, String> {
        let req = HeartbeatRequest {
            running: self.procs.keys().cloned().collect(),
            finished: self.pending_finished.clone(),
            failed_gpus: self.pending_failed.clone(),
        };
        let r = self
            .http
            .post(format!("{}/v1/nodes/{}/heartbeat", self.a.server, self.a.id))
            .bearer_auth(&self.a.token)
            .json(&req)
            .send()
            .await
            .map_err(|e| e.to_string())?;
        if r.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !r.status().is_success() {
            return Err(format!("HTTP {}", r.status()));
        }
        let resp: HeartbeatResponse = r.json().await.map_err(|e| e.to_string())?;
        // delivered: the control plane has these now
        self.pending_finished.clear();
        self.pending_failed.clear();
        Ok(Some(resp))
    }
}

fn spawn_process(a: &Assignment, log: &std::path::Path) -> std::io::Result<Handle> {
    let out = std::fs::File::create(log)?;
    let err = out.try_clone()?;
    let mut cmd = Command::new(a.command.first().ok_or(std::io::Error::other("empty command"))?);
    cmd.args(&a.command[1..])
        .envs(&a.env)
        .stdin(Stdio::null())
        .stdout(out)
        .stderr(err)
        .process_group(0);
    let child = cmd.spawn()?;
    let pid = child.id().unwrap_or(0);
    Ok(Handle::Process { child, pid })
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let mut a = parse();
    std::fs::create_dir_all(&a.work_dir).expect("work dir");
    let source = gpu::detect(&a.gpu_source, &a.nvidia_smi).unwrap_or_else(|e| panic!("GPU detection: {e}"));
    match &source {
        Source::Nvidia(g) => {
            tracing::info!(gpus = g.len(), "using real GPUs from nvidia-smi: {g:?}");
            a.docker.real_gpus = true;
        }
        Source::Simulated => tracing::info!(gpus = a.gpus, "no nvidia-smi; using simulated GPUs"),
    }
    tracing::info!(runtime = %a.runtime, "job runtime");
    let http = reqwest::Client::builder().timeout(Duration::from_secs(5)).build().unwrap();
    let mut ag = Agent {
        a,
        source,
        http,
        procs: HashMap::new(),
        done: BTreeSet::new(),
        pending_finished: vec![],
        pending_failed: vec![],
    };
    ag.register().await;
    let mut iv = tokio::time::interval(ag.a.interval);
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
    let mut int = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()).unwrap();
    loop {
        tokio::select! {
            _ = iv.tick() => {}
            _ = term.recv() => break,
            _ = int.recv() => break,
        }
        ag.reap();
        match ag.heartbeat().await {
            Ok(Some(resp)) => ag.reconcile(resp),
            Ok(None) => ag.register().await,
            Err(e) => tracing::warn!("heartbeat failed: {e}; jobs keep running"),
        }
    }
    tracing::info!("shutting down: stopping jobs and leaving the cluster");
    ag.stop_all().await;
    let _ = ag
        .http
        .delete(format!("{}/v1/nodes/{}", ag.a.server, ag.a.id))
        .bearer_auth(&ag.a.token)
        .send()
        .await;
}
