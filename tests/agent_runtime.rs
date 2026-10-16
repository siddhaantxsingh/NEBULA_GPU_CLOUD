//! The agent's container runtime (`--runtime docker`) against the real Docker
//! daemon, and real-GPU detection through a stand-in `nvidia-smi`.
//!
//! The Docker tests need a working `docker` CLI and daemon and the
//! `alpine:3.20` image. Without them they print a message and pass, unless
//! `NEBULA_REQUIRE_DOCKER=1` is set (CI sets it), in which case they fail.

use serde_json::{Value, json};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const ADMIN: &str = "admin-secret";
const NODE: &str = "node-secret";
const KEY: &str = "key-research";
const IMAGE: &str = "alpine:3.20";

struct Proc(Child);
impl Drop for Proc {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

fn docker_ok() -> bool {
    let ok = Command::new("docker")
        .args(["image", "inspect", IMAGE])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
        || Command::new("docker")
            .args(["pull", IMAGE])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|s| s.success());
    if !ok {
        assert!(
            std::env::var("NEBULA_REQUIRE_DOCKER").is_err(),
            "NEBULA_REQUIRE_DOCKER is set but docker or {IMAGE} is unavailable"
        );
        eprintln!("SKIPPED: docker daemon or {IMAGE} not available");
    }
    ok
}

fn docker(args: &[&str]) -> String {
    let o = Command::new("docker").args(args).output().unwrap();
    String::from_utf8_lossy(&o.stdout).trim().to_string()
}

/// names of all containers labelled with this node
fn containers(node: &str) -> Vec<String> {
    docker(&[
        "ps",
        "--all",
        "--filter",
        &format!("label=nebula.node={node}"),
        "--format",
        "{{.Names}}",
    ])
    .lines()
    .map(str::to_string)
    .collect()
}

struct Env {
    dir: PathBuf,
    url: String,
    port: u16,
    node: String,
    http: reqwest::Client,
    _tmp: tempfile::TempDir,
}

impl Drop for Env {
    fn drop(&mut self) {
        for c in containers(&self.node) {
            docker(&["rm", "--force", &c]);
        }
    }
}

impl Env {
    fn new(tag: &str) -> Env {
        let tmp = tempfile::tempdir().unwrap();
        let port = free_port();
        Env {
            dir: tmp.path().to_path_buf(),
            url: format!("http://127.0.0.1:{port}"),
            port,
            node: format!("test-{tag}-{port}"),
            http: reqwest::Client::new(),
            _tmp: tmp,
        }
    }

    async fn start_server(&self) -> Proc {
        let cfg = self.dir.join("nebulad.json");
        std::fs::write(
            &cfg,
            serde_json::to_vec(&json!({
                "listen": format!("127.0.0.1:{}", self.port),
                "data_dir": self.dir.join("state"),
                "admin_token": ADMIN,
                "node_token": NODE,
                "heartbeat_timeout_s": 2.0,
                "tick_ms": 100,
                "tenants": [{"name": "research", "api_key": KEY, "weight": 1.0}]
            }))
            .unwrap(),
        )
        .unwrap();
        let log = std::fs::File::create(self.dir.join("nebulad.log")).unwrap();
        let p = Proc(
            Command::new(env!("CARGO_BIN_EXE_nebulad"))
                .arg("--config")
                .arg(cfg)
                .stdout(Stdio::null())
                .stderr(log)
                .spawn()
                .unwrap(),
        );
        self.wait_for(
            || async { self.http.get(format!("{}/healthz", self.url)).send().await.is_ok() },
            "server up",
        )
        .await;
        p
    }

    fn workdir(&self) -> PathBuf {
        self.dir.join("work")
    }

    fn start_agent(&self, extra: &[&str]) -> Proc {
        std::fs::create_dir_all(self.workdir()).unwrap();
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.dir.join("agent.log"))
            .unwrap();
        Proc(
            Command::new(env!("CARGO_BIN_EXE_nebula-agent"))
                .args(["--server", &self.url, "--token", NODE, "--id", &self.node])
                .args(["--gpus", "4", "--islands", "1", "--interval-ms", "150"])
                .arg("--work-dir")
                .arg(self.workdir())
                .args(extra)
                .stdout(log.try_clone().unwrap())
                .stderr(log)
                .spawn()
                .unwrap(),
        )
    }

    fn docker_agent(&self) -> Proc {
        self.start_agent(&[
            "--runtime",
            "docker",
            "--gpu-source",
            "sim",
            "--image",
            IMAGE,
            "--stop-grace-s",
            "2",
        ])
    }

    async fn wait_for<F, Fut>(&self, mut f: F, what: &str)
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        let t0 = Instant::now();
        while !f().await {
            assert!(
                t0.elapsed() < Duration::from_secs(60),
                "timed out waiting for {what}; logs in {}:\n{}",
                self.dir.display(),
                std::fs::read_to_string(self.dir.join("agent.log")).unwrap_or_default()
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn submit(&self, body: Value) -> u64 {
        let r = self
            .http
            .post(format!("{}/v1/jobs", self.url))
            .bearer_auth(KEY)
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 201, "{:?}", r.text().await);
        r.json::<Value>().await.unwrap()["id"].as_u64().unwrap()
    }

    async fn job(&self, id: u64) -> Value {
        self.http
            .get(format!("{}/v1/jobs/{id}", self.url))
            .bearer_auth(KEY)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap()
    }

    async fn wait_state(&self, id: u64, state: &str) -> Value {
        let t0 = Instant::now();
        loop {
            let j = self.job(id).await;
            if j["state"]["state"] == state {
                return j;
            }
            assert!(
                t0.elapsed() < Duration::from_secs(60),
                "job {id} never reached {state}: {j}; logs in {}:\n{}",
                self.dir.display(),
                std::fs::read_to_string(self.dir.join("agent.log")).unwrap_or_default()
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn cluster(&self) -> Value {
        self.http
            .get(format!("{}/v1/admin/cluster", self.url))
            .bearer_auth(ADMIN)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap()
    }

    fn job_log(&self, job: u64, attempt: u32) -> String {
        std::fs::read_to_string(self.workdir().join(format!("job-{job}-a{attempt}.log"))).unwrap_or_default()
    }
}

fn inspect(name: &str, fmt: &str) -> String {
    docker(&["inspect", "--format", fmt, name])
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn container_limits_hardening_and_cancel() {
    if !docker_ok() {
        return;
    }
    let e = Env::new("limits");
    let _s = e.start_server().await;
    let _a = e.docker_agent();
    e.wait_for(|| async { e.cluster().await["gpus_total"] == 4 }, "node registered")
        .await;

    // 1. a job that inspects its own sandbox and exits 0
    let probe = r#"id -u; id -g
        touch /rootfs-write 2>/dev/null && echo rootfs-writable || echo rootfs-read-only
        grep CapEff /proc/self/status
        grep NoNewPrivs /proc/self/status
        ls /sys/class/net | tr '\n' ' '; echo
        echo "cuda=$CUDA_VISIBLE_DEVICES job=$NEBULA_JOB_ID"
        echo tmp-ok > /tmp/x && cat /tmp/x"#;
    let j = e
        .submit(json!({"gpus": 2, "est_runtime": 5, "cpus": 0.5, "memory_mb": 64, "command": ["sh", "-c", probe]}))
        .await;
    e.wait_state(j, "succeeded").await;
    let log = e.job_log(j, 0);
    let lines: Vec<&str> = log.lines().collect();
    assert_eq!(&lines[..2], &["65534", "65534"], "non-root user: {log}");
    assert!(log.contains("rootfs-read-only"), "{log}");
    assert!(log.contains("CapEff:\t0000000000000000"), "all capabilities dropped: {log}");
    assert!(log.contains("NoNewPrivs:\t1"), "{log}");
    assert!(log.contains("\nlo \n"), "network none: only loopback: {log}");
    let cuda = log.lines().find_map(|l| l.strip_prefix("cuda=")).unwrap();
    assert!(cuda.ends_with(&format!(" job={j}")), "{log}");
    assert_eq!(cuda.split(' ').next().unwrap().split(',').count(), 2, "{log}");
    assert!(log.contains("tmp-ok"), "{log}");
    // the finished container was removed
    assert!(containers(&e.node).is_empty(), "{:?}", containers(&e.node));

    // 2. limits as Docker applied them (inspect a running container)
    let long = e
        .submit(json!({"gpus": 1, "est_runtime": 60, "cpus": 0.5, "memory_mb": 64,
                       "command": ["sh", "-c", "trap 'echo got-sigterm; exit 0' TERM; while true; do sleep 0.1; done"]}))
        .await;
    e.wait_state(long, "running").await;
    let name = format!("nebula-{}-j{long}-a0", e.node);
    e.wait_for(|| async { inspect(&name, "{{.State.Status}}") == "running" }, "container running")
        .await;
    let hc = inspect(
        &name,
        "{{.HostConfig.NanoCpus}} {{.HostConfig.Memory}} {{.HostConfig.MemorySwap}} {{.HostConfig.ReadonlyRootfs}} {{.HostConfig.CapDrop}} {{.HostConfig.SecurityOpt}} {{.HostConfig.NetworkMode}} {{.HostConfig.PidsLimit}} {{.Config.User}}",
    );
    assert_eq!(
        hc,
        "500000000 67108864 67108864 true [ALL] [no-new-privileges] none 256 65534:65534"
    );
    let labels = inspect(
        &name,
        r#"{{index .Config.Labels "nebula.node"}} {{index .Config.Labels "nebula.job"}} {{index .Config.Labels "nebula.attempt"}}"#,
    );
    assert_eq!(labels, format!("{} {long} 0", e.node));

    // 3. cancel: SIGTERM reaches the job, the container is stopped and removed
    let t0 = Instant::now();
    let r = e
        .http
        .delete(format!("{}/v1/jobs/{long}", e.url))
        .bearer_auth(KEY)
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success(), "{}", r.status());
    e.wait_for(|| async { containers(&e.node).is_empty() }, "cancelled container removed")
        .await;
    let took = t0.elapsed();
    assert!(e.job_log(long, 0).contains("got-sigterm"), "{}", e.job_log(long, 0));
    eprintln!("cancel -> container removed in {took:?} (job handles SIGTERM)");

    // 4. a job that ignores SIGTERM (PID 1 without a handler) is SIGKILLed
    //    after the 2 s grace period
    let stubborn = e.submit(json!({"gpus": 1, "est_runtime": 60, "command": ["sleep", "600"]})).await;
    e.wait_state(stubborn, "running").await;
    let name = format!("nebula-{}-j{stubborn}-a0", e.node);
    e.wait_for(|| async { inspect(&name, "{{.State.Status}}") == "running" }, "container running")
        .await;
    let t0 = Instant::now();
    e.http
        .delete(format!("{}/v1/jobs/{stubborn}", e.url))
        .bearer_auth(KEY)
        .send()
        .await
        .unwrap();
    e.wait_for(|| async { containers(&e.node).is_empty() }, "stubborn container removed")
        .await;
    let took = t0.elapsed();
    eprintln!("cancel -> SIGTERM-ignoring container removed in {took:?}");
    assert!(took >= Duration::from_secs(2), "killed before the grace period: {took:?}");
    assert!(took < Duration::from_secs(15), "{took:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn container_oom_kill_fails_the_job() {
    if !docker_ok() {
        return;
    }
    let e = Env::new("oom");
    let _s = e.start_server().await;
    let _a = e.docker_agent();
    e.wait_for(|| async { e.cluster().await["gpus_total"] == 4 }, "node registered")
        .await;
    // `tail /dev/zero` buffers an endless line and grows until the limit
    let j = e
        .submit(json!({"gpus": 1, "est_runtime": 5, "memory_mb": 16, "command": ["tail", "/dev/zero"]}))
        .await;
    let v = e.wait_state(j, "failed").await;
    let reason = v["state"]["reason"].as_str().unwrap();
    eprintln!("OOM job reason: {reason}");
    assert!(reason.contains("exit code 137"), "{reason}");
    // cgroup v1 hosts (this machine) report OOMKilled; some cgroup v2 hosts
    // (GitHub's runners) do not, and the agent falls back to "probable OOM"
    assert!(
        reason.contains("OOM-killed (memory limit 16 MiB)") || reason.contains("probable OOM (memory limit 16 MiB"),
        "{reason}"
    );
    // a job within its limit on the same node still succeeds
    let ok = e
        .submit(json!({"gpus": 1, "est_runtime": 5, "memory_mb": 16, "command": ["true"]}))
        .await;
    e.wait_state(ok, "succeeded").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn orphaned_containers_are_removed_after_agent_kill9() {
    if !docker_ok() {
        return;
    }
    let e = Env::new("orphan");
    let _s = e.start_server().await;
    let mut a = e.docker_agent();
    e.wait_for(|| async { e.cluster().await["gpus_total"] == 4 }, "node registered")
        .await;
    let j = e.submit(json!({"gpus": 1, "est_runtime": 600, "command": ["sleep", "600"]})).await;
    e.wait_state(j, "running").await;
    let first = format!("nebula-{}-j{j}-a0", e.node);
    e.wait_for(|| async { inspect(&first, "{{.State.Status}}") == "running" }, "container running")
        .await;

    // SIGKILL the agent: it cannot clean up, the container keeps running
    a.0.kill().unwrap();
    a.0.wait().unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(inspect(&first, "{{.State.Status}}"), "running", "orphan should survive the agent");

    // a new agent with the same node id registers; the control plane
    // requeues the job, the agent removes the orphan and runs attempt 1
    let _a2 = e.docker_agent();
    let second = format!("nebula-{}-j{j}-a1", e.node);
    e.wait_for(
        || async { containers(&e.node) == vec![second.clone()] },
        "orphan removed and attempt 1 running",
    )
    .await;
    assert_eq!(e.job(j).await["restarts"], 1);
    let log = std::fs::read_to_string(e.dir.join("agent.log")).unwrap();
    assert!(log.contains("removing orphaned container"), "{log}");
}

/// Real-GPU path, exercised with a stand-in nvidia-smi script that prints the
/// documented CSV format. This checks the agent's wiring, not real hardware.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn agent_reports_gpus_from_nvidia_smi() {
    let e = Env::new("nvsmi");
    let fake = e.dir.join("nvidia-smi");
    std::fs::write(
        &fake,
        "#!/bin/sh\necho '0, NVIDIA GeForce RTX 4060, 8188, GPU-3f5c2a1e-8d1b-4c3e-9a77-0b6f4e2d9c11'\n",
    )
    .unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
    let _s = e.start_server().await;
    let _a = e.start_agent(&["--nvidia-smi", fake.to_str().unwrap()]);
    e.wait_for(|| async { e.cluster().await["gpus_total"] == 1 }, "node with 1 real GPU")
        .await;
    let c = e.cluster().await;
    let g = &c["nodes"][0]["gpus"][0];
    assert_eq!(g["model"], "NVIDIA GeForce RTX 4060");
    assert_eq!(g["memory_mb"], 8188);
    assert_eq!(g["uuid"], "GPU-3f5c2a1e-8d1b-4c3e-9a77-0b6f4e2d9c11");
    // a job asking for that model is placed on it (process runtime)
    let j = e
        .submit(json!({"gpus": 1, "gpu_model": "NVIDIA GeForce RTX 4060", "est_runtime": 5,
                       "command": ["sh", "-c", "echo cuda=$CUDA_VISIBLE_DEVICES"]}))
        .await;
    e.wait_state(j, "succeeded").await;
    assert_eq!(e.job_log(j, 0).trim(), "cuda=0");
}
