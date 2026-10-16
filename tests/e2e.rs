//! End-to-end: real `nebulad` and `nebula-agent` processes on loopback,
//! jobs that are real shell commands.

use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

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

struct Env {
    dir: PathBuf,
    url: String,
    port: u16,
    http: reqwest::Client,
}

const ADMIN: &str = "admin-secret";
const NODE: &str = "node-secret";

impl Env {
    fn config(&self) -> PathBuf {
        let p = self.dir.join("nebulad.json");
        std::fs::write(
            &p,
            serde_json::to_vec_pretty(&json!({
                "listen": format!("127.0.0.1:{}", self.port),
                "data_dir": self.dir.join("state"),
                "admin_token": ADMIN,
                "node_token": NODE,
                "heartbeat_timeout_s": 2.0,
                "tick_ms": 100,
                "tenants": [
                    {"name": "research", "api_key": "key-research", "weight": 1.0},
                    {"name": "prod", "api_key": "key-prod", "weight": 2.0, "max_priority": 10},
                ]
            }))
            .unwrap(),
        )
        .unwrap();
        p
    }

    async fn start_server(&self) -> Proc {
        let log = std::fs::File::create(self.dir.join("nebulad.log")).unwrap();
        let p = Proc(
            Command::new(env!("CARGO_BIN_EXE_nebulad"))
                .arg("--config")
                .arg(self.config())
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

    fn start_agent(&self, id: &str) -> Proc {
        let wd = self.workdir(id);
        std::fs::create_dir_all(&wd).unwrap();
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.dir.join(format!("{id}.log")))
            .unwrap();
        Proc(
            Command::new(env!("CARGO_BIN_EXE_nebula-agent"))
                .args([
                    "--server",
                    &self.url,
                    "--token",
                    NODE,
                    "--id",
                    id,
                    "--gpus",
                    "8",
                    "--islands",
                    "2",
                    "--interval-ms",
                    "150",
                ])
                .arg("--work-dir")
                .arg(&wd)
                .stdout(Stdio::null())
                .stderr(log)
                .spawn()
                .unwrap(),
        )
    }

    fn workdir(&self, id: &str) -> PathBuf {
        self.dir.join(format!("work-{id}"))
    }

    async fn wait_for<F, Fut>(&self, mut f: F, what: &str)
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        let t0 = Instant::now();
        while !f().await {
            assert!(
                t0.elapsed() < Duration::from_secs(30),
                "timed out waiting for {what}; logs in {}",
                self.dir.display()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn submit(&self, key: &str, body: Value) -> reqwest::Response {
        self.http
            .post(format!("{}/v1/jobs", self.url))
            .bearer_auth(key)
            .json(&body)
            .send()
            .await
            .unwrap()
    }

    async fn submit_ok(&self, key: &str, body: Value) -> u64 {
        let r = self.submit(key, body).await;
        assert_eq!(r.status(), 201, "{:?}", r.text().await);
        r.json::<Value>().await.unwrap()["id"].as_u64().unwrap()
    }

    async fn job(&self, key: &str, id: u64) -> Value {
        self.http
            .get(format!("{}/v1/jobs/{id}", self.url))
            .bearer_auth(key)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap()
    }

    async fn wait_state(&self, key: &str, id: u64, state: &str) -> Value {
        let t0 = Instant::now();
        loop {
            let j = self.job(key, id).await;
            if j["state"]["state"] == state {
                return j;
            }
            if t0.elapsed() > Duration::from_secs(30) {
                panic!("job {id} never reached {state}: {j}; logs in {}", self.dir.display());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
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
}

fn sh(script: &str) -> Value {
    json!(["sh", "-c", script])
}

fn read(p: &Path) -> String {
    std::fs::read_to_string(p).unwrap_or_default()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn control_plane_end_to_end() {
    let tmp = tempfile::tempdir().unwrap();
    let port = free_port();
    let e = Env {
        dir: tmp.path().to_path_buf(),
        url: format!("http://127.0.0.1:{port}"),
        port,
        http: reqwest::Client::new(),
    };
    let out = e.dir.join("out");
    std::fs::create_dir_all(&out).unwrap();
    let o = out.display().to_string();

    let mut server = e.start_server().await;
    let _a1 = e.start_agent("n1");
    let mut a2 = Some(e.start_agent("n2"));
    e.wait_for(|| async { e.cluster().await["gpus_total"] == 16 }, "two nodes registered")
        .await;

    // --- auth and validation
    assert_eq!(
        e.submit("wrong", json!({"gpus": 1, "est_runtime": 5, "command": ["true"]}))
            .await
            .status(),
        401
    );
    assert_eq!(
        e.submit(
            "key-research",
            json!({"gpus": 1, "est_runtime": 5, "command": ["true"], "priority": 5})
        )
        .await
        .status(),
        403
    );
    assert_eq!(
        e.submit("key-research", json!({"gpus": 17, "est_runtime": 5, "command": ["true"]}))
            .await
            .status(),
        422
    );
    assert_eq!(
        e.submit("key-research", json!({"gpus": 1, "est_runtime": 5, "command": []}))
            .await
            .status(),
        400
    );

    // --- a 4-GPU job gets one NVLink island and the right environment
    let j = e.submit_ok("key-research", json!({"gpus": 4, "est_runtime": 5, "command": sh(&format!("echo $CUDA_VISIBLE_DEVICES $NEBULA_JOB_ID $NEBULA_ATTEMPT $NEBULA_TENANT > {o}/single"))})).await;
    e.wait_state("key-research", j, "succeeded").await;
    let s = read(&out.join("single"));
    assert!(s.starts_with("0,1,2,3 ") || s.starts_with("4,5,6,7 "), "{s}");
    assert!(s.trim_end().ends_with(&format!("{j} 0 research")), "{s}");
    let other = e
        .http
        .get(format!("{}/v1/jobs/{j}", e.url))
        .bearer_auth("key-prod")
        .send()
        .await
        .unwrap();
    assert_eq!(other.status(), 404, "tenants cannot see each other's jobs");

    // --- a 12-GPU gang job spans both nodes: 8 + 4, ranks 0 and 1
    let gang = e
        .submit_ok("key-research", json!({"gpus": 12, "est_runtime": 5, "command": sh(&format!("echo $NEBULA_NUM_NODES $CUDA_VISIBLE_DEVICES > {o}/gang-$NEBULA_NODE_RANK; sleep 0.3"))}))
        .await;
    e.wait_state("key-research", gang, "succeeded").await;
    let (r0, r1) = (read(&out.join("gang-0")), read(&out.join("gang-1")));
    let n_gpus = |s: &str| s.split_whitespace().nth(1).unwrap().split(',').count();
    assert!(r0.starts_with("2 ") && r1.starts_with("2 "), "{r0} {r1}");
    let mut sizes = [n_gpus(&r0), n_gpus(&r1)];
    sizes.sort();
    assert_eq!(sizes, [4, 8]);

    // --- a failing command fails the job with its exit code
    let bad = e
        .submit_ok("key-research", json!({"gpus": 1, "est_runtime": 5, "command": sh("exit 3")}))
        .await;
    let jb = e.wait_state("key-research", bad, "failed").await;
    assert!(jb["state"]["reason"].as_str().unwrap().contains("exit code 3"), "{jb}");

    // --- preemption: a high-priority job evicts a low-priority one, which restarts later
    let low = e
        .submit_ok("key-research", json!({"gpus": 16, "est_runtime": 60, "command": sh(&format!("echo start $NEBULA_ATTEMPT >> {o}/low; sleep 4; echo end $NEBULA_ATTEMPT >> {o}/low"))}))
        .await;
    e.wait_for(|| async { read(&out.join("low")).contains("start 0") }, "low-priority job running")
        .await;
    let hi = e
        .submit_ok(
            "key-prod",
            json!({"gpus": 8, "est_runtime": 5, "priority": 10, "preemptible": false, "command": sh("sleep 0.3")}),
        )
        .await;
    e.wait_state("key-prod", hi, "succeeded").await;
    let jl = e.wait_state("key-research", low, "succeeded").await;
    assert_eq!(jl["restarts"], 1);
    let log = read(&out.join("low"));
    // two ranks (8 + 8 GPUs) per attempt; attempt 0 was killed before its end
    assert_eq!(
        log.lines().collect::<Vec<_>>(),
        vec!["start 0", "start 0", "start 1", "start 1", "end 1", "end 1"],
        "{log}"
    );
    // 4 s + the preempted part of attempt 0 is metered for 16 GPUs
    assert!(jl["gpu_seconds"].as_f64().unwrap() >= 16.0 * 4.0, "{jl}");

    // --- node failure: kill an agent; its job is requeued onto the other node
    let drain = |m: reqwest::Method| {
        e.http
            .request(m, format!("{}/v1/admin/nodes/n1/drain", e.url))
            .bearer_auth(ADMIN)
            .send()
    };
    assert_eq!(drain(reqwest::Method::POST).await.unwrap().status(), 204);
    let lj = e
        .submit_ok(
            "key-research",
            json!({"gpus": 8, "est_runtime": 60, "command": sh(&format!("echo $NEBULA_ATTEMPT >> {o}/nf; sleep 3"))}),
        )
        .await;
    let running = e.wait_state("key-research", lj, "running").await;
    assert_eq!(running["state"]["placement"][0]["node"], "n2", "n1 is draining");
    assert_eq!(drain(reqwest::Method::DELETE).await.unwrap().status(), 204);
    drop(a2.take()); // SIGKILL: no goodbye, heartbeats just stop
    let t_kill = Instant::now();
    e.wait_for(
        || async { e.job("key-research", lj).await["restarts"] == 1 },
        "requeue after heartbeat timeout",
    )
    .await;
    let waited = t_kill.elapsed();
    assert!(waited >= Duration::from_millis(1500), "requeued before the 2 s timeout: {waited:?}");
    let j2 = e.wait_state("key-research", lj, "succeeded").await;
    assert_eq!(j2["restarts"], 1);
    let c = e.cluster().await;
    let down: Vec<&Value> = c["nodes"].as_array().unwrap().iter().filter(|n| n["state"] == "down").collect();
    assert_eq!(down.len(), 1);
    // the agent comes back and its node is usable again
    a2 = Some(e.start_agent("n2"));
    e.wait_for(|| async { e.cluster().await["gpus_total"] == 16 }, "n2 back").await;

    // --- GPU failure on a running job: requeued, GPU taken out of service
    let gj = e
        .submit_ok("key-research", json!({"gpus": 8, "est_runtime": 60, "command": sh("sleep 2")}))
        .await;
    let running = e.wait_state("key-research", gj, "running").await;
    let node = running["state"]["placement"][0]["node"].as_str().unwrap().to_string();
    std::fs::write(e.workdir(&node).join("fail-gpu-5"), "").unwrap();
    e.wait_for(
        || async { e.job("key-research", gj).await["restarts"] == 1 },
        "requeue after GPU failure",
    )
    .await;
    let jg = e.wait_state("key-research", gj, "succeeded").await;
    let pl = &jg["state"];
    assert!(pl.get("placement").is_none());
    let c = e.cluster().await;
    assert_eq!(c["gpus_total"], 15);
    let n = c["nodes"].as_array().unwrap().iter().find(|n| n["id"] == node).unwrap();
    assert_eq!(n["gpus"][5]["healthy"], false);

    // --- control-plane restart: state survives, running jobs keep running
    let rj = e
        .submit_ok(
            "key-research",
            json!({"gpus": 2, "est_runtime": 60, "command": sh(&format!("echo go >> {o}/restart; sleep 3"))}),
        )
        .await;
    e.wait_state("key-research", rj, "running").await;
    let before = e
        .http
        .get(format!("{}/v1/jobs", e.url))
        .bearer_auth("key-research")
        .send()
        .await
        .unwrap()
        .json::<Vec<Value>>()
        .await
        .unwrap();
    Command::new("kill").arg("-TERM").arg(server.0.id().to_string()).status().unwrap();
    server.0.wait().unwrap();
    server = e.start_server().await;
    let after = e
        .http
        .get(format!("{}/v1/jobs", e.url))
        .bearer_auth("key-research")
        .send()
        .await
        .unwrap()
        .json::<Vec<Value>>()
        .await
        .unwrap();
    assert_eq!(before.len(), after.len());
    for (b, a) in before.iter().zip(&after) {
        assert_eq!(b["id"], a["id"]);
        assert_eq!(b["state"]["state"], a["state"]["state"]);
        assert_eq!(b["restarts"], a["restarts"]);
    }
    let jr = e.wait_state("key-research", rj, "succeeded").await;
    assert_eq!(jr["restarts"], 0, "the job ran through the control-plane restart");
    assert_eq!(read(&out.join("restart")).lines().count(), 1);

    // --- usage and metrics
    let u: Value = e
        .http
        .get(format!("{}/v1/usage", e.url))
        .bearer_auth("key-research")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(u["gpu_hours"].as_f64().unwrap() > 0.0);
    assert_eq!(u["jobs"]["failed"], 1);
    let m = e.http.get(format!("{}/metrics", e.url)).send().await.unwrap().text().await.unwrap();
    assert!(m.contains("nebula_gpus_total 15"), "{m}");
    drop(server);
    drop(a2);
}

#[test]
fn example_config_parses() {
    let c: nebula::server::Config =
        serde_json::from_slice(&std::fs::read(concat!(env!("CARGO_MANIFEST_DIR"), "/configs/example.json")).unwrap()).unwrap();
    assert_eq!(c.policy.order, nebula::sched::Order::FairShare);
    assert!(c.policy.backfill && c.policy.preemption);
    assert_eq!(c.tenants.len(), 2);
}
