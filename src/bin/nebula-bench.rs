//! Control-plane load test. Runs `nebulad` in-process on a temporary data
//! directory, registers `--nodes` simulated nodes that heartbeat every
//! `--hb-ms` and "finish" each assigned attempt after `--job-ms`, and has
//! `--clients` tenants submitting 1–8 GPU jobs as fast as they can for
//! `--seconds`. Reports submit and heartbeat latency, throughput, scheduling
//! pass time and journal size.
//!
//! `--rate R` caps the total submission rate (default: unlimited, closed loop).
//!
//! nebula-bench [--nodes 64] [--clients 16] [--seconds 10] [--hb-ms 1000] [--job-ms 2000] [--rate R] [--out FILE]

use nebula::server::{App, AttemptRef, Config, Finished, GpuInfo, HeartbeatRequest, HeartbeatResponse, RegisterRequest, TenantConfig};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

fn pct(v: &mut [f64], p: f64) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v.sort_by(f64::total_cmp);
    v[((v.len() - 1) as f64 * p).round() as usize]
}

#[tokio::main]
async fn main() {
    let mut nodes = 64usize;
    let mut clients = 16usize;
    let mut seconds = 10u64;
    let mut hb_ms = 1000u64;
    let mut job_ms = 2000u64;
    let mut out = None;
    let mut rate = 0f64;
    let a: Vec<String> = std::env::args().skip(1).collect();
    for p in a.chunks(2) {
        let v = p.get(1).expect("missing value").clone();
        match p[0].as_str() {
            "--nodes" => nodes = v.parse().unwrap(),
            "--clients" => clients = v.parse().unwrap(),
            "--seconds" => seconds = v.parse().unwrap(),
            "--hb-ms" => hb_ms = v.parse().unwrap(),
            "--job-ms" => job_ms = v.parse().unwrap(),
            "--out" => out = Some(v),
            "--rate" => rate = v.parse().unwrap(),
            x => panic!("unknown argument {x}"),
        }
    }
    let dir = tempfile_dir();
    let cfg = Config {
        listen: "127.0.0.1:0".into(),
        data_dir: dir.clone(),
        admin_token: "admin".into(),
        node_token: "node".into(),
        heartbeat_timeout_s: 30.0,
        tick_ms: 100,
        policy: Default::default(),
        tenants: (0..4)
            .map(|i| TenantConfig {
                name: format!("t{i}"),
                api_key: format!("k{i}"),
                quota_gpus: 0,
                weight: 1.0,
                max_priority: 0,
            })
            .collect(),
    };
    let app = App::open(cfg).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let srv = app.clone();
    tokio::spawn(async move { srv.serve(listener).await });
    let http = reqwest::Client::builder().pool_max_idle_per_host(256).build().unwrap();
    let stop = Arc::new(AtomicBool::new(false));

    // nodes
    let mut node_tasks = vec![];
    for n in 0..nodes {
        let (http, url, stop) = (http.clone(), url.clone(), stop.clone());
        node_tasks.push(tokio::spawn(async move {
            let id = format!("node-{n:04}");
            let reg = RegisterRequest {
                id: id.clone(),
                gpus: (0..8)
                    .map(|i| GpuInfo {
                        index: i,
                        memory_mb: None,
                        uuid: None,
                        model: "h100".into(),
                        island: i / 4,
                    })
                    .collect(),
            };
            http.post(format!("{url}/v1/nodes/register"))
                .bearer_auth("node")
                .json(&reg)
                .send()
                .await
                .unwrap();
            let mut started: HashMap<AttemptRef, Instant> = HashMap::new();
            let mut finished: Vec<Finished> = vec![];
            let mut lat = vec![];
            // spread heartbeats over the interval
            tokio::time::sleep(Duration::from_millis(hb_ms * n as u64 / nodes as u64)).await;
            let mut iv = tokio::time::interval(Duration::from_millis(hb_ms));
            while !stop.load(Ordering::Relaxed) {
                iv.tick().await;
                let done: Vec<AttemptRef> = started
                    .iter()
                    .filter(|(_, t)| t.elapsed() >= Duration::from_millis(job_ms))
                    .map(|(k, _)| k.clone())
                    .collect();
                for k in done {
                    started.remove(&k);
                    finished.push(Finished {
                        job: k.job,
                        attempt: k.attempt,
                        exit_code: 0,
                        reason: String::new(),
                    });
                }
                let req = HeartbeatRequest {
                    running: started.keys().cloned().collect(),
                    finished: std::mem::take(&mut finished),
                    failed_gpus: vec![],
                };
                let t0 = Instant::now();
                let r: HeartbeatResponse = http
                    .post(format!("{url}/v1/nodes/{id}/heartbeat"))
                    .bearer_auth("node")
                    .json(&req)
                    .send()
                    .await
                    .unwrap()
                    .json()
                    .await
                    .unwrap();
                lat.push(t0.elapsed().as_secs_f64() * 1e3);
                let want: std::collections::HashSet<AttemptRef> = r
                    .assignments
                    .iter()
                    .map(|a| AttemptRef {
                        job: a.job,
                        attempt: a.attempt,
                    })
                    .collect();
                started.retain(|k, _| want.contains(k));
                for k in want {
                    started.entry(k).or_insert_with(Instant::now);
                }
            }
            lat
        }));
    }
    tokio::time::sleep(Duration::from_millis(hb_ms + 200)).await;

    // tenants
    let t_start = Instant::now();
    let mut client_tasks = vec![];
    for c in 0..clients {
        let (http, url, stop) = (http.clone(), url.clone(), stop.clone());
        client_tasks.push(tokio::spawn(async move {
            let mut lat = vec![];
            let mut i = 0u32;
            let gap = if rate > 0.0 {
                Some(Duration::from_secs_f64(clients as f64 / rate))
            } else {
                None
            };
            let mut iv = tokio::time::interval(gap.unwrap_or(Duration::from_millis(1)));
            while !stop.load(Ordering::Relaxed) {
                if gap.is_some() {
                    iv.tick().await;
                }
                i += 1;
                let gpus = [1, 1, 1, 2, 4, 8][(i as usize + c) % 6];
                let t0 = Instant::now();
                let r = http
                    .post(format!("{url}/v1/jobs"))
                    .bearer_auth(format!("k{}", c % 4))
                    .json(&json!({"gpus": gpus, "est_runtime": 2.0, "command": ["true"]}))
                    .send()
                    .await
                    .unwrap();
                assert_eq!(r.status(), 201);
                lat.push(t0.elapsed().as_secs_f64() * 1e3);
            }
            lat
        }));
    }
    tokio::time::sleep(Duration::from_secs(seconds)).await;
    stop.store(true, Ordering::Relaxed);
    let elapsed = t_start.elapsed().as_secs_f64();
    let mut sub = vec![];
    for t in client_tasks {
        sub.extend(t.await.unwrap());
    }
    let mut hb = vec![];
    for t in node_tasks {
        hb.extend(t.await.unwrap());
    }
    let g = app.inner.lock();
    let c = &g.cluster;
    let done = c
        .jobs
        .values()
        .filter(|j| matches!(j.state, nebula::model::JobState::Succeeded { .. }))
        .count();
    let journal = std::fs::metadata(dir.join("journal.bin")).map(|m| m.len()).unwrap_or(0);
    let snapshot = std::fs::metadata(dir.join("snapshot.json")).map(|m| m.len()).unwrap_or(0);
    let res = json!({
        "nodes": nodes, "gpus": nodes * 8, "clients": clients, "seconds": elapsed, "heartbeat_interval_ms": hb_ms, "rate_limit_per_s": rate, "job_ms": job_ms,
        "submits": sub.len(), "submits_per_s": sub.len() as f64 / elapsed,
        "submit_ms": {"p50": pct(&mut sub, 0.5), "p99": pct(&mut sub, 0.99), "max": pct(&mut sub, 1.0)},
        "heartbeats": hb.len(), "heartbeats_per_s": hb.len() as f64 / elapsed,
        "heartbeat_ms": {"p50": pct(&mut hb, 0.5), "p99": pct(&mut hb, 0.99), "max": pct(&mut hb, 1.0)},
        "jobs_succeeded": done, "jobs_queued_at_end": c.jobs.values().filter(|j| j.is_queued()).count(),
        "sched_passes": g.metrics.sched_passes, "sched_mean_ms": 1e3 * g.metrics.sched_seconds_total / g.metrics.sched_passes.max(1) as f64,
        "journal_bytes": journal, "snapshot_bytes": snapshot,
    });
    println!("{}", serde_json::to_string_pretty(&res).unwrap());
    if let Some(p) = out {
        std::fs::write(p, serde_json::to_string_pretty(&res).unwrap()).unwrap();
    }
    let _ = std::fs::remove_dir_all(&dir);
    std::process::exit(0);
}

fn tempfile_dir() -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("nebula-bench-{}", std::process::id()));
    std::fs::create_dir_all(&d).unwrap();
    d
}
