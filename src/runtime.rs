//! Docker container runtime for the node agent (`--runtime docker`).
//!
//! Each job attempt runs as one container, created with the `docker` CLI:
//!
//! * CPU and memory limits from the job spec (`cpus`, `memory_mb`), or the
//!   agent's defaults; swap is disabled by setting `--memory-swap` equal to
//!   `--memory`, so exceeding the limit is an OOM kill;
//! * a read-only root filesystem with a small `/tmp` tmpfs;
//! * `no-new-privileges`, all capabilities dropped, a non-root user, a PID
//!   limit, and no network unless the agent is configured otherwise;
//! * `--gpus device=<indices>` only when the agent found real NVIDIA GPUs;
//! * labels (`nebula.node`, `nebula.job`, `nebula.attempt`) so the agent can
//!   find its containers again after a crash and remove the ones it does not
//!   own any more.
//!
//! The container is not started with `--rm`: the agent reads its exit code
//! and OOM flag with `docker inspect`, copies its logs, then removes it.

use crate::server::Assignment;
use std::path::Path;
use std::process::{Command, Stdio};

pub const LABEL_NODE: &str = "nebula.node";
pub const LABEL_JOB: &str = "nebula.job";
pub const LABEL_ATTEMPT: &str = "nebula.attempt";

#[derive(Clone, Debug)]
pub struct DockerConfig {
    /// path of the docker CLI
    pub docker: String,
    pub node: String,
    pub default_image: String,
    pub default_cpus: f64,
    pub default_memory_mb: u64,
    /// `none` (default), `bridge`, or a user-defined network
    pub network: String,
    /// `uid:gid`; must not be root
    pub user: String,
    pub pids_limit: u32,
    pub tmpfs_mb: u64,
    /// pass the assigned GPUs through with `--gpus device=...`
    pub real_gpus: bool,
}

impl DockerConfig {
    pub fn new(node: &str) -> DockerConfig {
        DockerConfig {
            docker: "docker".into(),
            node: node.into(),
            default_image: "alpine:3.20".into(),
            default_cpus: 1.0,
            default_memory_mb: 512,
            network: "none".into(),
            user: "65534:65534".into(),
            pids_limit: 256,
            tmpfs_mb: 64,
            real_gpus: false,
        }
    }
}

/// Container name for one attempt. The node id is reduced to the characters
/// Docker allows in names.
pub fn container_name(node: &str, job: u64, attempt: u32) -> String {
    let n: String = node
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect();
    format!("nebula-{n}-j{job}-a{attempt}")
}

/// The full `docker run` argument list (without the leading `docker`).
pub fn run_args(cfg: &DockerConfig, a: &Assignment) -> Result<Vec<String>, String> {
    if a.command.is_empty() {
        return Err("empty command".into());
    }
    let uid = cfg.user.split(':').next().unwrap_or("");
    if uid.is_empty() || uid == "0" || uid == "root" {
        return Err("refusing to run a job container as root".into());
    }
    let cpus = a.cpus.unwrap_or(cfg.default_cpus);
    let mem = a.memory_mb.unwrap_or(cfg.default_memory_mb);
    let image = a.image.clone().unwrap_or_else(|| cfg.default_image.clone());
    let mut v: Vec<String> = vec![
        "run".into(),
        "--detach".into(),
        "--name".into(),
        container_name(&cfg.node, a.job, a.attempt),
        "--label".into(),
        format!("{LABEL_NODE}={}", cfg.node),
        "--label".into(),
        format!("{LABEL_JOB}={}", a.job),
        "--label".into(),
        format!("{LABEL_ATTEMPT}={}", a.attempt),
        "--cpus".into(),
        format!("{cpus}"),
        "--memory".into(),
        format!("{mem}m"),
        "--memory-swap".into(),
        format!("{mem}m"),
        "--pids-limit".into(),
        cfg.pids_limit.to_string(),
        "--read-only".into(),
        "--tmpfs".into(),
        format!("/tmp:rw,nosuid,nodev,noexec,size={}m", cfg.tmpfs_mb),
        "--security-opt".into(),
        "no-new-privileges".into(),
        "--cap-drop".into(),
        "ALL".into(),
        "--user".into(),
        cfg.user.clone(),
        "--network".into(),
        cfg.network.clone(),
        "--stop-signal".into(),
        "SIGTERM".into(),
    ];
    let mut env = a.env.clone();
    if cfg.real_gpus && !a.gpus.is_empty() {
        let ids: Vec<String> = a.gpus.iter().map(|g| g.to_string()).collect();
        v.push("--gpus".into());
        // the quotes are part of Docker's syntax for a list of devices
        v.push(format!("\"device={}\"", ids.join(",")));
        // Inside the container the passed-through devices are numbered from 0.
        env.insert(
            "CUDA_VISIBLE_DEVICES".into(),
            (0..ids.len()).map(|i| i.to_string()).collect::<Vec<_>>().join(","),
        );
        env.insert("NEBULA_HOST_GPUS".into(), ids.join(","));
    }
    for (k, val) in &env {
        v.push("--env".into());
        v.push(format!("{k}={val}"));
    }
    v.push("--".into());
    v.push(image);
    v.extend(a.command.iter().cloned());
    Ok(v)
}

fn docker(cfg: &DockerConfig, args: &[&str]) -> Result<String, String> {
    let out = Command::new(&cfg.docker)
        .args(args)
        .stdin(Stdio::null())
        .output()
        .map_err(|e| format!("{}: {e}", cfg.docker))?;
    if !out.status.success() {
        return Err(format!(
            "docker {}: {}",
            args.first().unwrap_or(&""),
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

/// Start the container for `a`. A leftover container with the same name (from
/// a crashed agent) is removed first.
pub fn start(cfg: &DockerConfig, a: &Assignment) -> Result<String, String> {
    let args = run_args(cfg, a)?;
    let name = container_name(&cfg.node, a.job, a.attempt);
    let _ = docker(cfg, &["rm", "--force", &name]);
    let refs: Vec<&str> = args.iter().map(String::as_str).collect();
    docker(cfg, &refs)?;
    Ok(name)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Listed {
    pub name: String,
    pub job: Option<u64>,
    pub attempt: Option<u32>,
    /// `created`, `running`, `exited`, ...
    pub state: String,
}

impl Listed {
    pub fn is_live(&self) -> bool {
        matches!(self.state.as_str(), "created" | "running" | "restarting" | "paused")
    }
}

/// All containers (running or not) labelled with this node.
pub fn list(cfg: &DockerConfig) -> Result<Vec<Listed>, String> {
    let filter = format!("label={LABEL_NODE}={}", cfg.node);
    let fmt = format!("{{{{.Names}}}}\t{{{{.State}}}}\t{{{{.Label \"{LABEL_JOB}\"}}}}\t{{{{.Label \"{LABEL_ATTEMPT}\"}}}}");
    let out = docker(cfg, &["ps", "--all", "--no-trunc", "--filter", &filter, "--format", &fmt])?;
    Ok(out
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| {
            let f: Vec<&str> = l.split('\t').collect();
            Listed {
                name: f[0].to_string(),
                state: f.get(1).unwrap_or(&"").to_string(),
                job: f.get(2).and_then(|x| x.parse().ok()),
                attempt: f.get(3).and_then(|x| x.parse().ok()),
            }
        })
        .collect())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExitInfo {
    pub code: i32,
    pub oom_killed: bool,
}

pub fn exit_info(cfg: &DockerConfig, name: &str) -> Result<ExitInfo, String> {
    let out = docker(cfg, &["inspect", "--format", "{{.State.ExitCode}} {{.State.OOMKilled}}", name])?;
    let mut f = out.split_whitespace();
    let code = f.next().and_then(|x| x.parse().ok()).ok_or("bad inspect output")?;
    let oom_killed = f.next() == Some("true");
    Ok(ExitInfo { code, oom_killed })
}

/// Send a signal to the container's main process (`TERM` or `KILL`).
pub fn signal(cfg: &DockerConfig, name: &str, sig: &str) {
    if let Err(e) = docker(cfg, &["kill", "--signal", sig, name]) {
        tracing::debug!(container = name, "kill -{sig}: {e}");
    }
}

/// Append the container's stdout and stderr to `path`.
pub fn save_logs(cfg: &DockerConfig, name: &str, path: &Path) {
    let Ok(f) = std::fs::OpenOptions::new().create(true).append(true).open(path) else {
        return;
    };
    let Ok(f2) = f.try_clone() else { return };
    let _ = Command::new(&cfg.docker)
        .args(["logs", name])
        .stdin(Stdio::null())
        .stdout(f)
        .stderr(f2)
        .status();
}

pub fn remove(cfg: &DockerConfig, name: &str) {
    if let Err(e) = docker(cfg, &["rm", "--force", "--volumes", name]) {
        tracing::warn!(container = name, "rm: {e}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn asg(gpus: Vec<u32>) -> Assignment {
        let mut env = BTreeMap::new();
        env.insert(
            "CUDA_VISIBLE_DEVICES".to_string(),
            gpus.iter().map(|g| g.to_string()).collect::<Vec<_>>().join(","),
        );
        Assignment {
            job: 7,
            attempt: 2,
            gpus,
            command: vec!["sh".into(), "-c".into(), "echo hi; rm -rf /".into()],
            env,
            image: None,
            cpus: Some(0.5),
            memory_mb: Some(64),
        }
    }

    fn has(v: &[String], a: &str, b: &str) -> bool {
        v.windows(2).any(|w| w[0] == a && w[1] == b)
    }

    #[test]
    fn hardening_and_limits_are_always_set() {
        let cfg = DockerConfig::new("node/1");
        let v = run_args(&cfg, &asg(vec![2, 3])).unwrap();
        assert!(has(&v, "--name", "nebula-node_1-j7-a2"));
        assert!(has(&v, "--cpus", "0.5"));
        assert!(has(&v, "--memory", "64m"));
        assert!(has(&v, "--memory-swap", "64m"));
        assert!(has(&v, "--security-opt", "no-new-privileges"));
        assert!(has(&v, "--cap-drop", "ALL"));
        assert!(has(&v, "--user", "65534:65534"));
        assert!(has(&v, "--network", "none"));
        assert!(has(&v, "--label", "nebula.node=node/1"));
        assert!(has(&v, "--label", "nebula.job=7"));
        assert!(has(&v, "--label", "nebula.attempt=2"));
        assert!(v.contains(&"--read-only".to_string()));
        // simulated GPUs: no --gpus, host indices passed through unchanged
        assert!(!v.contains(&"--gpus".to_string()));
        assert!(has(&v, "--env", "CUDA_VISIBLE_DEVICES=2,3"));
        // the command follows the image, after `--`, unmodified
        let i = v.iter().position(|x| x == "--").unwrap();
        assert_eq!(v[i + 1], "alpine:3.20");
        assert_eq!(&v[i + 2..], &["sh", "-c", "echo hi; rm -rf /"]);
    }

    #[test]
    fn real_gpus_are_passed_through_and_renumbered() {
        let mut cfg = DockerConfig::new("n1");
        cfg.real_gpus = true;
        let v = run_args(&cfg, &asg(vec![2, 3])).unwrap();
        assert!(has(&v, "--gpus", "\"device=2,3\""));
        assert!(has(&v, "--env", "CUDA_VISIBLE_DEVICES=0,1"));
        assert!(has(&v, "--env", "NEBULA_HOST_GPUS=2,3"));
    }

    #[test]
    fn defaults_apply_and_root_is_refused() {
        let mut cfg = DockerConfig::new("n1");
        let mut a = asg(vec![0]);
        a.cpus = None;
        a.memory_mb = None;
        a.image = Some("busybox:1.36".into());
        let v = run_args(&cfg, &a).unwrap();
        assert!(has(&v, "--cpus", "1"));
        assert!(has(&v, "--memory", "512m"));
        assert!(v.contains(&"busybox:1.36".to_string()));
        cfg.user = "0:0".into();
        assert!(run_args(&cfg, &a).is_err());
        cfg.user = "root".into();
        assert!(run_args(&cfg, &a).is_err());
    }
}
