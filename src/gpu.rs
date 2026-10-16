//! GPU discovery for the node agent.
//!
//! When `nvidia-smi` is available the agent reports the real devices (name,
//! total memory, UUID). Otherwise it reports `--gpus` simulated devices.
//! NVLink topology is not detected: islands come from `--islands`.

use std::process::Command;

/// One device as reported by
/// `nvidia-smi --query-gpu=index,name,memory.total,uuid --format=csv,noheader,nounits`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DetectedGpu {
    pub index: u32,
    pub name: String,
    pub memory_mb: Option<u64>,
    pub uuid: String,
}

pub const QUERY_ARGS: [&str; 2] = ["--query-gpu=index,name,memory.total,uuid", "--format=csv,noheader,nounits"];

/// Parse the CSV output of [`QUERY_ARGS`]. Blank lines are skipped. A line
/// with the wrong number of fields or a non-numeric index is an error, so a
/// changed output format is noticed instead of silently reporting no GPUs.
pub fn parse_query(out: &str) -> Result<Vec<DetectedGpu>, String> {
    let mut v = vec![];
    for (n, line) in out.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let f: Vec<&str> = line.split(',').map(str::trim).collect();
        if f.len() != 4 {
            return Err(format!("line {}: expected 4 fields, got {}: {line:?}", n + 1, f.len()));
        }
        let index = f[0]
            .parse::<u32>()
            .map_err(|e| format!("line {}: bad index {:?}: {e}", n + 1, f[0]))?;
        if f[1].is_empty() || f[3].is_empty() {
            return Err(format!("line {}: empty name or uuid", n + 1));
        }
        // "[N/A]" and similar are reported when a field is unsupported
        let memory_mb = f[2].parse::<u64>().ok();
        v.push(DetectedGpu {
            index,
            name: f[1].to_string(),
            memory_mb,
            uuid: f[3].to_string(),
        });
    }
    let mut seen = std::collections::BTreeSet::new();
    for g in &v {
        if !seen.insert(g.index) {
            return Err(format!("duplicate GPU index {}", g.index));
        }
    }
    Ok(v)
}

/// Where the agent's GPUs come from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Source {
    /// real devices from nvidia-smi
    Nvidia(Vec<DetectedGpu>),
    /// `--gpus N` simulated devices
    Simulated,
}

/// Run `nvidia_smi` and parse its output. `Ok(None)` means the binary is not
/// installed (the normal case on a machine without NVIDIA drivers).
pub fn query(nvidia_smi: &str) -> Result<Option<Vec<DetectedGpu>>, String> {
    let out = match Command::new(nvidia_smi).args(QUERY_ARGS).output() {
        Ok(o) => o,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(format!("{nvidia_smi}: {e}")),
    };
    if !out.status.success() {
        return Err(format!(
            "{nvidia_smi} exited with {}: {}",
            out.status,
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    parse_query(&String::from_utf8_lossy(&out.stdout)).map(Some)
}

/// `mode` is `auto`, `nvidia` or `sim`. `auto` uses nvidia-smi when it is
/// installed and reports at least one GPU; `nvidia` fails if it cannot.
pub fn detect(mode: &str, nvidia_smi: &str) -> Result<Source, String> {
    match mode {
        "sim" => Ok(Source::Simulated),
        "auto" => match query(nvidia_smi) {
            Ok(Some(g)) if !g.is_empty() => Ok(Source::Nvidia(g)),
            Ok(_) => Ok(Source::Simulated),
            Err(e) => {
                tracing::warn!("nvidia-smi failed ({e}); using simulated GPUs");
                Ok(Source::Simulated)
            }
        },
        "nvidia" => match query(nvidia_smi)? {
            Some(g) if !g.is_empty() => Ok(Source::Nvidia(g)),
            _ => Err(format!("--gpu-source nvidia: {nvidia_smi} not found or reported no GPUs")),
        },
        m => Err(format!("unknown --gpu-source {m}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Written in the CSV format nvidia-smi prints for QUERY_ARGS (one line per
    // device). These fixtures were NOT captured on real hardware: this machine
    // has no GPU. docs/OWNER_ACTIONS.md asks the owner to capture the real
    // output on the RTX 4060 and add it here.
    const ONE_4060: &str = "0, NVIDIA GeForce RTX 4060, 8188, GPU-3f5c2a1e-8d1b-4c3e-9a77-0b6f4e2d9c11\n";
    const EIGHT_H100: &str = "\
0, NVIDIA H100 80GB HBM3, 81559, GPU-00000000-0000-0000-0000-000000000000
1, NVIDIA H100 80GB HBM3, 81559, GPU-00000000-0000-0000-0000-000000000001
2, NVIDIA H100 80GB HBM3, 81559, GPU-00000000-0000-0000-0000-000000000002
3, NVIDIA H100 80GB HBM3, 81559, GPU-00000000-0000-0000-0000-000000000003
4, NVIDIA H100 80GB HBM3, 81559, GPU-00000000-0000-0000-0000-000000000004
5, NVIDIA H100 80GB HBM3, 81559, GPU-00000000-0000-0000-0000-000000000005
6, NVIDIA H100 80GB HBM3, 81559, GPU-00000000-0000-0000-0000-000000000006
7, NVIDIA H100 80GB HBM3, 81559, GPU-00000000-0000-0000-0000-000000000007

";

    #[test]
    fn parses_single_consumer_gpu() {
        let g = parse_query(ONE_4060).unwrap();
        assert_eq!(
            g,
            vec![DetectedGpu {
                index: 0,
                name: "NVIDIA GeForce RTX 4060".into(),
                memory_mb: Some(8188),
                uuid: "GPU-3f5c2a1e-8d1b-4c3e-9a77-0b6f4e2d9c11".into(),
            }]
        );
    }

    #[test]
    fn parses_eight_gpus_and_trailing_blank_lines() {
        let g = parse_query(EIGHT_H100).unwrap();
        assert_eq!(g.len(), 8);
        assert_eq!(g[7].index, 7);
        assert_eq!(g[7].memory_mb, Some(81559));
        assert!(g.iter().all(|x| x.name == "NVIDIA H100 80GB HBM3"));
    }

    #[test]
    fn unsupported_memory_field_is_none() {
        let g = parse_query("0, Some GPU, [N/A], GPU-abc\n").unwrap();
        assert_eq!(g[0].memory_mb, None);
    }

    #[test]
    fn rejects_malformed_output() {
        assert!(parse_query("0, NVIDIA GeForce RTX 4060, 8188\n").is_err());
        assert!(parse_query("x, NVIDIA GeForce RTX 4060, 8188, GPU-1\n").is_err());
        assert!(parse_query("0, , 8188, GPU-1\n").is_err());
        assert!(parse_query("0, A, 1, GPU-1\n0, A, 1, GPU-2\n").is_err());
        // an error message (e.g. driver mismatch) is not a GPU list
        assert!(parse_query("Failed to initialize NVML: Driver/library version mismatch\n").is_err());
        assert_eq!(parse_query("").unwrap(), vec![]);
    }

    #[test]
    fn missing_binary_means_simulated() {
        assert_eq!(query("/nonexistent/nvidia-smi").unwrap(), None);
        assert_eq!(detect("auto", "/nonexistent/nvidia-smi").unwrap(), Source::Simulated);
        assert!(detect("nvidia", "/nonexistent/nvidia-smi").is_err());
        assert_eq!(detect("sim", "/nonexistent/nvidia-smi").unwrap(), Source::Simulated);
    }
}
