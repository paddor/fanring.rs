//! Tags shared by every result row of one benchmark run.

use std::time::{SystemTime, UNIX_EPOCH};

use super::affinity::Affinity;

#[derive(Debug)]
pub(crate) struct RunContext {
    pub(crate) run_id: String,
    pub(crate) cpu: String,
    pub(crate) affinity: Affinity,
}

impl RunContext {
    /// Tags this run with the current time, the CPU, and the pinning policy.
    pub(crate) fn from_env() -> Self {
        Self {
            run_id: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock before Unix epoch")
                .as_nanos()
                .to_string(),
            cpu: cpu_name(),
            affinity: Affinity::from_env(),
        }
    }
}

fn cpu_name() -> String {
    let Ok(cpuinfo) = std::fs::read_to_string("/proc/cpuinfo") else {
        return "unknown CPU".to_string();
    };
    cpuinfo
        .lines()
        .find_map(|line| line.strip_prefix("model name\t: "))
        .unwrap_or("unknown CPU")
        .to_string()
}
