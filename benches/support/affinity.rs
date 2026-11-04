//! CPU pinning for benchmark threads, physical cores first.

use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub(crate) struct Affinity {
    cores: Arc<[core_affinity::CoreId]>,
    description: Arc<str>,
}

impl Affinity {
    pub(crate) fn from_env() -> Self {
        match std::env::var("FANRING_BENCH_AFFINITY").as_deref() {
            Ok("off") => Self {
                cores: Arc::from([]),
                description: Arc::from("off"),
            },
            Ok("auto") | Err(_) => {
                let cores = core_affinity::get_core_ids().unwrap_or_else(|| {
                    panic!(
                        "CPU affinity is unavailable; set FANRING_BENCH_AFFINITY=off to continue"
                    )
                });
                assert!(!cores.is_empty(), "CPU affinity returned no available CPUs");
                let (cores, topology) = ordered_cores(cores);
                let ids = cores
                    .iter()
                    .map(|core| core.id.to_string())
                    .collect::<Vec<_>>()
                    .join(",");
                Self {
                    cores: cores.into(),
                    description: format!("{topology}:{ids}").into(),
                }
            }
            Ok(value) => {
                panic!("invalid FANRING_BENCH_AFFINITY={value:?}; expected auto or off")
            }
        }
    }

    pub(crate) fn pin(&self, index: usize) {
        if self.cores.is_empty() {
            return;
        }
        let core = self.cores[index % self.cores.len()];
        assert!(
            core_affinity::set_for_current(core),
            "failed to pin benchmark thread to CPU {}",
            core.id
        );
    }

    pub(crate) fn description(&self) -> &str {
        &self.description
    }
}

fn ordered_cores(
    mut cores: Vec<core_affinity::CoreId>,
) -> (Vec<core_affinity::CoreId>, &'static str) {
    cores.sort_unstable_by_key(|core| core.id);
    let mut groups = BTreeMap::<(u32, u32), Vec<core_affinity::CoreId>>::new();
    for core in &cores {
        let Some(package) = topology_id(core.id, "physical_package_id") else {
            return (cores, "logical-order");
        };
        let Some(core_id) = topology_id(core.id, "core_id") else {
            return (cores, "logical-order");
        };
        groups.entry((package, core_id)).or_default().push(*core);
    }
    let groups = groups.into_values().collect::<Vec<_>>();
    (sibling_major(&groups), "physical-first")
}

pub(super) fn sibling_major(groups: &[Vec<core_affinity::CoreId>]) -> Vec<core_affinity::CoreId> {
    let sibling_count = groups.iter().map(Vec::len).max().unwrap_or(0);
    (0..sibling_count)
        .flat_map(|sibling| {
            groups
                .iter()
                .filter_map(move |group| group.get(sibling).copied())
        })
        .collect()
}

fn topology_id(cpu: usize, name: &str) -> Option<u32> {
    std::fs::read_to_string(format!("/sys/devices/system/cpu/cpu{cpu}/topology/{name}"))
        .ok()?
        .trim()
        .parse()
        .ok()
}
