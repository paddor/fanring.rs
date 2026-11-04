use std::path::Path;

use core_affinity::CoreId;

use super::affinity::sibling_major;
use super::results::result_path;

fn six_smt_cores() -> Vec<Vec<CoreId>> {
    (0..6)
        .map(|id| vec![CoreId { id }, CoreId { id: id + 6 }])
        .collect()
}

#[test]
fn sibling_major_uses_each_physical_core_before_smt_siblings() {
    let ids = sibling_major(&six_smt_cores())
        .into_iter()
        .map(|core| core.id)
        .collect::<Vec<_>>();

    assert_eq!(ids, (0..12).collect::<Vec<_>>());
}

#[test]
fn result_path_uses_topology_file_in_implementation_directory() {
    assert_eq!(
        result_path(
            Path::new("/cache/fanring"),
            "crossfire-mpmc",
            "throughput-mpmc.jsonl"
        ),
        Path::new("/cache/fanring/crossfire/throughput-mpmc.jsonl")
    );
}
