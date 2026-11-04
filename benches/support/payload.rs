//! Message payloads every throughput benchmark sends.

use super::options::Filter;

const LABELS: [&str; 3] = ["u64", "bytes64", "bytes256"];

#[derive(Debug, Clone, Copy)]
pub(crate) struct Payload<T> {
    pub(crate) label: &'static str,
    pub(crate) value: T,
}

/// A benchmark body run once per selected payload type.
pub(crate) trait PayloadRun {
    fn run<T: Copy + Default + Send + Sync + 'static>(&mut self, payload: Payload<T>);
}

/// Runs `body` for each payload that `filter` selects, smallest first.
pub(crate) fn for_each_payload(filter: &Filter, body: &mut impl PayloadRun) {
    if filter.matches(LABELS[0]) {
        body.run(Payload {
            label: LABELS[0],
            value: 0u64,
        });
    }
    if filter.matches(LABELS[1]) {
        body.run(Payload {
            label: LABELS[1],
            value: [0u64; 8],
        });
    }
    if filter.matches(LABELS[2]) {
        body.run(Payload {
            label: LABELS[2],
            value: [0u64; 32],
        });
    }
}

pub(crate) fn selected_payload_count(filter: &Filter) -> usize {
    LABELS
        .into_iter()
        .filter(|label| filter.matches(label))
        .count()
}
