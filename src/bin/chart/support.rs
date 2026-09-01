use std::collections::{BTreeMap, BTreeSet};

use super::Row;

pub(super) fn run_is_complete(rows: &[&Row]) -> bool {
    if rows.is_empty() {
        return false;
    }
    let expected_rows = rows[0].expected_rows;
    if expected_rows != 0
        && (rows.len() != expected_rows
            || rows.iter().any(|row| row.expected_rows != expected_rows))
    {
        return false;
    }
    let mut groups =
        BTreeMap::<(&str, &str, usize, Option<usize>), (usize, BTreeSet<usize>)>::new();
    for row in rows {
        let (_, samples) = groups
            .entry((
                row.implementation.as_str(),
                row.payload.as_str(),
                row.producers,
                row.consumers,
            ))
            .or_insert_with(|| (row.samples, BTreeSet::new()));
        samples.insert(row.sample);
    }
    groups.values().all(|(expected, samples)| {
        *expected != 0 && samples.len() == *expected && samples.iter().copied().eq(0..*expected)
    })
}

pub(super) fn arg_value(name: &str) -> Option<String> {
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == name {
            return args.next();
        }
    }
    None
}

pub(super) fn has_arg(name: &str) -> bool {
    std::env::args().skip(1).any(|arg| arg == name)
}

#[cfg(test)]
mod tests {
    use super::super::Row;
    use super::super::render::{measurement, present_series};
    use super::run_is_complete;

    #[test]
    fn measurement_uses_median_and_relative_mad() {
        let measurement = measurement(vec![10.0, 12.0, 14.0]);
        assert_eq!(measurement.median, 12.0);
        assert!((measurement.relative_mad - 16.666_666).abs() < 0.000_001);
    }

    #[test]
    fn complete_run_requires_every_sample() {
        let first = row("fanring", 0, 2);
        let second = row("fanring", 1, 2);
        assert!(run_is_complete(&[&first, &second]));
        assert!(!run_is_complete(&[&first]));
    }

    #[test]
    fn unknown_implementations_are_rendered() {
        let known = row("fanring", 0, 1);
        let unknown = row("new-channel", 0, 1);
        assert_eq!(
            present_series(&[&known, &unknown]),
            vec![("fanring", "fanring"), ("new-channel", "new-channel")]
        );
    }

    #[test]
    fn legacy_total_capacity_is_accepted() {
        let row: Row = serde_json::from_str(
            r#"{
                "run_id":"run",
                "implementation":"fanring",
                "payload":"u64",
                "payload_bytes":8,
                "producers":1,
                "total_capacity":8192,
                "items_per_sec":1.0
            }"#,
        )
        .unwrap();

        assert_eq!(row.nominal_capacity, 8192);
        assert_eq!(row.capacity_model, None);
    }

    fn row(implementation: &str, sample: usize, samples: usize) -> Row {
        Row {
            run_id: "run".to_string(),
            cpu: "cpu".to_string(),
            mode: "try".to_string(),
            implementation: implementation.to_string(),
            payload: "u64".to_string(),
            payload_bytes: 8,
            producers: 1,
            consumers: None,
            nominal_capacity: 1,
            capacity_model: Some("per-ring-hwm".to_string()),
            items_per_sec: 1.0,
            sample,
            samples,
            expected_rows: samples,
        }
    }
}
