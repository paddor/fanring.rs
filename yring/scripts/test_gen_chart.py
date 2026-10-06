"""Checks that SPSC charts use complete measured runs."""

import json
from pathlib import Path
import tempfile
import unittest

import gen_chart


def measured_run(run_id="1"):
    return [
        {
            "run_id": run_id,
            "source_revision": "measured-source",
            "channel": channel,
            "payload": payload,
            "capacity": 1024,
            "batch_size": 64 if index in (1, 3) else 1,
            "sample": sample,
            "samples": 2,
            "expected_rows": 48,
            "duration_secs": 2,
            "warmup_secs": 0.25,
            "producer_cpu": 0,
            "consumer_cpu": 1,
            "payload_handling": "read_each_value",
            "time_check_interval": 1024,
            "throughput_items_per_sec": (sample + 1) * 1_000_000,
        }
        for payload in ("u64", "[u8; 32]", "[u8; 64]", "[u8; 128]")
        for index, channel in enumerate(gen_chart.CHANNEL_ORDER)
        for sample in range(2)
    ]


class ChartInputTests(unittest.TestCase):
    def load(self, rows):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "results.jsonl"
            path.write_text("".join(json.dumps(row) + "\n" for row in rows))
            return gen_chart.load_results(path)

    def test_uses_measured_medians(self):
        results, row = self.load(measured_run())
        self.assertEqual(row["run_id"], "1")
        self.assertEqual(len(results), 4)
        self.assertTrue(all(value == 1.5 for group in results.values()
                            for value in group.values()))

    def test_incomplete_new_run_does_not_replace_complete_run(self):
        _, row = self.load(measured_run() + measured_run("2")[:-1])
        self.assertEqual(row["run_id"], "1")

    def test_newest_complete_run_replaces_old_run(self):
        _, row = self.load(measured_run("20") + measured_run("3"))
        self.assertEqual(row["run_id"], "20")

    def test_rejects_duplicate_samples_and_mixed_configuration(self):
        for field, value in (("sample", 1), ("consumer_cpu", 2),
                             ("source_revision", "another-source"),
                             ("payload_handling", "count_only"),
                             ("time_check_interval", 1),
                             ("batch_size", 64)):
            with self.subTest(field=field):
                rows = measured_run()
                rows[0][field] = value
                with self.assertRaises(ValueError):
                    self.load(rows)

    def test_rejects_invalid_throughput_and_shared_cpu(self):
        for value in (float("nan"), float("inf"), 0, -1):
            rows = measured_run()
            rows[0]["throughput_items_per_sec"] = value
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.load(rows)
        rows = measured_run()
        for row in rows:
            row["consumer_cpu"] = row["producer_cpu"]
        with self.assertRaises(ValueError):
            self.load(rows)

    def test_svg_escapes_metadata(self):
        results, _ = self.load(measured_run())
        svg = gen_chart.generate_chart(results, "A&B", "<host>")
        self.assertIn("A&amp;B", svg)
        self.assertIn("&lt;host&gt;", svg)


if __name__ == "__main__":
    unittest.main()
