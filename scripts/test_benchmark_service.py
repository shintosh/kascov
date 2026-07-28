import importlib.util
import pathlib
import unittest


SCRIPT = pathlib.Path(__file__).with_name("benchmark_service.py")
SPEC = importlib.util.spec_from_file_location("benchmark_service", SCRIPT)
benchmark_service = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(benchmark_service)


class BenchmarkServiceTest(unittest.TestCase):
    def test_percentiles_require_one_hundred_observations(self):
        summary = benchmark_service.summarize_latency([1.0] * 99)
        self.assertEqual(99, summary["observations"])
        self.assertIsNone(summary["p50_ms"])
        self.assertIsNone(summary["p95_ms"])
        self.assertIsNone(summary["p99_ms"])

        summary = benchmark_service.summarize_latency([float(i) for i in range(100)])
        self.assertIsNotNone(summary["p50_ms"])
        self.assertIsNotNone(summary["p95_ms"])
        self.assertIsNotNone(summary["p99_ms"])

    def test_report_requires_measurement_and_hardware_groups(self):
        report = benchmark_service.build_report(
            network="testnet-10",
            duration_seconds=1.0,
            point_samples=[1.0] * 100,
            page_samples=[2.0] * 100,
            stream_events=3,
            requests_ok=200,
            requests_failed=0,
            rss_bytes=1024,
            database_bytes=2048,
            wal_bytes=512,
        )
        self.assertEqual("live_node", report["sample_source"])
        for field in ("hardware", "latency", "throughput", "resources"):
            self.assertIn(field, report)


if __name__ == "__main__":
    unittest.main()
