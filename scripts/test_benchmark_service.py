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
            sample_source="deterministic_fixture",
            source_identity="fixture:testnet-10:sha256:test",
            node_identity=None,
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
            replay={
                "streams_requested": 2,
                "streams_completed": 2,
                "records": 6,
                "expected_records": 6,
                "cursor_errors": 0,
                "connection_errors": 0,
                "after": "00112233445566778899aabbccddeeff:0",
                "current": "00112233445566778899aabbccddeeff:3",
            },
        )
        self.assertEqual("deterministic_fixture", report["sample_source"])
        self.assertEqual("fixture:testnet-10:sha256:test", report["source_identity"])
        self.assertIsNone(report["node_identity"])
        self.assertEqual(2, report["replay"]["streams_completed"])
        for field in ("hardware", "latency", "throughput", "resources"):
            self.assertIn(field, report)

    def test_live_samples_require_node_identity(self):
        with self.assertRaises(ValueError):
            benchmark_service.build_report(
                sample_source="live_node",
                source_identity="service:http://127.0.0.1:8080",
                node_identity=None,
                network="testnet-10",
                duration_seconds=1.0,
                point_samples=[],
                page_samples=[],
                stream_events=0,
                requests_ok=0,
                requests_failed=0,
                rss_bytes=0,
                database_bytes=0,
                wal_bytes=0,
                replay=None,
            )


if __name__ == "__main__":
    unittest.main()
