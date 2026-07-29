import argparse
import importlib.util
import pathlib
import unittest


SCRIPT = pathlib.Path(__file__).with_name("tune_sqlite.py")
SPEC = importlib.util.spec_from_file_location("tune_sqlite", SCRIPT)
tune_sqlite = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(tune_sqlite)


def serving_run(point_p95=2.0, page_p95=3.0, failed=0, rss=100):
    return {
        "latency": {
            "point": {"observations": 100, "p95_ms": point_p95},
            "page": {"observations": 100, "p95_ms": page_p95},
        },
        "throughput": {"requests_failed": failed},
        "resources": {"rss_bytes": rss},
        "replay": {
            "streams_requested": 8,
            "streams_ready": 8,
            "streams_completed": 8,
            "records": 8192,
            "expected_records": 8192,
            "cursor_errors": 0,
            "connection_errors": 0,
        },
    }


class TuneSqliteTest(unittest.TestCase):
    def test_candidate_parser_accepts_only_fixed_values(self):
        self.assertEqual((8, 16), tune_sqlite.parse_candidates("8,16,8", "fetch_ahead"))
        with self.assertRaises(argparse.ArgumentTypeError):
            tune_sqlite.parse_candidates("8,15", "fetch_ahead")
        with self.assertRaises(argparse.ArgumentTypeError):
            tune_sqlite.parse_candidates("", "read_pool")

    def test_plan_separates_ingestion_and_serving_cross_products(self):
        plan = tune_sqlite.sweep_plan((8, 16), (1000, 4000), (4, 8), (256, 512))
        self.assertEqual(4, len(plan["ingestion"]))
        self.assertEqual(4, len(plan["serving"]))
        self.assertNotIn("read_pool", plan["ingestion"][0])
        self.assertNotIn("fetch_ahead", plan["serving"][0])

    def test_serving_selection_uses_capacity_then_latency_rss_and_smaller_tuple(self):
        candidates = [
            {
                "read_pool": 8,
                "replay_page": 512,
                "workloads": [
                    {"multiplier": 1, "repetitions": 1, "runs": [serving_run()]},
                    {"multiplier": 2, "repetitions": 1, "runs": [serving_run(4, 5, rss=200)]},
                ],
            },
            {
                "read_pool": 4,
                "replay_page": 256,
                "workloads": [
                    {"multiplier": 2, "repetitions": 1, "runs": [serving_run(3, 6, rss=300)]}
                ],
            },
        ]
        selected = tune_sqlite.select_serving(candidates)
        self.assertEqual("selected", selected["status"])
        self.assertEqual(4, selected["read_pool"])
        self.assertEqual(256, selected["replay_page"])

        candidates[1]["workloads"][0]["runs"][0]["latency"]["point"]["p95_ms"] = 25
        selected = tune_sqlite.select_serving(candidates)
        self.assertEqual(8, selected["read_pool"])

    def test_no_passing_candidate_keeps_initial_tuple(self):
        candidate = {
            "read_pool": 4,
            "replay_page": 256,
            "workloads": [
                {"multiplier": 1, "repetitions": 1, "runs": [serving_run(failed=1)]}
            ],
        }
        self.assertEqual(
            {"status": "no_passing_candidate", **tune_sqlite.INITIAL},
            tune_sqlite.select_serving([candidate]),
        )

    def test_incomplete_or_noncontiguous_replay_cannot_pass(self):
        run = serving_run()
        run["replay"]["streams_completed"] = 7
        self.assertFalse(tune_sqlite._passing_serving_run(run))
        run = serving_run()
        run["replay"]["cursor_errors"] = 1
        self.assertFalse(tune_sqlite._passing_serving_run(run))
        run = serving_run()
        run["replay"]["streams_ready"] = 7
        self.assertFalse(tune_sqlite._passing_serving_run(run))

    def test_replay_bounds_keep_epoch_and_start_before_seeded_history(self):
        after, current = tune_sqlite._replay_bounds(
            {"current": "00112233445566778899aabbccddeeff:1024"}
        )
        self.assertEqual("00112233445566778899aabbccddeeff:0", after)
        self.assertEqual("00112233445566778899aabbccddeeff:1024", current)

    def test_replay_bounds_reject_empty_history(self):
        with self.assertRaises(RuntimeError):
            tune_sqlite._replay_bounds(
                {"current": "00112233445566778899aabbccddeeff:0"}
            )

    def test_each_fixed_candidate_is_accepted_by_the_parser(self):
        for name, candidates in tune_sqlite.FIXED.items():
            raw = ",".join(str(candidate) for candidate in candidates)
            self.assertEqual(candidates, tune_sqlite.parse_candidates(raw, name))


if __name__ == "__main__":
    unittest.main()
