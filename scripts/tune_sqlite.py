#!/usr/bin/env python3
"""Run the fixed Stage 2 SQLite ingestion and serving candidate sweep."""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import itertools
import json
import os
import pathlib
import platform
import shutil
import subprocess
import tempfile
import time
import urllib.request


ROOT = pathlib.Path(__file__).resolve().parents[1]
FIXED = {
    "fetch_ahead": (8, 16, 32, 64),
    "wal_autocheckpoint": (1_000, 4_000, 16_000),
    "read_pool": (4, 8, 16),
    "replay_page": (256, 512, 1_024),
}
INITIAL = {
    "fetch_ahead": 16,
    "wal_autocheckpoint": 1_000,
    "read_pool": 8,
    "replay_page": 512,
}


def parse_candidates(raw: str, name: str) -> tuple[int, ...]:
    try:
        values = tuple(dict.fromkeys(int(value.strip()) for value in raw.split(",")))
    except ValueError as error:
        raise argparse.ArgumentTypeError(f"{name} must contain integers") from error
    if not values or any(value not in FIXED[name] for value in values):
        raise argparse.ArgumentTypeError(
            f"{name} must use only {','.join(map(str, FIXED[name]))}"
        )
    return values


def sweep_plan(fetch_ahead, wal_autocheckpoint, read_pool, replay_page):
    return {
        "ingestion": [
            {"fetch_ahead": fetch, "wal_autocheckpoint": wal}
            for fetch, wal in itertools.product(fetch_ahead, wal_autocheckpoint)
        ],
        "serving": [
            {"read_pool": readers, "replay_page": replay}
            for readers, replay in itertools.product(read_pool, replay_page)
        ],
    }


def _passing_serving_run(run: dict) -> bool:
    point = run["latency"]["point"]
    page = run["latency"]["page"]
    replay = run["replay"]
    return (
        run["throughput"]["requests_failed"] == 0
        and replay["streams_requested"] == replay["streams_ready"]
        and replay["streams_requested"] == replay["streams_completed"]
        and replay["records"] == replay["expected_records"]
        and replay["cursor_errors"] == 0
        and replay["connection_errors"] == 0
        and point["observations"] >= 100
        and page["observations"] >= 100
        and point["p95_ms"] is not None
        and page["p95_ms"] is not None
        and point["p95_ms"] < 20
        and page["p95_ms"] < 50
    )


def select_serving(candidates: list[dict]) -> dict:
    ranked = []
    for candidate in candidates:
        passing = [
            workload
            for workload in candidate["workloads"]
            if len(workload["runs"]) == workload["repetitions"]
            and all(_passing_serving_run(run) for run in workload["runs"])
        ]
        if not passing:
            continue
        best = max(passing, key=lambda workload: workload["multiplier"])
        point_p95 = sum(run["latency"]["point"]["p95_ms"] for run in best["runs"]) / len(
            best["runs"]
        )
        rss = max(run["resources"]["rss_bytes"] for run in best["runs"])
        ranked.append(
            (
                -best["multiplier"],
                point_p95,
                rss,
                candidate["read_pool"],
                candidate["replay_page"],
                candidate,
            )
        )
    if not ranked:
        return {"status": "no_passing_candidate", **INITIAL}
    selected = min(ranked)[-1]
    return {
        "status": "selected",
        "read_pool": selected["read_pool"],
        "replay_page": selected["replay_page"],
    }


def _hardware() -> dict:
    return {
        "os": platform.platform(),
        "architecture": platform.machine(),
        "logical_cpus": os.cpu_count() or 1,
    }


def _wait_ready(base: str, process: subprocess.Popen, timeout: float = 10) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"Kascov exited before health was ready: {process.returncode}")
        try:
            with urllib.request.urlopen(f"{base}/healthz", timeout=0.25) as response:
                if response.status in (200, 503):
                    return
        except OSError:
            time.sleep(0.025)
    raise RuntimeError("Kascov health endpoint did not become ready")


def _stop(process: subprocess.Popen) -> None:
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def _start_service(args, directory: pathlib.Path, port: int, profile: dict) -> subprocess.Popen:
    command = [
        str(args.kascov_bin),
        "--rpc",
        "ws://127.0.0.1:1",
        "serve",
        "--listen",
        f"127.0.0.1:{port}",
        "--networks",
        args.network,
        "--db-dir",
        str(directory),
        "--fetch-ahead",
        str(profile["fetch_ahead"]),
        "--wal-autocheckpoint",
        str(profile["wal_autocheckpoint"]),
        "--read-pool",
        str(profile["read_pool"]),
        "--replay-page-size",
        str(profile["replay_page"]),
    ]
    return subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def _ensure_fixture(args) -> str:
    args.db_fixture.parent.mkdir(parents=True, exist_ok=True)
    completed = subprocess.run(
        [
            str(args.kascov_bin.with_name("kascov-bench")),
            "seed-delivery",
            "--database",
            str(args.db_fixture),
            "--network",
            args.network,
            "--records",
            "1024",
        ],
        cwd=ROOT,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError("Kascov could not seed the tuning fixture database")
    digest = hashlib.sha256(args.db_fixture.read_bytes()).hexdigest()
    return f"sqlite:{args.network}:sha256:{digest}"


def _replay_bounds(stream_info: dict) -> tuple[str, str]:
    current = stream_info["current"]
    epoch, separator, sequence = current.rpartition(":")
    if not separator or not epoch or int(sequence) < 1:
        raise RuntimeError("tuning fixture must expose a non-empty durable stream")
    return f"{epoch}:0", current


def _benchmark_module():
    path = ROOT / "scripts/benchmark_service.py"
    spec = importlib.util.spec_from_file_location("benchmark_service", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _run_ingestion(args, plan: dict, raw_directory: pathlib.Path) -> list[dict]:
    bench = args.kascov_bin.with_name("kascov-bench")
    candidates = []
    for candidate_index, candidate in enumerate(plan["ingestion"]):
        runs = []
        for repetition in range(args.repetitions):
            raw = raw_directory / f"ingestion-{candidate_index}-{repetition}.json"
            command = [
                str(bench),
                "fixture",
                "--blocks",
                "10000",
                "--events-per-block",
                "4",
                "--output",
                str(raw),
                "--fetch-ahead",
                str(candidate["fetch_ahead"]),
                "--wal-autocheckpoint",
                str(candidate["wal_autocheckpoint"]),
                "--read-pool",
                str(INITIAL["read_pool"]),
                "--replay-page",
                str(INITIAL["replay_page"]),
            ]
            completed = subprocess.run(command, cwd=ROOT, check=False)
            runs.append(
                {
                    "returncode": completed.returncode,
                    "raw_result": str(raw),
                    "report": json.loads(raw.read_text()) if completed.returncode == 0 else None,
                }
            )
        candidates.append(
            {
                **candidate,
                "evidence_scope": "deterministic_harness_only",
                "status": "inconclusive_without_live_accepted_samples",
                "runs": runs,
            }
        )
    return candidates


def _run_serving(
    args, plan: dict, raw_directory: pathlib.Path, source_identity: str
) -> list[dict]:
    benchmark = _benchmark_module()
    duration = float(os.environ.get("KASCOV_TUNING_DURATION_SECONDS", "1.25"))
    candidates = []
    for candidate_index, candidate in enumerate(plan["serving"]):
        profile = {**INITIAL, **candidate}
        workloads = []
        for multiplier in args.load_multipliers:
            runs = []
            for repetition in range(args.repetitions):
                with tempfile.TemporaryDirectory(prefix="kascov-tuning-run-") as temporary:
                    directory = pathlib.Path(temporary)
                    shutil.copy2(args.db_fixture, directory / f"{args.network}.db")
                    port = args.base_port + 1 + candidate_index
                    process = _start_service(args, directory, port, profile)
                    raw = raw_directory / (
                        f"serving-{candidate_index}-{multiplier}-{repetition}.json"
                    )
                    try:
                        base = f"http://127.0.0.1:{port}"
                        _wait_ready(base, process)
                        with urllib.request.urlopen(
                            f"{base}/data/{args.network}/stream-info.json", timeout=5
                        ) as response:
                            stream_after, stream_current = _replay_bounds(
                                json.load(response)
                            )
                        benchmark.run(
                            argparse.Namespace(
                                base=base,
                                network=args.network,
                                sample_source="deterministic_fixture",
                                source_identity=source_identity,
                                node_identity=None,
                                duration_seconds=duration,
                                streams=min(512, 8 * multiplier),
                                point_rps=100 * multiplier,
                                page_rps=100 * multiplier,
                                output=raw,
                                database=str(directory / f"{args.network}.db"),
                                service_pid=process.pid,
                                timeout_seconds=5.0,
                                stream_after=stream_after,
                                stream_current=stream_current,
                            )
                        )
                        runs.append(json.loads(raw.read_text()))
                    finally:
                        _stop(process)
            workloads.append(
                {
                    "multiplier": multiplier,
                    "repetitions": args.repetitions,
                    "runs": runs,
                }
            )
        candidates.append({**candidate, "workloads": workloads})
    return candidates


def run(args) -> dict:
    source_identity = _ensure_fixture(args)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    raw_directory = args.output.with_suffix("")
    raw_directory.mkdir(parents=True, exist_ok=True)
    plan = sweep_plan(
        args.fetch_ahead,
        args.wal_autocheckpoint,
        args.read_pool,
        args.replay_page,
    )
    ingestion = _run_ingestion(args, plan, raw_directory)
    serving = _run_serving(args, plan, raw_directory, source_identity)
    report = {
        "schema_version": 2,
        "sample_source": "deterministic_fixture",
        "source_identity": source_identity,
        "hardware": _hardware(),
        "network": args.network,
        "binary": str(args.kascov_bin),
        "database_fixture": str(args.db_fixture),
        "fixed_candidates": FIXED,
        "initial": INITIAL,
        "repetitions": args.repetitions,
        "ingestion": ingestion,
        "serving": serving,
        "selection": {
            "ingestion": {"status": "no_passing_candidate", **INITIAL},
            "serving": select_serving(serving),
        },
    }
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--kascov-bin", type=pathlib.Path, required=True)
    parser.add_argument("--base-port", type=int, required=True)
    parser.add_argument("--network", required=True)
    parser.add_argument("--db-fixture", type=pathlib.Path, required=True)
    parser.add_argument("--fetch-ahead", required=True)
    parser.add_argument("--wal-autocheckpoint", required=True)
    parser.add_argument("--read-pool", required=True)
    parser.add_argument("--replay-page", required=True)
    parser.add_argument("--load-multipliers", required=True)
    parser.add_argument("--repetitions", type=int, required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    args = parser.parse_args()
    args.fetch_ahead = parse_candidates(args.fetch_ahead, "fetch_ahead")
    args.wal_autocheckpoint = parse_candidates(
        args.wal_autocheckpoint, "wal_autocheckpoint"
    )
    args.read_pool = parse_candidates(args.read_pool, "read_pool")
    args.replay_page = parse_candidates(args.replay_page, "replay_page")
    try:
        args.load_multipliers = tuple(
            dict.fromkeys(int(value.strip()) for value in args.load_multipliers.split(","))
        )
    except ValueError:
        parser.error("--load-multipliers must contain integers")
    if not args.load_multipliers or any(
        value <= 0 or value & (value - 1) for value in args.load_multipliers
    ):
        parser.error("--load-multipliers must contain positive powers of two")
    if args.repetitions <= 0:
        parser.error("--repetitions must be positive")
    if not args.kascov_bin.is_file():
        parser.error("--kascov-bin must exist")
    return args


if __name__ == "__main__":
    run(parse_args())
