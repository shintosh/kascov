#!/usr/bin/env python3
"""Bounded HTTP, SSE, and resource benchmark for one local Kascov service."""

import argparse
import concurrent.futures
import json
import os
import pathlib
import platform
import resource
import subprocess
import sys
import threading
import time
import urllib.request


MIN_PERCENTILE_OBSERVATIONS = 100


def _percentile(sorted_values, percentile):
    rank = max(0, (percentile * len(sorted_values) + 99) // 100 - 1)
    return sorted_values[min(rank, len(sorted_values) - 1)]


def summarize_latency(samples):
    values = sorted(float(value) for value in samples)
    summary = {
        "observations": len(values),
        "p50_ms": None,
        "p95_ms": None,
        "p99_ms": None,
    }
    if len(values) < MIN_PERCENTILE_OBSERVATIONS:
        return summary
    summary.update(
        p50_ms=_percentile(values, 50),
        p95_ms=_percentile(values, 95),
        p99_ms=_percentile(values, 99),
    )
    return summary


def _hardware():
    return {
        "os": platform.platform(),
        "architecture": platform.machine(),
        "logical_cpus": os.cpu_count() or 1,
        "python": platform.python_version(),
    }


def build_report(
    *,
    network,
    duration_seconds,
    point_samples,
    page_samples,
    stream_events,
    requests_ok,
    requests_failed,
    rss_bytes,
    database_bytes,
    wal_bytes,
):
    elapsed = max(float(duration_seconds), sys.float_info.epsilon)
    return {
        "schema_version": 1,
        "sample_source": "live_node",
        "hardware": _hardware(),
        "workload": {
            "network": network,
            "duration_seconds": elapsed,
            "point_requests": len(point_samples),
            "page_requests": len(page_samples),
            "stream_events": stream_events,
        },
        "latency": {
            "point": summarize_latency(point_samples),
            "page": summarize_latency(page_samples),
        },
        "throughput": {
            "requests_per_second": requests_ok / elapsed,
            "stream_events_per_second": stream_events / elapsed,
            "requests_ok": requests_ok,
            "requests_failed": requests_failed,
        },
        "resources": {
            "rss_bytes": int(rss_bytes),
            "database_bytes": int(database_bytes),
            "wal_bytes": int(wal_bytes),
        },
    }


def _rss_bytes(pid):
    if pid is not None:
        try:
            output = subprocess.run(
                ["ps", "-o", "rss=", "-p", str(pid)],
                check=True,
                capture_output=True,
                text=True,
            ).stdout
            return int(output.strip()) * 1024
        except (OSError, subprocess.CalledProcessError, ValueError):
            return 0
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(value if sys.platform == "darwin" else value * 1024)


def _file_size(path):
    if path is None:
        return 0
    try:
        return pathlib.Path(path).stat().st_size
    except FileNotFoundError:
        return 0


def _get_latency(url, timeout):
    started = time.monotonic()
    with urllib.request.urlopen(url, timeout=timeout) as response:
        response.read()
    return (time.monotonic() - started) * 1000.0


def _stream_worker(url, stop, counter, lock):
    while not stop.is_set():
        try:
            request = urllib.request.Request(url, headers={"Accept": "text/event-stream"})
            with urllib.request.urlopen(request, timeout=10) as response:
                for raw_line in response:
                    if stop.is_set():
                        return
                    if raw_line.startswith(b"data:"):
                        with lock:
                            counter[0] += 1
        except OSError:
            if not stop.is_set():
                time.sleep(0.1)


def run(args):
    base = args.base.rstrip("/")
    point_url = f"{base}/data/{args.network}-live.json"
    page_url = f"{base}/data/{args.network}/events?limit=100"
    stream_url = f"{base}/data/{args.network}/stream"
    stop = threading.Event()
    lock = threading.Lock()
    stream_events = [0]
    stream_threads = [
        threading.Thread(
            target=_stream_worker,
            args=(stream_url, stop, stream_events, lock),
            daemon=True,
        )
        for _ in range(args.streams)
    ]
    for thread in stream_threads:
        thread.start()

    point_samples = []
    page_samples = []
    failed = 0
    futures = {}
    started = time.monotonic()
    deadline = started + args.duration_seconds
    next_point = started
    next_page = started
    workers = max(4, min(64, args.point_rps + args.page_rps))
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        while time.monotonic() < deadline:
            now = time.monotonic()
            while args.point_rps > 0 and next_point <= now:
                futures[executor.submit(_get_latency, point_url, args.timeout_seconds)] = "point"
                next_point += 1.0 / args.point_rps
            while args.page_rps > 0 and next_page <= now:
                futures[executor.submit(_get_latency, page_url, args.timeout_seconds)] = "page"
                next_page += 1.0 / args.page_rps
            time.sleep(0.001)

        for future in concurrent.futures.as_completed(futures):
            kind = futures[future]
            try:
                sample = future.result()
            except OSError:
                failed += 1
                continue
            (point_samples if kind == "point" else page_samples).append(sample)

    elapsed = time.monotonic() - started
    stop.set()
    for thread in stream_threads:
        thread.join(timeout=0.05)
    database = pathlib.Path(args.database) if args.database else None
    with lock:
        event_count = stream_events[0]
    report = build_report(
        network=args.network,
        duration_seconds=elapsed,
        point_samples=point_samples,
        page_samples=page_samples,
        stream_events=event_count,
        requests_ok=len(point_samples) + len(page_samples),
        requests_failed=failed,
        rss_bytes=_rss_bytes(args.service_pid),
        database_bytes=_file_size(database),
        wal_bytes=_file_size(f"{database}-wal") if database else 0,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", required=True)
    parser.add_argument("--network", required=True)
    parser.add_argument("--duration-seconds", type=float, required=True)
    parser.add_argument("--streams", type=int, required=True)
    parser.add_argument("--point-rps", type=int, required=True)
    parser.add_argument("--page-rps", type=int, required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    parser.add_argument("--database")
    parser.add_argument("--service-pid", type=int)
    parser.add_argument("--timeout-seconds", type=float, default=5.0)
    args = parser.parse_args()
    if args.duration_seconds <= 0:
        parser.error("--duration-seconds must be greater than zero")
    for name in ("streams", "point_rps", "page_rps"):
        if getattr(args, name) < 0:
            parser.error(f"--{name.replace('_', '-')} cannot be negative")
    return args


if __name__ == "__main__":
    run(parse_args())
