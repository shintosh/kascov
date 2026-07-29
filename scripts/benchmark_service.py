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
import urllib.parse
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
    sample_source,
    source_identity,
    node_identity,
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
    replay,
):
    if sample_source not in ("deterministic_fixture", "live_node"):
        raise ValueError("sample_source must be deterministic_fixture or live_node")
    if not source_identity:
        raise ValueError("source_identity is required")
    if sample_source == "live_node" and not node_identity:
        raise ValueError("live_node samples require node_identity")
    elapsed = max(float(duration_seconds), sys.float_info.epsilon)
    return {
        "schema_version": 2,
        "sample_source": sample_source,
        "source_identity": source_identity,
        "node_identity": node_identity,
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
        "replay": replay,
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


def _cursor_parts(cursor):
    epoch, separator, sequence = cursor.rpartition(":")
    if not separator or not epoch:
        raise ValueError(f"invalid stream cursor: {cursor}")
    return epoch, int(sequence)


def _stream_worker(url, after, current, stop, result, lock):
    last_id = after
    ready_recorded = False
    target_epoch = target_sequence = None
    if current is not None:
        target_epoch, target_sequence = _cursor_parts(current)
    while not stop.is_set():
        try:
            headers = {"Accept": "text/event-stream"}
            if last_id != after:
                headers["Last-Event-ID"] = last_id
            request = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(request, timeout=10) as response:
                event_name = None
                event_id = None
                for raw_line in response:
                    if stop.is_set():
                        return
                    line = raw_line.decode("utf-8").rstrip("\r\n")
                    if line.startswith("event:"):
                        event_name = line[6:].strip()
                    elif line.startswith("id:"):
                        event_id = line[3:].strip()
                    elif line == "":
                        with lock:
                            if event_name == "ready" and not ready_recorded:
                                result["streams_ready"] += 1
                                ready_recorded = True
                            if event_id:
                                epoch, sequence = _cursor_parts(event_id)
                                if last_id is not None:
                                    last_epoch, last_sequence = _cursor_parts(last_id)
                                    if epoch != last_epoch or sequence != last_sequence + 1:
                                        result["cursor_errors"] += 1
                                result["records"] += 1
                                last_id = event_id
                                if target_epoch == epoch and target_sequence == sequence:
                                    result["streams_completed"] += 1
                                    return
                        event_name = None
                        event_id = None
        except OSError:
            if not stop.is_set():
                with lock:
                    result["connection_errors"] += 1
                time.sleep(0.1)


def run(args):
    base = args.base.rstrip("/")
    point_url = f"{base}/data/{args.network}-live.json"
    page_url = f"{base}/data/{args.network}/events?limit=100"
    stream_url = f"{base}/data/{args.network}/stream"
    if args.stream_after is not None:
        stream_url = f"{stream_url}?{urllib.parse.urlencode({'after': args.stream_after})}"
    stop = threading.Event()
    lock = threading.Lock()
    replay = {
        "streams_requested": args.streams,
        "streams_ready": 0,
        "streams_completed": 0,
        "records": 0,
        "expected_records": None,
        "cursor_errors": 0,
        "connection_errors": 0,
        "after": args.stream_after,
        "current": args.stream_current,
    }
    if args.stream_after is not None:
        after_epoch, after_sequence = _cursor_parts(args.stream_after)
        current_epoch, current_sequence = _cursor_parts(args.stream_current)
        if after_epoch != current_epoch or after_sequence > current_sequence:
            raise ValueError("stream replay bounds must share an epoch and be ordered")
        replay["expected_records"] = args.streams * (current_sequence - after_sequence)
    stream_threads = [
        threading.Thread(
            target=_stream_worker,
            args=(
                stream_url,
                args.stream_after,
                args.stream_current,
                stop,
                replay,
                lock,
            ),
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
        replay_result = dict(replay)
        event_count = replay_result["records"]
    report = build_report(
        sample_source=args.sample_source,
        source_identity=args.source_identity,
        node_identity=args.node_identity,
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
        replay=replay_result,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", required=True)
    parser.add_argument("--network", required=True)
    parser.add_argument(
        "--sample-source",
        choices=("deterministic_fixture", "live_node"),
        required=True,
    )
    parser.add_argument("--source-identity", required=True)
    parser.add_argument("--node-identity")
    parser.add_argument("--duration-seconds", type=float, required=True)
    parser.add_argument("--streams", type=int, required=True)
    parser.add_argument("--point-rps", type=int, required=True)
    parser.add_argument("--page-rps", type=int, required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    parser.add_argument("--database")
    parser.add_argument("--service-pid", type=int)
    parser.add_argument("--timeout-seconds", type=float, default=5.0)
    parser.add_argument("--stream-after")
    parser.add_argument("--stream-current")
    args = parser.parse_args()
    if args.duration_seconds <= 0:
        parser.error("--duration-seconds must be greater than zero")
    for name in ("streams", "point_rps", "page_rps"):
        if getattr(args, name) < 0:
            parser.error(f"--{name.replace('_', '-')} cannot be negative")
    if args.sample_source == "live_node" and not args.node_identity:
        parser.error("--node-identity is required for live_node samples")
    if (args.stream_after is None) != (args.stream_current is None):
        parser.error("--stream-after and --stream-current must be provided together")
    return args


if __name__ == "__main__":
    run(parse_args())
