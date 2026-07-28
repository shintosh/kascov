#!/usr/bin/env python3
"""Run the complete local Kascov data-service contract proof."""

from __future__ import annotations

import argparse
import glob
import json
import os
import pathlib
import subprocess
import time


ROOT = pathlib.Path(__file__).resolve().parents[1]


def command_specs(root: pathlib.Path = ROOT) -> list[list[str]]:
    python_tests = sorted(
        glob.glob(str(root / "scripts/test_*.py"))
        + glob.glob(str(root / "clients/py/test_*.py"))
    )
    node_tests = sorted(
        glob.glob(str(root / "clients/js/*.test.mjs"))
        + glob.glob(str(root / "web/*.test.mjs"))
    )
    return [
        ["cargo", "test", "-p", "kascov-core", "--lib", "--tests"],
        ["cargo", "test", "-p", "kascov-argent"],
        ["cargo", "test", "-p", "kascov", "--bin", "kascov", "--tests"],
        ["python3", "-m", "unittest", *python_tests],
        ["node", "--test", *node_tests],
    ]


def run_commands(commands, *, runner=subprocess.run, cwd: pathlib.Path = ROOT) -> dict:
    started = time.monotonic()
    results = []
    for command in commands:
        command_started = time.monotonic()
        environment = None
        if command[:3] == ["python3", "-m", "unittest"]:
            environment = os.environ.copy()
            existing = environment.get("PYTHONPATH")
            client_path = str(cwd / "clients/py")
            environment["PYTHONPATH"] = (
                client_path if not existing else client_path + os.pathsep + existing
            )
        completed = runner(command, cwd=cwd, check=False, env=environment)
        result = {
            "command": command,
            "returncode": completed.returncode,
            "elapsed_seconds": time.monotonic() - command_started,
        }
        results.append(result)
        if completed.returncode != 0:
            break
    return {
        "schema_version": 1,
        "status": "passed" if len(results) == len(commands) and all(
            result["returncode"] == 0 for result in results
        ) else "failed",
        "elapsed_seconds": time.monotonic() - started,
        "commands": results,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--list", action="store_true", help="print commands without running them")
    parser.add_argument("--output", type=pathlib.Path, help="write the JSON result to this path")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    commands = command_specs()
    if args.list:
        print(json.dumps(commands, indent=2))
        return 0
    report = run_commands(commands)
    encoded = json.dumps(report, indent=2) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded, encoding="utf-8")
    print(encoded, end="")
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
