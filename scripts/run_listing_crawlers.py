#!/usr/bin/env python3
"""Launch multiple Google Maps listing crawler instances in parallel."""

from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import threading
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Launch parallel listing crawler instances"
    )
    parser.add_argument(
        "--config",
        default="config/gmaps_listings_working.yaml",
        help="Path to listing crawler config",
    )
    parser.add_argument(
        "--instances",
        type=int,
        default=4,
        help="Number of crawler processes to launch",
    )
    parser.add_argument(
        "--headless",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Run Chrome headless",
    )
    return parser.parse_args()


def stream_output(instance_name: str, stream):
    for line in iter(stream.readline, ""):
        if not line:
            break
        print(f"[{instance_name}] {line}", end="", flush=True)


def main() -> int:
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[1]

    processes: list[tuple[str, subprocess.Popen[str]]] = []
    threads: list[threading.Thread] = []

    def terminate_all(_sig=None, _frame=None):
        for _, proc in processes:
            if proc.poll() is None:
                proc.terminate()

    signal.signal(signal.SIGINT, terminate_all)
    signal.signal(signal.SIGTERM, terminate_all)

    for i in range(1, args.instances + 1):
        instance_name = f"listing-{i}"
        env = os.environ.copy()
        env["SCRAPER_INSTANCE_NAME"] = instance_name

        cmd = [
            sys.executable,
            "main.py",
            "--config",
            args.config,
            "--instance-label",
            instance_name,
        ]
        if args.headless:
            cmd.append("--headless")
        else:
            cmd.append("--no-headless")

        proc = subprocess.Popen(
            cmd,
            cwd=str(repo_root),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        processes.append((instance_name, proc))

        assert proc.stdout is not None
        thread = threading.Thread(
            target=stream_output, args=(instance_name, proc.stdout), daemon=True
        )
        thread.start()
        threads.append(thread)

    exit_codes = []
    try:
        for instance_name, proc in processes:
            exit_codes.append(proc.wait())
    finally:
        terminate_all()
        for thread in threads:
            thread.join(timeout=1)

    return max(exit_codes) if exit_codes else 0


if __name__ == "__main__":
    raise SystemExit(main())
