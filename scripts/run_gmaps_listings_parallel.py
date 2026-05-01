#!/usr/bin/env python3
"""Launch multiple labeled Google Maps listing crawler processes."""

import argparse
import signal
import subprocess
import sys
from pathlib import Path


def build_command(config_path: str, instance_label: str) -> list[str]:
    return [
        sys.executable,
        "main.py",
        "--config",
        config_path,
        "--instance-label",
        instance_label,
    ]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Launch 4 parallel Google Maps listing crawler processes"
    )
    parser.add_argument(
        "--config",
        default="config/gmaps_listings_working.yaml",
        help="Crawler config to pass to each process",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=4,
        help="Number of crawler processes to start",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    processes: list[subprocess.Popen[str]] = []

    def terminate_children(signum, frame):
        for process in processes:
            if process.poll() is None:
                process.terminate()

    signal.signal(signal.SIGINT, terminate_children)
    signal.signal(signal.SIGTERM, terminate_children)

    for index in range(args.count):
        label = f"crawler-{index + 1}"
        command = build_command(args.config, label)
        process = subprocess.Popen(command, cwd=repo_root)
        processes.append(process)
        print(f"started {label}: {' '.join(command)}")

    exit_code = 0
    try:
        for process in processes:
            result = process.wait()
            if result != 0 and exit_code == 0:
                exit_code = result
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
