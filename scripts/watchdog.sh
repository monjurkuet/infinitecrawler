#!/usr/bin/env bash
# watchdog.sh — BD pipeline health watchdog with auto-restart.
# Logs status, auto-heals dead crawlers, reports only on issues or heal actions.
set -euo pipefail
cd "$(cd "$(dirname "$0")/.." && pwd)"
uv run python scripts/monitor_pipeline.py --restart --quiet --json
