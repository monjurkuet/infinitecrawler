#!/usr/bin/env bash
# watchdog.sh — BD pipeline health watchdog with auto-restart.
# Logs status, auto-heals dead crawlers, reports only on issues or heal actions.
set -u
cd "$(cd "$(dirname "$0")/.." && pwd)"

# systemd runs without login PATH; locate uv + python explicitly.
UV_BIN="$(command -v uv 2>/dev/null || echo /root/.local/bin/uv)"
PYTHON_BIN="$(command -v python 2>/dev/null || echo /root/codebase/vhd/infinitecrawler/.venv/bin/python3)"

# Ensure all 7 daemons are alive (idempotent + 60s backoff per label).
if ! PATH="/root/.local/bin:/root/codebase/vhd/infinitecrawler/.venv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin" bash scripts/launch_daemons.sh; then
  echo "launch_daemons.sh: spawned something; sleeping 15s before health check"
  sleep 15
fi

REPORT=$(PATH="/root/.local/bin:/root/codebase/vhd/infinitecrawler/.venv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin" "$UV_BIN" run "$PYTHON_BIN" scripts/monitor_pipeline.py --restart --quiet --json 2>&1) || true
echo "$REPORT"

# Tidy: drop lastspawn_*.ts older than 7 days to keep _system/ bounded.
find _system -name 'lastspawn_*.ts' -mtime +7 -delete 2>/dev/null

# Webhook alert on staleness (1h threshold)
if echo "$REPORT" | grep -qi "staleness\|STALENESS\|no new.*data\|dead"; then
  WEBHOOK_URL="${HEALTHCHECK_WEBHOOK_URL:-}"
  if [ -n "$WEBHOOK_URL" ]; then
    curl -sf -X POST -H "Content-Type: application/json" \
      -d "{\"text\": \"InfiniteCrawler STALENESS: $REPORT\"}" \
      "$WEBHOOK_URL" 2>/dev/null || true
  fi
fi
