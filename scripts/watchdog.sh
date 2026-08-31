#!/usr/bin/env bash
# watchdog.sh — BD pipeline health watchdog with auto-restart.
# Logs status, auto-heals dead crawlers, reports only on issues or heal actions.
#
# Host-corrected (was hardcoded for /root on a different box). This host uses:
#   - Growloop-owned uv-managed venv: /home/growloop/.venvs/ic/bin/python3
#   - systemd user units (infinitecrawler-*.service) managed via `systemctl --user`
set -u
cd "$(cd "$(dirname "$0")/.." && pwd)"

PYTHON_BIN="/home/growloop/.venvs/ic/bin/python3"
export PG_HOST="${PG_HOST:-/var/run/postgresql}"
export PYTHONUNBUFFERED=1

# Ensure all perpetual daemons are alive (idempotent + 60s backoff per label).
if ! bash scripts/launch_daemons.sh; then
  echo "launch_daemons.sh: spawned something; sleeping 15s before health check"
  sleep 15
fi

REPORT=$("$PYTHON_BIN" scripts/monitor_pipeline.py --restart --quiet --json 2>&1) || true
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
