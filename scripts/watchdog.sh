#!/usr/bin/env bash
# watchdog.sh — BD pipeline health watchdog with auto-restart.
# Logs status, auto-heals dead crawlers, reports only on issues or heal actions.
set -euo pipefail
cd "$(cd "$(dirname "$0")/.." && pwd)"
REPORT=$(uv run python scripts/monitor_pipeline.py --restart --quiet --json 2>&1) || true
echo "$REPORT"

# Webhook alert on staleness (1h threshold)
if echo "$REPORT" | grep -qi "staleness\|STALENESS\|no new.*data\|dead"; then
  WEBHOOK_URL="${HEALTHCHECK_WEBHOOK_URL:-}"
  if [ -n "$WEBHOOK_URL" ]; then
    curl -sf -X POST -H "Content-Type: application/json" \
      -d "{\"text\": \"InfiniteCrawler STALENESS: $REPORT\"}" \
      "$WEBHOOK_URL" 2>/dev/null || true
  fi
fi
