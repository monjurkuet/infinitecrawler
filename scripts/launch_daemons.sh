#!/usr/bin/env bash
# launch_daemons.sh — spawn every pipeline daemon if not already running.
# Used by watchdog.sh and the `start` API endpoint.  Idempotent + backoff.
set -u
cd "$(cd "$(dirname "$0")/.." && pwd)"
# Make sure `uv` is on PATH for child processes (systemd has minimal PATH).
export PATH="/root/.local/bin:/root/codebase/vhd/infinitecrawler/.venv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
LOG_DIR=/var/log/infinitecrawler
mkdir -p "$LOG_DIR" _system

ensure_daemon() {
  local label="$1" pattern="$2" cmd="$3"
  if pgrep -f "$pattern" >/dev/null; then
    return 0  # already alive
  fi
  local bf="_system/lastspawn_${label}.ts"
  if [[ -f "$bf" ]] && (( $(date +%s) - $(cat "$bf") < 60 )); then
    return 0  # backoff: skip spawn within 60s of last attempt
  fi
  date +%s > "$bf"
  nohup bash -c "exec $cmd" >> "$LOG_DIR/infinitecrawler-${label}.log" 2>&1 &
  disown
  sleep 5  # prevent respawn race during pinchtab warm-up
  return 1  # signal "just spawned" so caller can pause monitoring
}

SPAWNED=0
ensure_daemon listing    'daemons\.listing_daemon' \
  'uv run python -m daemons.listing_daemon' && SPAWNED=1
ensure_daemon search     'daemons\.search_daemon' \
  'uv run python -m daemons.search_daemon' && SPAWNED=1
ensure_daemon email-extract 'db_email_extract\.py' \
  'uv run python scripts/db_email_extract.py --loop --loop-gap 30 --max 2000 --concurrency 25 --burst 3' && SPAWNED=1
ensure_daemon linkedin-firehose 'db_linkedin_firehose\.py' \
  'uv run python scripts/db_linkedin_firehose.py --max-queries 8000 --concurrency 8 --loop --loop-gap 60' && SPAWNED=1
ensure_daemon linkedin-search 'db_linkedin_search\.py' \
  'uv run python scripts/db_linkedin_search.py --loop --loop-gap 600 --max 2000' && SPAWNED=1
ensure_daemon classify   'db_classify\.py' \
  'uv run python scripts/db_classify.py --loop --loop-gap 300 --max 2000' && SPAWNED=1
ensure_daemon linkedin-match 'match_linkedin_to_gmaps\.py' \
  'uv run python scripts/match_linkedin_to_gmaps.py --loop --loop-gap 900' && SPAWNED=1

[[ "$SPAWNED" == "0" ]]
