#!/usr/bin/env bash
# launch_daemons.sh — start the 4 perpetual pipeline daemons via systemd.
#
# Scope: ONLY the 4 perpetually-looped daemons that have *-loop.service units:
#   - infinitecrawler-listing.service
#   - infinitecrawler-search.service
#   - infinitecrawler-email-extract-loop.service
#   - infinitecrawler-linkedin-firehose-loop.service
#
# NOT in scope (managed by their own timers as oneshots):
#   - infinitecrawler-linkedin-search.service  (4h timer)
#   - infinitecrawler-classify.service         (daily 03:00 timer)
#   - infinitecrawler-linkedin-match.service   (4x/day timer)
#
# Uses `systemctl --user` so processes live in systemd's cgroup and are
# tracked/lifecycle-managed by systemd (no nohup orphans).
# Idempotent + 60s backoff via _system/lastspawn_<label>.ts.
set -u
cd "$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR=/var/log/infinitecrawler
mkdir -p "$LOG_DIR" _system

ensure_daemon() {
  local label="$1" pattern="$2" unit="$3"
  if pgrep -f "$pattern" >/dev/null; then
    return 0  # already alive
  fi
  local bf="_system/lastspawn_${label}.ts"
  if [[ -f "$bf" ]] && (( $(date +%s) - $(cat "$bf") < 60 )); then
    return 0  # backoff: skip spawn within 60s of last attempt
  fi
  date +%s > "$bf"
  systemctl --user start "$unit" >/dev/null 2>&1 || true
  return 1  # signal "just spawned" so caller can pause monitoring
}

SPAWNED=0
ensure_daemon listing    'daemons\.listing_daemon' \
  'infinitecrawler-listing.service' && SPAWNED=1
ensure_daemon search     'daemons\.search_daemon' \
  'infinitecrawler-search.service' && SPAWNED=1
ensure_daemon email-extract 'db_email_extract\.py' \
  'infinitecrawler-email-extract-loop.service' && SPAWNED=1
ensure_daemon linkedin-firehose 'db_linkedin_firehose\.py' \
  'infinitecrawler-linkedin-firehose-loop.service' && SPAWNED=1

[[ "$SPAWNED" == "0" ]]
