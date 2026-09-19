#!/usr/bin/env bash
# ic_status.sh — 25-line live dashboard for InfiniteCrawler
# Usage: ic_status.sh [--watch [interval_sec]]  (default interval 30s)
# Requires: PG_HOST/PG_PORT in .env or env, redis-cli, systemctl --user

set -euo pipefail

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
ENV_FILE="$REPO_ROOT/.env"
[[ -f "$ENV_FILE" ]] && source "$ENV_FILE"
export PGPASSWORD="${PGPASSWORD:-changeme}"
PSQL="psql -h ${PG_HOST:-/var/run/postgresql} -p ${PG_PORT:-5432} -U postgres -d infinitecrawler -tA -F'|'"
REDIS="redis-cli -n 0"

WATCH=0
INTERVAL=30
for arg in "$@"; do
  case $arg in
    --watch) WATCH=1 ;;
    *) [[ $arg =~ ^[0-9]+$ ]] && INTERVAL=$arg ;;
  esac
done

hr() { printf '─%.0s' {1..80}; echo; }
cell() { printf '%-26s' "$1"; }
grid3() { cell "$1"; cell "$2"; echo "$3"; }
grid4() { cell "$1"; cell "$2"; cell "$3"; echo "$4"; }
age() { 
    local ts="$1"
    [[ -z "$ts" || "$ts" = "0" ]] && echo "?" && return
    date -d "@$ts" '+%H:%M' 2>/dev/null || date -r "$ts" '+%H:%M' 2>/dev/null || echo "?"; 
  }

collect() {
  # ── DAEMONS ──
  mapfile -t UNITS < <(systemctl --user list-units 'infinitecrawler-*.service' --no-legend --plain 2>/dev/null | awk '{print $1,$3,$4}')
  UP=0; TOT=${#UNITS[@]}
  for u in "${UNITS[@]}"; do [[ $u == *active* ]] && UP=$((UP+1)); done

  # ── PG COUNTS ──
  IFS='|' read -r LIST_TOTAL LIST_1H LIST_24H < <($PSQL -c "
    SELECT count(*), count(*) FILTER (WHERE created_at > now() - interval '1 hour'),
           count(*) FILTER (WHERE created_at > now() - interval '24 hour')
    FROM scraper.gmaps_listings" 2>/dev/null || echo "0|0|0")
  IFS='|' read -r SRCH_TOTAL SRCH_1H SRCH_24H < <($PSQL -c "
    SELECT count(*), count(*) FILTER (WHERE created_at > now() - interval '1 hour'),
           count(*) FILTER (WHERE created_at > now() - interval '24 hour')
    FROM scraper.gmaps_search_results" 2>/dev/null || echo "0|0|0")
  IFS='|' read -r EMAIL_TOTAL EMAIL_1H EMAIL_24H < <($PSQL -c "
    SELECT count(*), count(*) FILTER (WHERE discovered_at > now() - interval '1 hour'),
           count(*) FILTER (WHERE discovered_at > now() - interval '24 hour')
    FROM scraper.emails" 2>/dev/null || echo "0|0|0")
  IFS='|' read -r BBB_TOTAL BBB_1H BBB_24H < <($PSQL -c "
    SELECT count(*), count(*) FILTER (WHERE created_at > now() - interval '1 hour'),
           count(*) FILTER (WHERE created_at > now() - interval '24 hour')
    FROM scraper.bbb_listings" 2>/dev/null || echo "0|0|0")
  IFS='|' read -r LJ_TOTAL LJ_1H LJ_24H < <($PSQL -c "
    SELECT count(*), count(*) FILTER (WHERE created_at > now() - interval '1 hour'),
           count(*) FILTER (WHERE created_at > now() - interval '24 hour')
    FROM scraper.linkedin_jobs" 2>/dev/null || echo "0|0|0")
  IFS='|' read -r LJC_TOTAL < <($PSQL -c "SELECT count(*) FROM scraper.linkedin_companies" 2>/dev/null || echo "0")

  # ── REDIS QUEUES ──
  SRCH_PEND=$($REDIS LLEN gmaps:pending 2>/dev/null || echo 0)
  SRCH_PROC=$($REDIS LLEN gmaps:processing 2>/dev/null || echo 0)
  SRCH_PHAN=$($REDIS LLEN gmaps:phantom 2>/dev/null || echo 0)
  SRCH_FAIL=$($REDIS HLEN gmaps:failed 2>/dev/null || echo 0)
  LIST_PEND=$($REDIS LLEN listing:pending 2>/dev/null || echo 0)
  LIST_PROC=$($REDIS LLEN listing:processing 2>/dev/null || echo 0)
  LIST_PHAN=$($REDIS LLEN listing:phantom 2>/dev/null || echo 0)
  LIST_POOL=$($REDIS SCARD listing:browser_pool 2>/dev/null || echo 0)
  NEARBY_PEND=$($REDIS LLEN nearby:pending 2>/dev/null || echo 0)
  NEARBY_QUOTA=$($REDIS GET nearby:quota_reset_ts 2>/dev/null || echo 0)
  PLACES_QUOTA=$($REDIS GET places:quota_reset_ts 2>/dev/null || echo 0)
  BBB_PEND=$($REDIS LLEN bbb:pending 2>/dev/null || echo 0)
  BBB_PROC=$($REDIS SCARD bbb:processing 2>/dev/null || echo 0)
  EMAIL_PEND=$($REDIS LLEN email:pending 2>/dev/null || echo 0)
  EMAIL_BROW=$($REDIS SCARD email:browser_pool 2>/dev/null || echo 0)
  LJ_PEND=$($REDIS SCARD linkedin:jobs:pending 2>/dev/null || echo 0)
  LJ_PROC=$($REDIS SCARD linkedin:jobs:processing 2>/dev/null || echo 0)
  LJ_COMP=$($REDIS SCARD linkedin:companies:pending 2>/dev/null || echo 0)

  # ── FILL RATES (1h window) ──
  IFS='|' read -r PHONE WEB RATE ADDR CAT PHAN < <($PSQL -c "
    SELECT round(100.0*count(phone) FILTER (WHERE phone IS NOT NULL)/NULLIF(count(*),0),1),
           round(100.0*count(website) FILTER (WHERE website IS NOT NULL)/NULLIF(count(*),0),1),
           round(100.0*count(rating) FILTER (WHERE rating IS NOT NULL)/NULLIF(count(*),0),1),
           round(100.0*count(address) FILTER (WHERE address IS NOT NULL)/NULLIF(count(*),0),1),
           round(100.0*count(category) FILTER (WHERE category IS NOT NULL)/NULLIF(count(*),0),1),
           count(*) FILTER (WHERE source_url LIKE '%?cid=%')
    FROM scraper.gmaps_listings
    WHERE created_at > now() - interval '1 hour'" 2>/dev/null || echo "0|0|0|0|0|0")

  # ── NEARBY / PLACES DAILY ──
  NEARBY_DAY=$($PSQL -c "SELECT count(*) FROM scraper.gmaps_listings WHERE source_type='nearby_search' AND created_at > now() - interval '24 hour'" 2>/dev/null || echo 0)
  PLACES_DAY=0  # no distinct source_type

  # ── BLOG / CLASSIFY ──
  BLOG_PEND=$($REDIS LLEN blog:pending 2>/dev/null || echo 0)
  CLASS_PEND=$($REDIS LLEN classify:pending 2>/dev/null || echo 0)

  # ── SERVICE PINGS ──
  API=$(curl -sf -m 2 http://localhost:8015/health 2>/dev/null | grep -q '"status":"ok"' && echo ✓ || echo ✗)
  PRM=$(curl -sf -m 2 http://localhost:8016/health 2>/dev/null | grep -q '"status":"ok"' && echo ✓ || echo ✗)
  WEB=$(curl -sf -m 2 http://localhost:5173 2>/dev/null | grep -q '<html' && echo ✓ || echo ✗)
  ADM=$(curl -sf -m 2 http://localhost:5174 2>/dev/null | grep -q '<html' && echo ✓ || echo ✗)
  PINCH=$(curl -sf -m 2 http://localhost:9222/json/version 2>/dev/null | grep -q '"Browser"' && echo ✓ || echo ✗)

  # ── LAST FLUSH ──
  FLUSH=$($REDIS GET ic:last_flush 2>/dev/null | xargs -I{} date -d @{} '+%H:%M:%S' 2>/dev/null || echo "never")
}

render() {
  clear
  printf '  IC %d/%d  Δ %s            LAST FLUSH %s              PG ✓  REDIS ✓\n' "$UP" "$TOT" "$(date '+%H:%M')" "$FLUSH"
  hr
  grid3 "SEARCH" "LISTING" "PLACES          NEARBY"
  grid3 "seed $SRCH_PEND  1h↑$SRCH_1H" "cre $LIST_PROC/h  ph $LIST_PHAN" "day $PLACES_DAY  quota $(age "$PLACES_QUOTA")" "day $NEARBY_DAY  quota $(age "$NEARBY_QUOTA")"
  grid3 "15d $SRCH_TOTAL  24h $SRCH_24H" "upd $LIST_PEND/h   pool $LIST_POOL" "proc $SRCH_PROC  fail $SRCH_FAIL" "pend $NEARBY_PEND"
  grid4 "BBB" "EMAIL" "JOBS" "PROF"
  grid4 "cre $BBB_PROC/h  pnd $BBB_PEND" "http $EMAIL_PEND/h  brow $EMAIL_BROW" "det $LJ_PROC/h   pnd $LJ_PEND" "comp $LJ_COMP  tot $LJC_TOTAL"
  grid4 "24h $BBB_24H  tot $BBB_TOTAL" "24h $EMAIL_24H  tot $EMAIL_TOTAL" "24h $LJ_24H  tot $LJ_TOTAL" "24h $LJ_1H"
  hr
  printf '  FILL 1h  phone %s%%  web %s%%  rate %s%%  addr %s%%  cat %s%%  phantom %s\n' "$PHONE" "$WEB" "$RATE" "$ADDR" "$CAT" "$PHAN"
  hr
  printf '  BLOG  seed-out +%s/h  ·  web-pnd %s  ·  class %s  ·  grid %s\n' "$SRCH_1H" "$BLOG_PEND" "$CLASS_PEND" "$NEARBY_PEND"
  hr
  awk -v ltotal="$LIST_TOTAL" -v stotal="$SRCH_TOTAL" -v etotal="$EMAIL_TOTAL" -v ljtotal="$LJ_TOTAL" -v ljc="$LJC_TOTAL" \
    'BEGIN{printf "  TOT  lst %.1fM  srch %.1fM  eml %.1fM  lnk %sk  co %s\n", ltotal/1000000, stotal/1000000, etotal/1000000, ljtotal/1000, ljc}'
  hr
  printf '  WATCH  pinchtab %s  8015 %s  8016 %s  5173 %s  5174 %s\n' "$PINCH" "$API" "$PRM" "$WEB" "$ADM"
}

main() {
  while :; do
    collect
    render
    [[ $WATCH -eq 0 ]] && break
    sleep "$INTERVAL"
  done
}
main