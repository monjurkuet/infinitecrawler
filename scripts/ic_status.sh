#!/usr/bin/env bash
# ic_status.sh — 50-line live dashboard for InfiniteCrawler
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

hr() { printf '─%.0s' {1..120}; echo; }
cell() { printf '%-30s' "$1"; }
grid3() { cell "$1"; cell "$2"; echo "$3"; }
grid4() { cell "$1"; cell "$2"; cell "$3"; echo "$4"; }
grid5() { cell "$1"; cell "$2"; cell "$3"; cell "$4"; echo "$5"; }
age() {
    local ts="$1"
    [[ -z "$ts" || "$ts" = "0" ]] && echo "?" && return
    date -d "@$ts" '+%H:%M' 2>/dev/null || date -r "$ts" '+%H:%M' 2>/dev/null || echo "?"
}

collect() {
  # ── DAEMONS ──
  mapfile -t UNITS < <(systemctl --user list-units 'infinitecrawler-*.service' --no-legend --plain 2>/dev/null | awk '{print $1,$3,$4}')
  UP=0; TOT=${#UNITS[@]}
  for u in "${UNITS[@]}"; do [[ $u == *active* ]] && UP=$((UP+1)); done

  # ── PG COUNTS ──
  IFS='|' read -r LIST_TOTAL LIST_1H LIST_24H LIST_7D < <($PSQL -c "
    SELECT count(*), count(*) FILTER (WHERE created_at > now() - interval '1 hour'),
           count(*) FILTER (WHERE created_at > now() - interval '24 hour'),
           count(*) FILTER (WHERE created_at > now() - interval '7 day')
    FROM scraper.gmaps_listings" 2>/dev/null || echo "0|0|0|0")
  IFS='|' read -r SRCH_TOTAL SRCH_1H SRCH_24H SRCH_7D < <($PSQL -c "
    SELECT count(*), count(*) FILTER (WHERE created_at > now() - interval '1 hour'),
           count(*) FILTER (WHERE created_at > now() - interval '24 hour'),
           count(*) FILTER (WHERE created_at > now() - interval '7 day')
    FROM scraper.gmaps_search_results" 2>/dev/null || echo "0|0|0|0")
  IFS='|' read -r EMAIL_TOTAL EMAIL_1H EMAIL_24H EMAIL_7D < <($PSQL -c "
    SELECT count(*), count(*) FILTER (WHERE discovered_at > now() - interval '1 hour'),
           count(*) FILTER (WHERE discovered_at > now() - interval '24 hour'),
           count(*) FILTER (WHERE discovered_at > now() - interval '7 day')
    FROM scraper.emails" 2>/dev/null || echo "0|0|0|0")
  IFS='|' read -r BBB_TOTAL BBB_1H BBB_24H BBB_7D < <($PSQL -c "
    SELECT count(*), count(*) FILTER (WHERE created_at > now() - interval '1 hour'),
           count(*) FILTER (WHERE created_at > now() - interval '24 hour'),
           count(*) FILTER (WHERE created_at > now() - interval '7 day')
    FROM scraper.bbb_listings" 2>/dev/null || echo "0|0|0|0")
  IFS='|' read -r LJ_TOTAL LJ_1H LJ_24H LJ_7D < <($PSQL -c "
    SELECT count(*), count(*) FILTER (WHERE created_at > now() - interval '1 hour'),
           count(*) FILTER (WHERE created_at > now() - interval '24 hour'),
           count(*) FILTER (WHERE created_at > now() - interval '7 day')
    FROM scraper.linkedin_jobs" 2>/dev/null || echo "0|0|0|0")
  IFS='|' read -r LJC_TOTAL LJC_1H LJC_24H < <($PSQL -c "
    SELECT count(*), count(*) FILTER (WHERE last_checked_at > now() - interval '1 hour'),
           count(*) FILTER (WHERE last_checked_at > now() - interval '24 hour')
    FROM scraper.linkedin_companies" 2>/dev/null || echo "0|0|0")

  # ── REDIS QUEUES ──
  SRCH_PEND=$($REDIS LLEN gmaps:pending 2>/dev/null || echo 0)
  SRCH_PROC=$($REDIS LLEN gmaps:processing 2>/dev/null || echo 0)
  SRCH_PHAN=$($REDIS LLEN gmaps:phantom 2>/dev/null || echo 0)
  SRCH_FAIL=$($REDIS HLEN gmaps:failed 2>/dev/null || echo 0)
  SRCH_BLOCKED=$($REDIS SCARD gmaps:phantom:blocked 2>/dev/null || echo 0)
  LIST_PEND=$($REDIS LLEN listing:pending 2>/dev/null || echo 0)
  LIST_PROC=$($REDIS LLEN listing:processing 2>/dev/null || echo 0)
  LIST_PHAN=$($REDIS LLEN listing:phantom 2>/dev/null || echo 0)
  LIST_POOL=$($REDIS SCARD listing:browser_pool 2>/dev/null || echo 0)
  NEARBY_PEND=$($REDIS LLEN nearby:pending 2>/dev/null || echo 0)
  NEARBY_QUOTA=$($REDIS GET nearby:quota_reset_ts 2>/dev/null || echo 0)
  PLACES_QUOTA=$($REDIS GET places:quota_reset_ts 2>/dev/null || echo 0)
  PLACES_KEYS=$($REDIS GET places:active_keys 2>/dev/null || echo 0)
  BBB_PEND=$($REDIS LLEN bbb:pending 2>/dev/null || echo 0)
  BBB_PROC=$($REDIS SCARD bbb:processing 2>/dev/null || echo 0)
  # BBB_PROF_PEND=$($REDIS LLEN bbb:profile:pending 2>/dev/null || echo 0)
  BBB_PROF_PEND=0
  EMAIL_PEND=$($REDIS LLEN email:pending 2>/dev/null || echo 0)
  EMAIL_BROW=$($REDIS SCARD email:browser_pool 2>/dev/null || echo 0)
  EMAIL_HTTP_PEND=$($REDIS LLEN email:http:pending 2>/dev/null || echo 0)
  LJ_PEND=$($REDIS SCARD linkedin:jobs:pending 2>/dev/null || echo 0)
  LJ_PROC=$($REDIS SCARD linkedin:jobs:processing 2>/dev/null || echo 0)
  LJ_COMP=$($REDIS SCARD linkedin:companies:pending 2>/dev/null || echo 0)
  LJ_SEARCH_PEND=$($REDIS LLEN linkedin:search:pending 2>/dev/null || echo 0)

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
  NEARBY_7D=$($PSQL -c "SELECT count(*) FROM scraper.gmaps_listings WHERE source_type='nearby_search' AND created_at > now() - interval '7 day'" 2>/dev/null || echo 0)
  PLACES_DAY=0  # no distinct source_type

  # ── BLOG / CLASSIFY ──
  BLOG_PEND=$($REDIS LLEN blog:pending 2>/dev/null || echo 0)
  CLASS_PEND=$($REDIS LLEN classify:pending 2>/dev/null || echo 0)
  CLASS_DONE_24H=$($REDIS GET classify:done_24h 2>/dev/null || echo 0)

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
  # Row 1: Platform headers
  grid5 "SEARCH" "LISTING" "PLACES API" "NEARBY SCAN" "BBB"
  # Row 2: Pending/velocity
  grid5 "pnd $SRCH_PEND  1h↑$SRCH_1H  24h↑$SRCH_24H" "cre $LIST_PROC/h  ph $LIST_PHAN  pool $LIST_POOL" "day $PLACES_DAY  quota $(age "$PLACES_QUOTA")  keys $PLACES_KEYS" "day $NEARBY_DAY  7d $NEARBY_7D  quota $(age "$NEARBY_QUOTA")" "cre $BBB_PROC/h  pnd $BBB_PEND  prof $BBB_PROF_PEND"
  # Row 3: Totals & processing
  grid5 "tot $SRCH_TOTAL  7d $SRCH_7D  proc $SRCH_PROC  fail $SRCH_FAIL  blk $SRCH_BLOCKED" "tot $LIST_TOTAL  7d $LIST_7D  pnd $LIST_PEND  ph $LIST_PHAN" " " "pnd $NEARBY_PEND" "tot $BBB_TOTAL  7d $BBB_7D  24h $BBB_24H"
  hr
  # Row 4: Second platform row
  grid4 "EMAIL" "LINKEDIN JOBS" "LINKEDIN CO" "CLASSIFY/BLOG"
  # Row 5: Email breakdown
  grid4 "http $EMAIL_HTTP_PEND/h  brow $EMAIL_BROW  pnd $EMAIL_PEND" "det $LJ_PROC/h  pnd $LJ_PEND  srch $LJ_SEARCH_PEND" "comp $LJ_COMP  tot $LJC_TOTAL" "pnd $CLASS_PEND  24h✓ $CLASS_DONE_24H"
  # Row 6: Totals & velocity
  grid4 "tot $EMAIL_TOTAL  7d $EMAIL_7D  24h $EMAIL_24H  1h $EMAIL_1H" "tot $LJ_TOTAL  7d $LJ_7D  24h $LJ_24H  1h $LJ_1H" "24h $LJC_24H  1h $LJC_1H" "blog pnd $BLOG_PEND"
  hr
  # Fill rates
  printf '  FILL 1h  phone %s%%  web %s%%  rate %s%%  addr %s%%  cat %s%%  phantom %s  blk %s\n' "$PHONE" "$WEB" "$RATE" "$ADDR" "$CAT" "$PHAN" "$SRCH_BLOCKED"
  hr
  # Velocity sparkline (1h/24h/7d per platform)
  awk -v l1="$LIST_1H" -v l24="$LIST_24H" -v l7="$LIST_7D" \
      -v s1="$SRCH_1H" -v s24="$SRCH_24H" -v s7="$SRCH_7D" \
      -v e1="$EMAIL_1H" -v e24="$EMAIL_24H" -v e7="$EMAIL_7D" \
      -v b1="$BBB_1H" -v b24="$BBB_24H" -v b7="$BBB_7D" \
      -v j1="$LJ_1H" -v j24="$LJ_24H" -v j7="$LJ_7D" \
      -v n1="$NEARBY_DAY" -v n7="$NEARBY_7D" \
    'BEGIN{
      printf "  VEL 1h/24h/7d  lst %s/%s/%s  srch %s/%s/%s  eml %s/%s/%s  bbb %s/%s/%s  job %s/%s/%s  nby %s/%s\n",
        l1, l24, l7, s1, s24, s7, e1, e24, e7, b1, b24, b7, j1, j24, j7, n1, n7
    }'
  hr
  # Totals row
  awk -v ltotal="$LIST_TOTAL" -v stotal="$SRCH_TOTAL" -v etotal="$EMAIL_TOTAL" -v btotal="$BBB_TOTAL" -v ljtotal="$LJ_TOTAL" -v ljc="$LJC_TOTAL" -v ntotal="$NEARBY_7D" \
    'BEGIN{printf "  TOT  lst %.1fM  srch %.1fM  eml %.1fM  bbb %sk  job %sk  co %s  nby %s\n", ltotal/1000000, stotal/1000000, etotal/1000000, btotal/1000, ljtotal/1000, ljc, ntotal/1000}'
  hr
  # Service health strip
  printf '  WATCH  pinchtab %s  8015 %s  8016 %s  5173 %s  5174 %s\n' "$PINCH" "$API" "$PRM" "$WEB" "$ADM"
  # Redis health
  printf '  REDIS  gmaps:fail %s  phantom %s  blocked %s  email:http %s  lnk:search %s\n' "$SRCH_FAIL" "$SRCH_PHAN" "$SRCH_BLOCKED" "$EMAIL_HTTP_PEND" "$LJ_SEARCH_PEND"
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