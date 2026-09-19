#!/usr/bin/env bash
# ic_status.sh — 30-row × 6-col bordered dashboard for InfiniteCrawler
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

# Box-drawing characters
TL="┌" TR="┐" BL="└" BR="┘" H="─" V="│"
TJ="┬" BJ="┴" LJ="├" RJ="┤" CJ="┼"

# Column widths (sum = 138 + borders)
W=(22 24 24 24 22 22)

hr_line() {
  local left="$1" mid="$2" right="$3" sep="$4"
  local line="$left"
  for i in "${!W[@]}"; do
    line+=$(printf '%*s' "${W[i]}" | tr ' ' "$H")
    [[ $i -lt $((${#W[@]}-1)) ]] && line+="$sep" || line+="$right"
  done
  printf '%s\n' "$line"
}
top()    { hr_line "$TL" "$TJ" "$TR" "$TJ"; }
mid()    { hr_line "$LJ" "$CJ" "$RJ" "$CJ"; }
bot()    { hr_line "$BL" "$BJ" "$BR" "$BJ"; }

cell() { printf '%-*s' "$1" "$2"; }
row6() {
  printf "$V"
  cell "${W[0]}" "$1"; printf "$V"
  cell "${W[1]}" "$2"; printf "$V"
  cell "${W[2]}" "$3"; printf "$V"
  cell "${W[3]}" "$4"; printf "$V"
  cell "${W[4]}" "$5"; printf "$V"
  cell "${W[5]}" "$6"; printf "$V\n"
}

age() {
  local ts="$1"
  [[ -z "$ts" || "$ts" = "0" ]] && echo "—" && return
  date -d "@$ts" '+%H:%M' 2>/dev/null || date -r "$ts" '+%H:%M' 2>/dev/null || echo "?"
}

# strip quotes from psql -tA output
clean() { local v="$1"; v="${v#\'}"; v="${v%\'}"; printf '%s' "$v"; }

fmt_num() {
  local n=$(clean "$1")
  if [[ $n -ge 1000000 ]]; then awk -v n="$n" 'BEGIN{printf "%.1fM", n/1000000}'
  elif [[ $n -ge 1000 ]]; then awk -v n="$n" 'BEGIN{printf "%.1fk", n/1000}'
  else printf '%s' "$n"; fi
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
    FROM scraper.gmaps_listings" 2>/dev/null | sed "s/'//g" || echo "0|0|0|0")
  IFS='|' read -r SRCH_TOTAL SRCH_1H SRCH_24H SRCH_7D < <($PSQL -c "
    SELECT count(*), count(*) FILTER (WHERE created_at > now() - interval '1 hour'),
           count(*) FILTER (WHERE created_at > now() - interval '24 hour'),
           count(*) FILTER (WHERE created_at > now() - interval '7 day')
    FROM scraper.gmaps_search_results" 2>/dev/null | sed "s/'//g" || echo "0|0|0|0")
  IFS='|' read -r EMAIL_TOTAL EMAIL_1H EMAIL_24H EMAIL_7D < <($PSQL -c "
    SELECT count(*), count(*) FILTER (WHERE discovered_at > now() - interval '1 hour'),
           count(*) FILTER (WHERE discovered_at > now() - interval '24 hour'),
           count(*) FILTER (WHERE discovered_at > now() - interval '7 day')
    FROM scraper.emails" 2>/dev/null | sed "s/'//g" || echo "0|0|0|0")
  IFS='|' read -r BBB_TOTAL BBB_1H BBB_24H BBB_7D < <($PSQL -c "
    SELECT count(*), count(*) FILTER (WHERE created_at > now() - interval '1 hour'),
           count(*) FILTER (WHERE created_at > now() - interval '24 hour'),
           count(*) FILTER (WHERE created_at > now() - interval '7 day')
    FROM scraper.bbb_listings" 2>/dev/null | sed "s/'//g" || echo "0|0|0|0")
  IFS='|' read -r LJ_TOTAL LJ_1H LJ_24H LJ_7D < <($PSQL -c "
    SELECT count(*), count(*) FILTER (WHERE created_at > now() - interval '1 hour'),
           count(*) FILTER (WHERE created_at > now() - interval '24 hour'),
           count(*) FILTER (WHERE created_at > now() - interval '7 day')
    FROM scraper.linkedin_jobs" 2>/dev/null | sed "s/'//g" || echo "0|0|0|0")
  IFS='|' read -r LJC_TOTAL LJC_1H LJC_24H < <($PSQL -c "
    SELECT count(*), count(*) FILTER (WHERE last_checked_at > now() - interval '1 hour'),
           count(*) FILTER (WHERE last_checked_at > now() - interval '24 hour')
    FROM scraper.linkedin_companies" 2>/dev/null | sed "s/'//g" || echo "0|0|0")

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
    WHERE created_at > now() - interval '1 hour'" 2>/dev/null | sed "s/'//g" || echo "0|0|0|0|0|0")

  # ── NEARBY / PLACES DAILY ──
  NEARBY_DAY=$($PSQL -c "SELECT count(*) FROM scraper.gmaps_listings WHERE source_type='nearby_search' AND created_at > now() - interval '24 hour'" 2>/dev/null | sed "s/'//g" || echo 0)
  NEARBY_7D=$($PSQL -c "SELECT count(*) FROM scraper.gmaps_listings WHERE source_type='nearby_search' AND created_at > now() - interval '7 day'" 2>/dev/null | sed "s/'//g" || echo 0)
  PLACES_DAY=0

  # ── BLOG / CLASSIFY ──
  BLOG_PEND=$($REDIS LLEN blog:pending 2>/dev/null || echo 0)
  CLASS_PEND=$($REDIS LLEN classify:pending 2>/dev/null || echo 0)
  CLASS_DONE_24H=$($REDIS GET classify:done_24h 2>/dev/null || echo 0)

  # ── SERVICE PINGS ──
  API=$(curl -sf -m 2 http://localhost:8015/api/health 2>/dev/null | grep -q '"status":"ok"' && echo "✓" || echo "✗")
  PRM=$(curl -sf -m 2 http://localhost:8016/health 2>/dev/null | grep -q '"status":"ok"' && echo "✓" || echo "✗")
  WEB=$(curl -sf -m 2 http://localhost:5173 2>/dev/null | grep -q '<html' && echo "✓" || echo "✗")
  ADM=$(curl -sf -m 2 http://localhost:5174 2>/dev/null | grep -q '<html' && echo "✓" || echo "✗")
  PINCH=$(curl -sf -m 2 http://localhost:9869/json/version 2>/dev/null | grep -q '"Browser"' && echo "✓" || echo "✗")

  # ── LAST FLUSH ──
  FLUSH=$($REDIS GET ic:last_flush 2>/dev/null | xargs -I{} date -d @{} '+%H:%M:%S' 2>/dev/null || echo "never")
}

render() {
  clear
  printf "  InfiniteCrawler  •  %d/%d daemons  •  %s  •  last flush: %s\n\n" "$UP" "$TOT" "$(date '+%H:%M:%S')" "$FLUSH"

  top
  row6 "METRIC" "SEARCH / LISTING" "PLACES / NEARBY" "BBB / LINKEDIN" "QUEUES / REDIS" "TOTALS / SERVICES"
  mid

  row6 "GMaps SEARCH" "pend $(fmt_num "$SRCH_PEND")  1h↑$(fmt_num "$SRCH_1H")  24h↑$(fmt_num "$SRCH_24H")" "proc $(fmt_num "$SRCH_PROC")  fail $(fmt_num "$SRCH_FAIL")  blk $(fmt_num "$SRCH_BLOCKED")" "tot $(fmt_num "$SRCH_TOTAL")  7d $(fmt_num "$SRCH_7D")" "gmaps:pending" "gmaps:processing"
  row6 "GMaps LISTING" "cre $(fmt_num "$LIST_PROC")/h  phantom $(fmt_num "$LIST_PHAN")  pool $(fmt_num "$LIST_POOL")" "pend $(fmt_num "$LIST_PEND")  1h↑$(fmt_num "$LIST_1H")  24h↑$(fmt_num "$LIST_24H")" "tot $(fmt_num "$LIST_TOTAL")  7d $(fmt_num "$LIST_7D")" "listing:pending" "listing:phantom"
  mid

  row6 "PLACES API" "day $(fmt_num "$PLACES_DAY")  keys $PLACES_KEYS  quota $(age "$PLACES_QUOTA")" "—" "—" "places:quota_reset" "places:active_keys"
  row6 "NEARBY SCAN" "day $(fmt_num "$NEARBY_DAY")  7d $(fmt_num "$NEARBY_7D")  quota $(age "$NEARBY_QUOTA")" "pend $(fmt_num "$NEARBY_PEND")" "tot 7d $(fmt_num "$NEARBY_7D")" "nearby:pending" "nearby:quota"
  mid

  row6 "BBB" "cre $(fmt_num "$BBB_PROC")/h  pend $(fmt_num "$BBB_PEND")" "tot $(fmt_num "$BBB_TOTAL")  7d $(fmt_num "$BBB_7D")  24h $(fmt_num "$BBB_24H")" "1h $(fmt_num "$BBB_1H")" "bbb:pending" "bbb:processing"
  mid

  row6 "EMAIL" "http $(fmt_num "$EMAIL_HTTP_PEND")/h  brow $(fmt_num "$EMAIL_BROW")  pend $(fmt_num "$EMAIL_PEND")" "tot $(fmt_num "$EMAIL_TOTAL")  7d $(fmt_num "$EMAIL_7D")  24h $(fmt_num "$EMAIL_24H")" "1h $(fmt_num "$EMAIL_1H")" "email:pending" "email:http:pending"
  mid

  row6 "LINKEDIN JOBS" "det $(fmt_num "$LJ_PROC")/h  pend $(fmt_num "$LJ_PEND")  srch $(fmt_num "$LJ_SEARCH_PEND")" "tot $(fmt_num "$LJ_TOTAL")  7d $(fmt_num "$LJ_7D")  24h $(fmt_num "$LJ_24H")" "1h $(fmt_num "$LJ_1H")" "jobs:pending" "jobs:processing"
  row6 "LINKEDIN CO" "comp $(fmt_num "$LJ_COMP")  24h $(fmt_num "$LJC_24H")  1h $(fmt_num "$LJC_1H")" "tot $(fmt_num "$LJC_TOTAL")" "—" "companies:pending" "—"
  mid

  row6 "CLASSIFY" "pend $(fmt_num "$CLASS_PEND")  24h✓ $(fmt_num "$CLASS_DONE_24H")" "—" "—" "classify:pending" "classify:done_24h"
  row6 "BLOG" "pend $(fmt_num "$BLOG_PEND")" "—" "—" "blog:pending" "—"
  mid

  row6 "FILL 1h" "phone ${PHONE}%  web ${WEB}%  rate ${RATE}%" "addr ${ADDR}%  cat ${CAT}%  phantom ${PHAN}" "blocked ${SRCH_BLOCKED}" "—" "gmaps_listings 1h window"
  mid

  row6 "VEL 1h" "$(fmt_num "$LIST_1H")  $(fmt_num "$SRCH_1H")  $(fmt_num "$EMAIL_1H")  $(fmt_num "$BBB_1H")  $(fmt_num "$LJ_1H")  $(fmt_num "$NEARBY_DAY")" "—" "—" "—" "per-platform 1h"
  row6 "VEL 24h" "$(fmt_num "$LIST_24H")  $(fmt_num "$SRCH_24H")  $(fmt_num "$EMAIL_24H")  $(fmt_num "$BBB_24H")  $(fmt_num "$LJ_24H")  $(fmt_num "$NEARBY_DAY")" "—" "—" "—" "per-platform 24h"
  row6 "VEL 7d" "$(fmt_num "$LIST_7D")  $(fmt_num "$SRCH_7D")  $(fmt_num "$EMAIL_7D")  $(fmt_num "$BBB_7D")  $(fmt_num "$LJ_7D")  $(fmt_num "$NEARBY_7D")" "—" "—" "—" "per-platform 7d"
  mid

  row6 "TOTALS" "lst $(fmt_num "$LIST_TOTAL")  srch $(fmt_num "$SRCH_TOTAL")" "eml $(fmt_num "$EMAIL_TOTAL")  bbb $(fmt_num "$BBB_TOTAL")" "job $(fmt_num "$LJ_TOTAL")  co $(fmt_num "$LJC_TOTAL")  nby $(fmt_num "$NEARBY_7D")" "—" "all-time rows"
  mid

  row6 "SERVICES" "pinchtab $PINCH  api:8015 $API" "premium:8016 $PRM  web:5173 $WEB" "admin:5174 $ADM" "—" "HTTP health checks"
  row6 "REDIS" "gmaps:fail $(fmt_num "$SRCH_FAIL")  phantom $(fmt_num "$SRCH_PHAN")" "blocked $(fmt_num "$SRCH_BLOCKED")  email:http $(fmt_num "$EMAIL_HTTP_PEND")" "lnk:search $(fmt_num "$LJ_SEARCH_PEND")" "—" "queue health"
  bot

  printf "\n  Legend: pend=pending  proc=processing  cre=creations/h  det=detail/h  comp=companies  ph=phantom  blk=blocked  srch=search  7d=7-day  1h/24h=window\n"
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