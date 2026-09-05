#!/usr/bin/env bash
# ic_status.sh — live status board for the InfiniteCrawler pipeline.
# Usage:  ic_status              one-shot snapshot
#         ic_status --watch [s]  refresh every N seconds (default 30)
#
# Sections: LIVE NOW -> ACTIVITY (1h/24h) -> DATA QUALITY -> BACKLOGS
#           -> EMAIL TREND -> TOTALS -> DASHBOARDS.
set -uo pipefail

PSQL="psql -h /var/run/postgresql -U postgres -d infinitecrawler -tA -F|"
export PGPASSWORD="${PGPASSWORD:-changeme}"
LOGDIR=/var/log/infinitecrawler

# ---------- palette ----------
if [ -t 1 ]; then
  B=$'\e[1m'; DIM=$'\e[2m'; X=$'\e[0m'
  GR=$'\e[32m'; YL=$'\e[33m'; RD=$'\e[31m'; CY=$'\e[36m'
  OK="${GR}●${X}"; WARN="${YL}●${X}"; BAD="${RD}●${X}"; NA="${DIM}·${X}"
else B=; DIM=; X=; GR=; YL=; RD=; CY=; OK="[ok]"; WARN="[warn]"; BAD="[BAD]"; NA="-"; fi

q() { $PSQL -c "$1" 2>/dev/null; }   # psql wrapper (tuples, | sep)

sec() {  # section header with rule
  local t="$1"
  local pad=$(( 76 - ${#t} ))
  [ "$pad" -lt 2 ] && pad=2
  local rule; rule=$(printf '%.0s─' $(seq "$pad"))
  printf '%s─ %s %s%s\n' "$CY$B" "$t" "$rule" "$X"
}
row() {  printf '  %s %-38s %10s%s\n' "$1" "$2" "$3" "$X"; }
row2() { printf '  %s %-26s %14s %10s %s\n' "$1" "$2" "$3" "$4" "$X"; }

# gauge: num $1 warn_if_below $2 -> colored number
num() {
  local v="${1:-0}" lo="${2:-1}"
  if [ "$v" -ge "$lo" ] 2>/dev/null; then echo "${GR}$v${X}"; else echo "${WARN}$v${X}"; fi
}

spark() {  # sparkline from space-separated ints
  awk 'BEGIN{
    split("▁ ▂ ▃ ▄ ▅ ▆ ▇ █",b," "); n=split(ARGV[1],a," ");
    mx=1; for(i=1;i<=n;i++) if(a[i]>mx) mx=a[i];
    for(i=1;i<=n;i++){ l=int(a[i]/mx*7+0.999); if(l<1)l=1; printf "%s",b[l]; } print ""
  }' "$1"
}

render() {
  clear 2>/dev/null || true
  NOW=$(date '+%F %T %Z')
  printf '%s%s  INFINITECRAWLER %s %s\n' "$B" "$CY" "$NOW" "$X"
  printf '%s%s\n' "$DIM" "  ────────────────────────────────────────────────────────────────────────$X"

  # ============================ 1. LIVE NOW ============================
  sec "LIVE NOW"
  ACT=$(systemctl --user list-units 'infinitecrawler-*.service' --no-legend 2>/dev/null | grep -c ' active ' || true)
  TOT=$(systemctl --user list-units 'infinitecrawler-*.service' --no-legend 2>/dev/null | wc -l)
  PEND=$(redis-cli LLEN gmaps:pending 2>/dev/null || echo 0)
  PROC=$(redis-cli LLEN gmaps:processing 2>/dev/null || echo 0)
  PHAN=$(redis-cli LLEN gmaps:phantom 2>/dev/null || echo 0)
  COMP=$(redis-cli SCARD gmaps:completed 2>/dev/null || echo 0)

  [ "$ACT" = "$TOT" ] && DS=$(echo "$OK") || DS=$(echo "$BAD")
  row "$DS" "daemons running" "$ACT/$TOT"
  [ "$PEND" -ge 50 ] && QS="$OK" || QS="$WARN"
  row "$QS" "queue  pending / processing" "$(num "$PEND" 50) / $PROC"
  [ "$PHAN" -le 50 ] && PS="$OK" || PS="$WARN"
  row "$PS" "phantom queue (bare shells)" "$PHAN"
  row "$NA" "completed lifetime" "$COMP"

  echo "  ${DIM}extracting right now:${X}"
  mapfile -t CUR < <(redis-cli LRANGE gmaps:processing 0 -1 2>/dev/null | head -4)
  if [ ${#CUR[@]} -eq 0 ]; then
    echo "    ${DIM}(workers between items)${X}"
  else
    for u in "${CUR[@]}"; do
      short=$(echo "$u" | sed -E 's|.*maps/place/||; s|/data.*||; s|^.{0,34}$|&|; s|^(.{34}).*|\1…|')
      [ -z "$short" ] && short="?cid=…"
      printf '    %s↳%s %s\n' "$CY" "$X" "$short"
    done
  fi
  FLUSH=$(strings "$LOGDIR/infinitecrawler-listing.log" 2>/dev/null | grep 'listing.flush' | tail -1 | awk '{print $1, $2}')
  [ -n "$FLUSH" ] && echo "    ${DIM}last flush: $FLUSH${X}"

  # ===================== 2. ACTIVITY (1h vs 24h) =======================
  sec "ACTIVITY"
  # If the DB was recently restored/bootstrapped, 1h and 24h coincide — annotate it.
  DB_AGE_H=$(q "SELECT GREATEST(0, EXTRACT(EPOCH FROM now()-min(created_at))/3600)::int FROM scraper.gmaps_listings" )
  AGE_NOTE=""
  if [ "${DB_AGE_H:-99}" -lt 24 ]; then AGE_NOTE="  ${DIM}(DB ${DB_AGE_H}h old — 1h and 24h overlap until it ages past 24h)${X}"; fi
  printf '  %s %-32s %14s %10s%s%b\n' " " "pipeline" "1h" "24h" "$X" "$AGE_NOTE"
  printf '  %s\n' "${DIM}    ────────────────────────────────────────────────────────${X}"
  mapfile -t A < <(q "
    SELECT 'search seeds',  (SELECT count(*) FROM scraper.gmaps_search_results WHERE created_at>=now()-interval '1 hour'),
                            (SELECT count(*) FROM scraper.gmaps_search_results WHERE created_at>=now()-interval '24 hours')
    UNION ALL SELECT 'listings created (browser)',
                            (SELECT count(*) FROM scraper.gmaps_listings WHERE source_type='gmaps_listing' AND created_at>=now()-interval '1 hour'),
                            (SELECT count(*) FROM scraper.gmaps_listings WHERE source_type='gmaps_listing' AND created_at>=now()-interval '24 hours')
    UNION ALL SELECT 'listings updated (browser)',
                            (SELECT count(*) FROM scraper.gmaps_listings WHERE source_type='gmaps_listing' AND updated_at>=now()-interval '1 hour'),
                            (SELECT count(*) FROM scraper.gmaps_listings WHERE source_type='gmaps_listing' AND updated_at>=now()-interval '24 hours')
    UNION ALL SELECT 'emails found (http)',
                            (SELECT count(*) FROM scraper.emails WHERE extraction_method='http' AND discovered_at>=now()-interval '1 hour'),
                            (SELECT count(*) FROM scraper.emails WHERE extraction_method='http' AND discovered_at>=now()-interval '24 hours')
    UNION ALL SELECT 'emails found (browser)',
                            (SELECT count(*) FROM scraper.emails WHERE extraction_method='browser' AND discovered_at>=now()-interval '1 hour'),
                            (SELECT count(*) FROM scraper.emails WHERE extraction_method='browser' AND discovered_at>=now()-interval '24 hours')
    UNION ALL SELECT 'linkedin profiles checked',
                            (SELECT count(*) FROM scraper.linkedin_profiles WHERE checked_at>=now()-interval '1 hour'),
                            (SELECT count(*) FROM scraper.linkedin_profiles WHERE checked_at>=now()-interval '24 hours')
    UNION ALL SELECT 'nearby grid cells',
                            (SELECT count(*) FROM scraper.nearby_scan_grid WHERE scanned_at>=now()-interval '1 hour'),
                            (SELECT count(*) FROM scraper.nearby_scan_grid WHERE scanned_at>=now()-interval '24 hours')
  ")
  declare -A H1 H24
  for l in "${A[@]}"; do IFS='|' read -r k v1 v24 <<< "$l"; H1[$k]=$v1; H24[$k]=$v24; done
  for k in "search seeds" "listings created (browser)" "listings updated (browser)" \
           "emails found (http)" "emails found (browser)" "linkedin profiles checked" "nearby grid cells"; do
    v1=${H1[$k]:-0}; v24=${H24[$k]:-0}
    st="$OK"; [ "$v1" -eq 0 ] && [ "$v24" -eq 0 ] && st="$DIM$NA (quota)${X}"
    row2 "$st" "$k" "$(num "$v1")" "${GR}$v24${X}"
  done

  # ===================== 3. DATA QUALITY (24h browser rows) ============
  sec "DATA QUALITY — gmaps_listing last 24h"
  printf '  %s %-32s %7s %9s   %s%s\n' " " "field" "fill" "baseline" "status" "$X"
  printf '  %s\n' "${DIM}    ───────────────────────────────────────────${X}"
  mapfile -t DQ < <(q "
    SELECT 'phone',   round(100.0*count(phone)  /nullif(count(*),0)) FROM scraper.gmaps_listings WHERE source_type='gmaps_listing' AND created_at>=now()-interval '24 hours'
    UNION ALL SELECT 'website', round(100.0*count(website)/nullif(count(*),0)) FROM scraper.gmaps_listings WHERE source_type='gmaps_listing' AND created_at>=now()-interval '24 hours'
    UNION ALL SELECT 'rating',  round(100.0*count(rating) /nullif(count(*),0)) FROM scraper.gmaps_listings WHERE source_type='gmaps_listing' AND created_at>=now()-interval '24 hours'
    UNION ALL SELECT 'address', round(100.0*count(address)/nullif(count(*),0)) FROM scraper.gmaps_listings WHERE source_type='gmaps_listing' AND created_at>=now()-interval '24 hours'
    UNION ALL SELECT 'category',round(100.0*count(category)/nullif(count(*),0)) FROM scraper.gmaps_listings WHERE source_type='gmaps_listing' AND created_at>=now()-interval '24 hours'
  ")
  for l in "${DQ[@]}"; do
    IFS='|' read -r f pct <<< "$l"
    case $f in phone) base=75; th=70;; website) base=31; th=25;; rating) base=88; th=70;;
               address) base=98; th=90;; category) base=93; th=85;; *) base='-'; th=0;; esac
    [ "$pct" -ge "$th" ] && st="$OK" || st="$BAD"
    printf '  %s %-32s %7s%% %9s%%   %s\n' "$st" "$f" "$pct" "$base" "$X"
  done
  PH1H=$(q "SELECT count(*) FROM scraper.gmaps_listings WHERE source_type='gmaps_listing'
             AND created_at>=now()-interval '1 hour' AND phone IS NULL AND website IS NULL
             AND rating IS NULL AND address IS NULL")
  [ "${PH1H:-1}" = "0" ] && pst="$OK" || pst="$BAD"
  row "$pst" "phantom rows (bare shells) last 1h" "$PH1H"

  # ===================== 4. BACKLOGS / REMAINING =======================
  sec "BACKLOGS — remaining work"
  UNC=$(q "SELECT count(*) FROM scraper.gmaps_search_results s
            WHERE NOT EXISTS (SELECT 1 FROM scraper.gmaps_listings l WHERE l.source_url = s.key_value)")
  EB=$(q "SELECT count(*) FROM scraper.gmaps_listings WHERE website IS NOT NULL AND email_scanned_at IS NULL")
  UC=$(q "SELECT count(*) FROM scraper.gmaps_listings WHERE classified_at IS NULL AND source_type='gmaps_listing'")
  NPEND=$(q "SELECT count(*) FROM scraper.nearby_scan_grid WHERE status='pending'")
  NTOT=$(q "SELECT count(*) FROM scraper.nearby_scan_grid")
  SEEDS1=${H1["search seeds"]:-0}; LC1=${H1["listings created (browser)"]:-0}
  NET=$(( SEEDS1 - LC1 ))
  if   [ "$NET" -le 0 ]; then dir="${GR}▼ shrinking ${X}"
  elif [ "$NET" -lt 100 ]; then dir="${YL}▲ +${NET}/h${X}"
  else dir="${YL}▲ growing +${NET}/h (seeds outpace browser)${X}"; fi
  npct=$(awk -v a=${NPEND:-1} -v t=${NTOT:-1} 'BEGIN{printf "%.1f", 100*(t-a)/t}')
  printf '  %-40s %12s  %s\n' "seeds never deep-extracted" "$UNC" "$dir"
  printf '  %-40s %12s  %s\n' "websites awaiting email scan" "$EB" "${DIM}drains via email loop${X}"
  printf '  %-40s %12s  %s\n' "unclassified (gmaps_listing)" "$UC" "${DIM}nightly classify${X}"
  printf '  %-40s %12s  %s\n' "nearby grid pending" "$NPEND" "${DIM}${npct}% scanned, quota-paced${X}"

  # ===================== 5. EMAIL TREND ================================
  sec "EMAILS PER HOUR — last 8h"
  mapfile -t EH < <(q "SELECT to_char(h,'HH24')||':'||count(*)
    FROM (SELECT date_trunc('hour',discovered_at) h FROM scraper.emails
          WHERE discovered_at>=now()-interval '8 hours') t
    GROUP BY h ORDER BY h")
  vals=""; labels=""
  for e in "${EH[@]:-}"; do [ -z "$e" ] && continue; labels+="${e%%:*}   "; vals+="${e##*:} "; done
  if [ -n "$vals" ]; then
    echo "    $(spark "$vals")  ${DIM}(height ∝ emails/h)${X}"
    echo "    ${DIM}${labels}${X}"
  fi
  BR24=${H24["emails found (browser)"]:-0}
  echo "    browser-method 24h: ${GR}$BR24${X} ${DIM}(contact-page walk fix — was 0 before 2026-09-03)${X}"

  # ===================== 6. ALL-TIME TOTALS ============================
  sec "TOTALS — all time"
  mapfile -t T < <(q "
    SELECT 'listings', count(*) FROM scraper.gmaps_listings
    UNION ALL SELECT 'search results', count(*) FROM scraper.gmaps_search_results
    UNION ALL SELECT 'emails', count(*) FROM scraper.emails
    UNION ALL SELECT 'emails distinct', count(DISTINCT email) FROM scraper.emails
    UNION ALL SELECT 'listings with >=1 email', count(DISTINCT listing_id) FROM scraper.emails
    UNION ALL SELECT 'linkedin profiles', count(*) FROM scraper.linkedin_profiles
    UNION ALL SELECT 'linkedin companies', count(*) FROM scraper.linkedin_companies
  ")
  for l in "${T[@]}"; do
    IFS='|' read -r k v <<< "$l"
    printf '  %-34s %'"'"'s\n' "$k" "$(printf '%s' "$v" | sed ':a;s/\B[0-9]\{3\}\>/,&/;ta')"
  done

  # ===================== 7. DASHBOARDS =================================
  sec "DASHBOARDS & SERVICES"
  chk() { curl -s -m3 "$1" >/dev/null 2>&1 && echo "$OK" || echo "$BAD"; }
  printf '  %s  %-26s %s  %s\n' "$(chk http://127.0.0.1:8015/api/health)" "api        :8015" "${GR}ok${X}" "${DIM}internal${X}" 2>/dev/null
  printf '  %s  %-26s %s  %s\n' "$(chk http://127.0.0.1:8016/health)"  "premium-api :8016" "${GR}ok${X}" "${DIM}JWT paywall${X}"
  printf '  %s  %-26s %s  %s\n' "$(chk http://[::1]:5173/)"             "web        :5173" "${GR}ok${X}" "${DIM}premium SPA${X}"
  PC=$(curl -s -m3 -o /dev/null -w '%{http_code}' http://127.0.0.1:9868/health 2>/dev/null)
  if [ "$PC" = "401" ]; then pe="$OK"; ps="401 ${DIM}(token-gated = up)${X}"
  elif [ "$PC" = "200" ]; then pe="$OK"; ps="200"
  else pe="$BAD"; ps="$PC"; fi
  printf '  %s  %-26s %s\n' "$pe" "pinchtab   :9868" "$ps"
  DISK=$(df -h / | awk 'NR==2{print $4}')
  MEM=$(free -h | awk '/^Mem:/{print $3" / "$2}')
  printf '  %s  disk free %-10s mem %s\n' "$DIM" "$DISK" "$MEM${X}"
  echo
}

if [ "${1:-}" = "--watch" ] || [ "${1:-}" = "-w" ]; then
  SECS="${2:-30}"
  trap 'tput cnorm 2>/dev/null; exit 0' INT TERM
  tput civis 2>/dev/null || true
  while true; do render; sleep "$SECS"; done
else
  render
fi
