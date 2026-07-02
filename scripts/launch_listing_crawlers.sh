#!/usr/bin/env bash
# Launch Google Maps listing crawler instances in background.
# Designed for cron no_agent=true mode — fast exit, no blocking.
set -euo

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

# Source env if .env exists
if [ -f .env ]; then
    set -a
    source .env
    set +a
fi

PGHOST="${POSTGRESQL_HOST:-100.92.181.21}"
PGPORT="${POSTGRES_PORT:-5432}"
PGUSER="${POSTGRES_USERNAME:-postgres}"
PGPASSWORD="${POSTGRES_PASSWORD:-changeme}"
PGDB="${POSTGRES_DB:-infinitecrawler}"
INSTANCES=4
LOCK_DIR="/tmp/listing-crawler.lock"

echo "=== Listing Crawler Launch $(date -u '+%Y-%m-%dT%H:%M:%SZ') ==="

# 1. Lock + stale-cleanup — atomic mkdir prevents concurrent launches
#    If lock exists but no processes, clean it (previous crash)
RUNNING=$(pgrep -f "main\.py.*instance-label listing" 2>/dev/null | wc -l)
if [ -d "$LOCK_DIR" ]; then
    if [ "$RUNNING" -lt 4 ]; then
        echo "Stale lock found (running=$RUNNING < 4). Removing."
        rm -rf "$LOCK_DIR"
    else
        echo "Lock $LOCK_DIR exists — crawler already running ($RUNNING instances). Skipping."
        echo "Pending: $(redis-cli LLEN gmaps:pending)"
        echo "Completed: $(redis-cli SCARD gmaps:completed)"
        echo "Processing: $(redis-cli LLEN gmaps:processing)"
        exit 0
    fi
fi
if ! mkdir "$LOCK_DIR" 2>/dev/null; then
    echo "Lock $LOCK_DIR exists — crawler already running ($RUNNING instances). Skipping."
    echo "Pending: $(redis-cli LLEN gmaps:pending)"
    echo "Completed: $(redis-cli SCARD gmaps:completed)"
    echo "Processing: $(redis-cli LLEN gmaps:processing)"
    exit 0
fi
# Lock kept alive by background crawlers — no EXIT cleanup here

# 2. pgrep guard — prevent launch if crawlers already running (no lock fallback)
if [ "$RUNNING" -ge 4 ]; then
    echo "Crawler already running ($RUNNING instances). Skipping."
    echo "Pending: $(redis-cli LLEN gmaps:pending)"
    echo "Completed: $(redis-cli SCARD gmaps:completed)"
    echo "Processing: $(redis-cli LLEN gmaps:processing)"
    exit 0
fi

# 3. Export uncrawled URLs from PG to file
echo "Exporting uncrawled URLs from PostgreSQL..."
PGPASSWORD="$PGPASSWORD" PGCONNECT_TIMEOUT=10 psql -h "$PGHOST" -U "$PGUSER" -d "$PGDB" -t -A \
    -c "SET statement_timeout='60000'; COPY (
        SELECT DISTINCT sr.payload->>'url' AS source_url
        FROM scraper.gmaps_search_results sr
        LEFT JOIN scraper.gmaps_listings gl
          ON gl.source_url = sr.payload->>'url'
        WHERE sr.payload->>'url' IS NOT NULL
          AND gl.source_url IS NULL
        ORDER BY source_url
    ) TO STDOUT WITH CSV;" > input/uncrawled_urls.txt.tmp

# Strip CSV header line and quotes
tail -n +2 input/uncrawled_urls.txt.tmp | sed 's/^"//;s/"$//' > input/uncrawled_urls.txt
rm -f input/uncrawled_urls.txt.tmp

URL_COUNT=$(wc -l < input/uncrawled_urls.txt)
echo "Exported $URL_COUNT uncrawled URLs to input/uncrawled_urls.txt"

if [ "$URL_COUNT" -eq 0 ]; then
    echo "No uncrawled URLs. Nothing to do."
    exit 0
fi

# 3. Clear stale redis queue keys (preserve completed!)
echo "Clearing redis queues..."
redis-cli DEL gmaps:pending gmaps:processing gmaps:failed > /dev/null
echo "Redis queues cleared (completed preserved: $(redis-cli SCARD gmaps:completed) items)"

# 4. Launch crawler instances in background
echo "Launching $INSTANCES crawler instances..."
mkdir -p logs

for i in $(seq 1 $INSTANCES); do
    INSTANCE="listing-${i}"
    LOGFILE="logs/${INSTANCE}.log"
    nohup uv run python main.py \
        --config config/gmaps_listings_working.yaml \
        --instance-label "$INSTANCE" \
        --headless \
        >> "$LOGFILE" 2>&1 &
    echo "  Started $INSTANCE (PID $!) → $LOGFILE"
done

# 5. Brief wait then report status
sleep 3
echo ""
echo "=== Launch Status ==="
echo "Pending: $(redis-cli LLEN gmaps:pending)"
echo "Completed: $(redis-cli SCARD gmaps:completed)"
echo "Processing: $(redis-cli LLEN gmaps:processing)"
echo "Failed: $(redis-cli LLEN gmaps:failed)"
echo "Crawler PIDs: $(pgrep -f 'main.py.*listing' | tr '\n' ' ')"
echo "=== Done ===\n"

# 6. Generate lead exports (background — non-blocking, crawlers already running)
echo "=== Generating Leads (background) ==="
nohup uv run python scripts/generate_leads.py --min-score 0.2 >> logs/leads_export.log 2>&1 &
LEADS_PID=$!
echo "  generate_leads.py started (PID $LEADS_PID)"
# Brief wait to catch immediate failures
sleep 2
if kill -0 "$LEADS_PID" 2>/dev/null; then
    echo "  Lead generation running in background"
else
    echo "  Lead generation exited quickly, check logs/leads_export.log"
fi
echo "=== Launch Complete ==="