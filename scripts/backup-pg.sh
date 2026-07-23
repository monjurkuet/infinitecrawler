#!/bin/bash
# InfiniteCrawler PostgreSQL backup — daily dump + compress + rotate
set -euo pipefail

BACKUP_DIR="/root/codebase/vhd/infinitecrawler/backups"
RETENTION_DAYS=14
TS=$(date +%Y%m%d_%H%M%S)
PG_HOST="${PG_HOST:-127.0.0.1}"
PG_PORT="${PG_PORT:-5432}"
PG_USER="${PG_USER:-postgres}"
PG_PASSWORD="${PG_PASSWORD:-changeme}"
PG_DB="${PG_DB:-infinitecrawler}"
PG_DUMP="/usr/lib/postgresql/15/bin/pg_dump"

mkdir -p "$BACKUP_DIR"

# Custom-format dump (compressed, parallel-restore friendly)
PGPASSWORD="$PG_PASSWORD" "$PG_DUMP" \
  -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" \
  --format=custom --no-owner --no-acl \
  -f "$BACKUP_DIR/ic_pg_$TS.dump"

# Compress with zstd (better ratio/speed than gzip)
zstd -f --rm "$BACKUP_DIR/ic_pg_$TS.dump" -o "$BACKUP_DIR/ic_pg_$TS.dump.zst"

# Readable SQL snapshot for quick inspection
PGPASSWORD="$PG_PASSWORD" "$PG_DUMP" \
  -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" \
  --no-owner --no-acl \
  | gzip > "$BACKUP_DIR/ic_pg_${TS}.sql.gz"

# Prune old backups
find "$BACKUP_DIR" -name 'ic_pg_*.dump.zst' -mtime +$RETENTION_DAYS -delete
find "$BACKUP_DIR" -name 'ic_pg_*.sql.gz' -mtime +$RETENTION_DAYS -delete

# Manifest
SIZE=$(stat -c%s "$BACKUP_DIR/ic_pg_${TS}.dump.zst" 2>/dev/null || echo 0)
echo "$TS|ic_pg_${TS}.dump.zst|$SIZE|$PG_DB" >> "$BACKUP_DIR/MANIFEST.txt"

echo "✓ PG backup: ic_pg_${TS}.dump.zst ($(du -h "$BACKUP_DIR/ic_pg_${TS}.dump.zst" | cut -f1))"
