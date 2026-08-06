#!/usr/bin/env bash
# Dead-code assertion — fails if a previously-removed file reappears.
#
# Each entry below is code that was intentionally deleted during a refactor.
# Adding it back means a regression slipped in. Add new entries to DEAD_FILES
# whenever you delete a module, script, or doc so the playbook's Phase 5c
# check stays meaningful.
#
# Usage (from playbook Phase 5c):
#   bash scripts/assert_dead_code.sh [--update]
#
# Exit codes:
#   0  all dead files are gone
#   1  one or more dead files reappeared (regression detected)
#
# --update  rewrites DEAD_FILES in place from the current repo state, so the
#           next run passes. Use only after intentionally re-introducing a
#           file and deciding to drop the assertion.

set -u

cd "$(dirname "$0")/.."

# Each line: "path|reason". Paths are relative to repo root.
# Keep this list sorted alphabetically by path so diffs are minimal.
DEAD_FILES=(
  "AGENTS.md|migrated to .agents/knowledge-base.md + kilo.json in 2026-08"
  "docs/GMAPS_LISTINGS_SCRAPER.md|consolidated into .agents/knowledge-base.md"
  "docs/GMAPS_SEARCH_SCRAPER.md|consolidated into .agents/knowledge-base.md"
  "scripts/check-stuck-chrome.sh|replaced by scripts/watchdog.sh + systemd watchdog timer"
  "strategies/input/__init__.py|input-strategy layer removed; daemons read URLs from PG/Redis"
)

if [ "${1:-}" = "--update" ]; then
  echo "ERROR: --update mode is a no-op; DEAD_FILES is a static guard list." >&2
  echo "       Edit this file directly to drop an assertion after a deliberate reintroduction." >&2
  exit 2
fi

failures=0
echo "Checking ${#DEAD_FILES[@]} dead-file assertions:"
for entry in "${DEAD_FILES[@]}"; do
  path="${entry%%|*}"
  reason="${entry##*|}"
  if [ -e "$path" ]; then
    echo "  FAIL: $path reappeared (was removed: $reason)" >&2
    failures=$((failures + 1))
  else
    echo "  ok:   $path"
  fi
done

if [ "$failures" -gt 0 ]; then
  echo >&2
  echo "REGRESSION: $failures dead file(s) reappeared." >&2
  exit 1
fi

echo "All dead-file assertions clear."
exit 0
