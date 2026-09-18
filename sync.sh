#!/usr/bin/env bash
# sync.sh — uv sync + post-install nodriver patch
# Usage: ./sync.sh   (replaces bare `uv sync`)
set -euo pipefail

cd "$(dirname "$0")"
uv sync "$@"
uv run python scripts/patch_nodriver.py
