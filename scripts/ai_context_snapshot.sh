#!/usr/bin/env bash
set -euo pipefail
OUT="docs/ai/CONTEXT.md"
mkdir -p docs/ai

echo "# AI CONTEXT SNAPSHOT" > "$OUT"
echo "" >> "$OUT"
date +"Generated at: %Y-%m-%d %H:%M %Z" >> "$OUT"
echo "" >> "$OUT"

echo "## Service URLs" >> "$OUT"
echo "- Admin: ${ADMIN_BASE_URL:-unset}" >> "$OUT"
echo "- Consumer: ${CONSUMER_BASE_URL:-unset}" >> "$OUT"
echo "" >> "$OUT"

echo "## Env names (safe)" >> "$OUT"
( set +e; env | cut -d= -f1 | sort | uniq ) >> "$OUT"

echo "" >> "$OUT"
echo "## Repo tree (top 2)" >> "$OUT"
( set +e; command -v tree >/dev/null 2>&1 && tree -L 2 || find . -maxdepth 2 -type d | sort ) >> "$OUT"
