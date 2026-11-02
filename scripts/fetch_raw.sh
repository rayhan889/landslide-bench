#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
[ -f .env ] && { set -a; source .env; set +a; }

RAW=data/raw
CSV=Global_Landslide_Catalog_Export_rows.csv
JSON=Global_Landslide_Catalog_Export_rows.json
mkdir -p "$RAW"

sha() { if command -v shasum >/dev/null; then shasum -a 256 "$@"; else sha256sum "$@"; fi; }

fetch() { # <filename> <url>
  local file="$RAW/$1" url="${2:-}"
  if [ -s "$file" ]; then echo "have $1, skipping download"; return 0; fi
  [ -n "$url" ] || { echo "missing $1 and no URL in .env"; return 1; }
  curl -fL --retry 3 -o "$file" "$url"
}

fetch "$CSV"  "${GLC_CSV_URL:-}"
fetch "$JSON" "${GLC_JSON_URL:-}" || echo "JSON is optional (row-count cross-check only)"

( cd "$RAW" && sha Global_* > SHA256SUMS )
cat "$RAW/SHA256SUMS"
