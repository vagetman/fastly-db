#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${1:?Usage: $0 <service-url>}"
BASE_URL="${BASE_URL%/}"

query() {
  local sql="$1"
  echo "=> $sql"
  curl -sf -X POST "${BASE_URL}/query" -d "$sql"
  echo
}

echo "--- Creating bench_items table ---"
query "CREATE TABLE IF NOT EXISTS bench_items (id INT, name TEXT, value FLOAT)"

echo "--- Seeding 50 rows ---"
for i in $(seq 1 50); do
  query "INSERT INTO bench_items VALUES ($i, 'item-$i', $(echo "$i * 1.5" | bc))"
done

echo "--- Verifying ---"
query "SELECT COUNT(*) AS total FROM bench_items"

echo "Done."
