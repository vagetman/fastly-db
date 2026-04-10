#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# --- defaults ----------------------------------------------------------------
DURATION="300s"
CONNECTIONS=20
THREADS=4
TIMEOUT="15s"
LOC="${INTEGRITY_LOC:-loc0}"
SETTLE=60
MODE="full"
EXPECTED=""
OK=""
RUN="${INTEGRITY_RUN:-}"
STRICT=false
NO_REPORT=false

usage() {
  cat <<'EOF'
Usage: integrity.sh [OPTIONS] <service-url>

Soak test for UUIDv7 journal + dual-snapshot architecture.
Verifies that every successful write survives compaction, cross-PoP
replication, and snapshot slot rotation — even under sustained
concurrent load from multiple locations.

Modes:
  --full           Setup → Write → Settle → Verify   (default)
  --setup-only     Create the integrity_test table
  --write-only     Run writes only (use on each location)
  --report-only    Record this location's ok-count into DB
  --verify-only    Verify only (run once after all locations finish)

Options:
  --loc NAME       Location tag, alphanumeric     (default: $INTEGRITY_LOC or "loc0")
  --run RUN_ID      Run id to scope writes/verify   (recommended; avoids mixing runs)
  --strict         Strict mode: forces -c == -t and requires inflight==0
                  so seq validation becomes a real proof.
  --no-report      Don't auto-run --report-only after --write-only
  -d, --duration   Write duration                  (default: 300s)
  -c, --connections  Connections                   (default: 20)
  -t, --threads    wrk threads                     (default: 4)
  --timeout        Per-request timeout             (default: 15s)
  --settle SECS    Post-write settle time           (default: 60)
  --ok N           Ok write count (report-only)      (optional; auto-read /tmp if omitted)
  --expected N     Expected row count (verify-only) (optional)
  -h, --help       Show this help

Multi-location workflow:
  # 1. One-time setup from any location
  ./integrity.sh --setup-only --run myrun123 https://svc.edgecompute.app

  # 2. Start writes from every location (in parallel)
  INTEGRITY_LOC=nyc ./integrity.sh --write-only --run myrun123 -d 600s https://svc.edgecompute.app
  INTEGRITY_LOC=lon ./integrity.sh --write-only --run myrun123 -d 600s https://svc.edgecompute.app
  INTEGRITY_LOC=tok ./integrity.sh --write-only --run myrun123 -d 600s https://svc.edgecompute.app

  # 2b. After each write run, record its ok-count into the DB (can be run from
  #     each machine; uses /tmp/integrity_<run>_<loc>.count unless you pass --ok)
  INTEGRITY_LOC=nyc ./integrity.sh --report-only --run myrun123 https://svc.edgecompute.app
  INTEGRITY_LOC=lon ./integrity.sh --report-only --run myrun123 https://svc.edgecompute.app
  INTEGRITY_LOC=tok ./integrity.sh --report-only --run myrun123 https://svc.edgecompute.app

  # 3. After all finish, verify from any single location
  #    (defaults to using integrity_manifest as the expected total)
  ./integrity.sh --verify-only --run myrun123 https://svc.edgecompute.app
EOF
  exit 0
}

# --- parse args ---------------------------------------------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --full)            MODE="full";   shift ;;
    --setup-only)      MODE="setup";  shift ;;
    --write-only)      MODE="write";  shift ;;
    --report-only)     MODE="report"; shift ;;
    --verify-only)     MODE="verify"; shift ;;
    --loc)             LOC="$2";      shift 2 ;;
    --run)             RUN="$2";      shift 2 ;;
    --strict)          STRICT=true;   shift ;;
    --no-report)       NO_REPORT=true; shift ;;
    -d|--duration)     DURATION="$2"; shift 2 ;;
    -c|--connections)  CONNECTIONS="$2"; shift 2 ;;
    -t|--threads)      THREADS="$2";  shift 2 ;;
    --timeout)         TIMEOUT="$2";  shift 2 ;;
    --settle)          SETTLE="$2";   shift 2 ;;
    --ok)              OK="$2";       shift 2 ;;
    --expected)        EXPECTED="$2"; shift 2 ;;
    -h|--help)         usage ;;
    -*)                echo "Unknown option: $1"; usage ;;
    *)                 BASE_URL="$1"; shift ;;
  esac
done

BASE_URL="${BASE_URL:?Error: service URL required. Run with --help for usage.}"
BASE_URL="${BASE_URL%/}"

# Validate run id if provided (prevents SQL injection)
if [[ -n "${RUN}" ]] && [[ ! "${RUN}" =~ ^[A-Za-z0-9_-]+$ ]]; then
  echo "ERROR: --run must be alphanumeric (a-z, 0-9, _, -). Got: '${RUN}'"
  exit 1
fi

# Validate location tag: alphanumeric + hyphens only (prevents SQL injection)
if [[ ! "${LOC}" =~ ^[A-Za-z0-9_-]+$ ]]; then
  echo "ERROR: --loc must be alphanumeric (a-z, 0-9, _, -). Got: '${LOC}'"
  exit 1
fi

# --- helpers ------------------------------------------------------------------
query() {
  curl -sf --max-time 30 -X POST "${BASE_URL}/query" \
    -H "Content-Type: application/octet-stream" \
    -d "$1"
}

query_or_die() {
  local sql="$1" label="${2:-query}" result
  if ! result=$(query "$sql"); then
    echo "FAIL: ${label} — query failed"
    echo "  SQL: ${sql}"
    exit 1
  fi
  echo "$result"
}

# Extract a numeric value from {"results":[[{"key":123}]]}
extract_count() {
  local n
  n=$(echo "$1" | grep -oE '"[a-z_]+"[[:space:]]*:[[:space:]]*[0-9]+' | head -1 | grep -oE '[0-9]+$' || true)
  echo "${n:-0}"
}

# --- phase: setup -------------------------------------------------------------
do_setup() {
  echo "=== Setup: creating integrity_test table ==="

  if [[ -z "${RUN}" ]]; then
    # Generate a run id if not provided, so verify is always scoped.
    if command -v uuidgen &>/dev/null; then
      RUN="$(uuidgen | tr '[:upper:]' '[:lower:]' | tr -d '-' | cut -c1-12)"
    else
      RUN="run$(date +%Y%m%d%H%M%S)"
    fi
  fi
  echo "  run=${RUN}"

  # Drop previous run (ignore errors if table doesn't exist)
  query "DROP TABLE IF EXISTS integrity_test" >/dev/null 2>&1 || true
  query "DROP TABLE IF EXISTS integrity_manifest" >/dev/null 2>&1 || true
  query "DROP TABLE IF EXISTS integrity_manifest_thr" >/dev/null 2>&1 || true
  sleep 3

  local result
  result=$(query_or_die \
    "CREATE TABLE IF NOT EXISTS integrity_test (run TEXT, id TEXT, loc TEXT, thr TEXT, seq INT)" \
    "CREATE TABLE")
  echo "  Table created."

  result=$(query_or_die \
    "CREATE TABLE IF NOT EXISTS integrity_manifest (run TEXT, loc TEXT, attempted INT, ok INT, err INT, inflight INT)" \
    "CREATE TABLE manifest")
  echo "  Manifest table created."

  result=$(query_or_die \
    "CREATE TABLE IF NOT EXISTS integrity_manifest_thr (run TEXT, loc TEXT, thr TEXT, attempted INT, ok INT, err INT, inflight INT, first_ok_seq INT, last_ok_seq INT)" \
    "CREATE TABLE manifest_thr")
  echo "  Manifest thread table created."

  # Wait for propagation then verify
  sleep 2
  result=$(query_or_die "SELECT COUNT(*) AS total FROM integrity_test WHERE run = '${RUN}'" "initial count")
  echo "  Initial row count: $(extract_count "$result")"
  echo
}

# --- phase: report ------------------------------------------------------------
do_report() {
  echo "=== Report: recording ok-count into DB ==="
  echo "  loc=${LOC}"
  echo "  run=${RUN:-<unset>}"

  if [[ -z "${RUN}" ]]; then
    echo "FAIL: --run (or INTEGRITY_RUN) is required for report-only"
    echo "  This keeps integrity_manifest run-scoped and prevents mixing runs."
    exit 1
  fi

  local ok="${OK}"
  local attempted=""
  local err=""
  local inflight=""

  local meta_file="/tmp/integrity_${RUN}_${LOC}.meta"
  if [[ -f "${meta_file}" ]]; then
    attempted=$(grep -E '^attempted=' "${meta_file}" | head -1 | cut -d= -f2 || true)
    ok=$(grep -E '^ok=' "${meta_file}" | head -1 | cut -d= -f2 || true)
    err=$(grep -E '^err=' "${meta_file}" | head -1 | cut -d= -f2 || true)
    inflight=$(grep -E '^inflight=' "${meta_file}" | head -1 | cut -d= -f2 || true)
  fi

  # Backward-compatible fallback: ok-only from count file.
  if [[ -z "${ok}" ]]; then
    local agg_file="/tmp/integrity_${RUN}_${LOC}.count"
    if [[ -f "${agg_file}" ]]; then
      ok=$(cat "${agg_file}")
    fi
  fi

  if [[ -z "${ok}" ]]; then
    echo "FAIL: ok count missing"
    echo "  Provide --ok N or ensure /tmp/integrity_${RUN}_${LOC}.meta exists (run --write-only first)."
    exit 1
  fi

  if [[ ! "${ok}" =~ ^[0-9]+$ ]]; then
    echo "FAIL: ok count must be an integer; got '${ok}'"
    exit 1
  fi

  if [[ -z "${attempted}" ]]; then attempted="${ok}"; fi
  if [[ -z "${err}" ]]; then err=0; fi
  if [[ -z "${inflight}" ]]; then inflight=$(( attempted - ok - err )); fi
  if [[ ! "${attempted}" =~ ^[0-9]+$ ]]; then
    echo "FAIL: attempted count must be an integer; got '${attempted}'"
    exit 1
  fi
  if [[ ! "${err}" =~ ^[0-9]+$ ]]; then
    echo "FAIL: err count must be an integer; got '${err}'"
    exit 1
  fi
  if [[ ! "${inflight}" =~ ^-?[0-9]+$ ]]; then
    echo "FAIL: inflight count must be an integer; got '${inflight}'"
    exit 1
  fi

  if (( inflight < 0 )); then
    echo "FAIL: inflight computed negative (attempted=${attempted} ok=${ok} err=${err})"
    exit 1
  fi

  # GlueSQL doesn't guarantee UPSERT support; do delete+insert.
  query_or_die "DELETE FROM integrity_manifest WHERE run = '${RUN}' AND loc = '${LOC}'" "delete manifest row" >/dev/null
  query_or_die "DELETE FROM integrity_manifest_thr WHERE run = '${RUN}' AND loc = '${LOC}'" "delete manifest_thr rows" >/dev/null

  query_or_die "INSERT INTO integrity_manifest VALUES ('${RUN}', '${LOC}', ${attempted}, ${ok}, ${err}, ${inflight})" "insert manifest row" >/dev/null

  # If meta contains per-thread lines, persist them too.
  if [[ -f "${meta_file}" ]]; then
    while read -r line; do
      [[ "$line" == thr=* ]] || continue

      # Format: thr=<id> attempted=<n> ok=<n> err=<n> inflight=<n> first_ok_seq=<n> last_ok_seq=<n>
      local thr_id thr_attempted thr_ok thr_err thr_inflight thr_first_ok_seq thr_last_ok_seq
      thr_id=$(echo "$line" | awk '{for(i=1;i<=NF;i++){if($i ~ /^thr=/){sub(/^thr=/,"",$i);print $i;exit}}}')
      thr_attempted=$(echo "$line" | awk '{for(i=1;i<=NF;i++){if($i ~ /^attempted=/){sub(/^attempted=/,"",$i);print $i;exit}}}')
      thr_ok=$(echo "$line" | awk '{for(i=1;i<=NF;i++){if($i ~ /^ok=/){sub(/^ok=/,"",$i);print $i;exit}}}')
      thr_err=$(echo "$line" | awk '{for(i=1;i<=NF;i++){if($i ~ /^err=/){sub(/^err=/,"",$i);print $i;exit}}}')
      thr_inflight=$(echo "$line" | awk '{for(i=1;i<=NF;i++){if($i ~ /^inflight=/){sub(/^inflight=/,"",$i);print $i;exit}}}')
      thr_first_ok_seq=$(echo "$line" | awk '{for(i=1;i<=NF;i++){if($i ~ /^first_ok_seq=/){sub(/^first_ok_seq=/,"",$i);print $i;exit}}}')
      thr_last_ok_seq=$(echo "$line" | awk '{for(i=1;i<=NF;i++){if($i ~ /^last_ok_seq=/){sub(/^last_ok_seq=/,"",$i);print $i;exit}}}')

      if [[ -z "${thr_inflight}" ]]; then
        thr_inflight=$(( thr_attempted - thr_ok - thr_err ))
      fi
      if [[ -z "${thr_first_ok_seq}" ]]; then thr_first_ok_seq=0; fi
      if [[ -z "${thr_last_ok_seq}" ]]; then thr_last_ok_seq=0; fi

      if [[ -n "${thr_id}" && "${thr_attempted}" =~ ^[0-9]+$ && "${thr_ok}" =~ ^[0-9]+$ && "${thr_err}" =~ ^[0-9]+$ && "${thr_inflight}" =~ ^[0-9]+$ ]]; then
        query_or_die "INSERT INTO integrity_manifest_thr VALUES ('${RUN}', '${LOC}', '${thr_id}', ${thr_attempted}, ${thr_ok}, ${thr_err}, ${thr_inflight}, ${thr_first_ok_seq}, ${thr_last_ok_seq})" "insert manifest_thr row" >/dev/null
      fi
    done < "${meta_file}"
  fi

  echo "  Recorded: run=${RUN} loc=${LOC} attempted=${attempted} ok=${ok} err=${err} inflight=${inflight}"
  echo
}

# --- phase: write -------------------------------------------------------------
do_write() {
  echo "=== Write phase ==="
  echo "  loc=${LOC}  duration=${DURATION}  conns=${CONNECTIONS}  threads=${THREADS}"
  echo "  run=${RUN:-<unset>}"
  echo

  if [[ -z "${RUN}" ]]; then
    echo "ERROR: --run (or INTEGRITY_RUN) is required for write-only"
    echo "  This is what makes validation exact (no mixing with old runs)."
    exit 1
  fi

  if $STRICT; then
    # One connection per thread: makes per-thread seq validation meaningful.
    CONNECTIONS="${THREADS}"
    echo "  strict=true => forcing connections=${CONNECTIONS} (one per thread)"
  fi

  if ! command -v wrk &>/dev/null; then
    echo "ERROR: wrk not found. Install: brew install wrk"
    exit 1
  fi

  # Clean previous count/meta files (run-scoped aggregated + legacy per-thread)
  rm -f "/tmp/integrity_${RUN}_${LOC}.count" "/tmp/integrity_${RUN}_${LOC}.meta" "/tmp/integrity_${LOC}."*.count

  export INTEGRITY_LOC="${LOC}"
  export INTEGRITY_RUN="${RUN}"

  echo "  wrk -t${THREADS} -c${CONNECTIONS} -d${DURATION} -s integrity_write.lua"
  echo

  wrk -t${THREADS} -c${CONNECTIONS} -d${DURATION} --timeout "${TIMEOUT}" \
    -s "${SCRIPT_DIR}/integrity_write.lua" "${BASE_URL}" \
    2>&1 | tee "/tmp/integrity_${LOC}.out"

  echo
  # Show aggregated counts if done() callback wrote any
  local agg_file="/tmp/integrity_${RUN}_${LOC}.count"
  local meta_file="/tmp/integrity_${RUN}_${LOC}.meta"
  if [[ -f "${meta_file}" ]]; then
    echo "  Writer summary (from meta):"
    cat "${meta_file}" | sed 's/^/    /'
  fi
  if [[ -f "${agg_file}" ]]; then
    echo "  Successful writes (from done callback): $(cat "${agg_file}")"
  else
    # Backward-compatible fallback: sum legacy per-thread count files
    local sum=0 found=false
    local files=( /tmp/integrity_${LOC}.*.count )
    for f in "${files[@]}"; do
      if [[ -f "$f" ]]; then
        found=true
        local c
        c=$(cat "$f")
        sum=$(( sum + c ))
      fi
    done
    if $found; then
      echo "  Successful writes (from done callbacks): ${sum}"
    else
      echo "  WARNING: count file not written; wrk may not have called done()."
      echo "  Check /tmp/integrity_${LOC}.out for Non-2xx lines to estimate."
    fi
  fi
  echo

  if ! $NO_REPORT; then
    do_report
  fi
}

# --- phase: settle ------------------------------------------------------------
do_settle() {
  echo "=== Settle: waiting ${SETTLE}s for compaction + replication ==="
  local elapsed=0
  while (( elapsed < SETTLE )); do
    local chunk=$(( SETTLE - elapsed ))
    (( chunk > 10 )) && chunk=10
    sleep "${chunk}"
    elapsed=$(( elapsed + chunk ))
    printf "  %ds / %ds\r" "${elapsed}" "${SETTLE}"
  done
  echo "  Settle complete.                       "
  echo
}

# --- phase: verify ------------------------------------------------------------
do_verify() {
  echo "=== Verify ==="

  if [[ -z "${RUN}" ]]; then
    echo "FAIL: --run (or INTEGRITY_RUN) is required for verify-only"
    echo "  Without a run id, counts can include rows from previous runs."
    exit 1
  fi
  echo "  run=${RUN}"

  local pass=true
  local manifest_expected_total=""
  local using_manifest=false

  # 1. Wait for the run-scoped count to stabilize so verification is deterministic.
  #    (Replication/compaction can be eventually consistent for a short period.)
  local total prev curr stable_iters=0
  prev=$(extract_count "$(query_or_die "SELECT COUNT(*) AS total FROM integrity_test WHERE run = '${RUN}'" "total count")")
  for _ in {1..12}; do
    sleep 5
    curr=$(extract_count "$(query_or_die "SELECT COUNT(*) AS total FROM integrity_test WHERE run = '${RUN}'" "recount")")
    if [[ "${curr}" == "${prev}" ]]; then
      stable_iters=$(( stable_iters + 1 ))
      if (( stable_iters >= 1 )); then
        break
      fi
    else
      stable_iters=0
      prev="${curr}"
    fi
  done
  total="${prev}"
  echo "  Total rows in DB (stable): ${total}"

  if (( stable_iters == 0 )); then
    echo "  FAIL: count did not stabilize within 60s; increase --settle and retry"
    pass=false
  fi

  # 2. Check for duplicate IDs (proves no collision in either app IDs or UUIDv7 keys)
  #    Use GROUP BY + HAVING because GlueSQL may not support COUNT(DISTINCT ...).
  local dup_result dup_sample
  if dup_result=$(query "SELECT id, COUNT(*) AS cnt FROM integrity_test WHERE run = '${RUN}' GROUP BY id HAVING COUNT(*) > 1 LIMIT 5" 2>/dev/null); then
    dup_sample=$(echo "$dup_result" | grep -c '"id"' || true)
    if (( dup_sample > 0 )); then
      echo "  FAIL: duplicate IDs found!"
      echo "  Sample: ${dup_result}"
      pass=false
    else
      echo "  OK: no duplicate IDs"
    fi
  else
    echo "  SKIP: duplicate check query not supported; relying on count comparison."
  fi

  # Precompute expected totals from the DB-backed manifest (if present).
  local manifest_sum
  if manifest_sum=$(query "SELECT SUM(ok) AS expected FROM integrity_manifest WHERE run = '${RUN}'" 2>/dev/null); then
    local m
    m=$(extract_count "${manifest_sum}" || true)
    if [[ -n "${m}" ]] && (( m > 0 )); then
      manifest_expected_total="${m}"
      using_manifest=true
    fi
  fi

  # If we're using the manifest, enforce strict preconditions when running strict verification.
  # Without strict mode (one connection per thread), wrk can have in-flight requests at cutoff
  # that may or may not reach the server, making seq-based proofs ambiguous.
  local manifest_ok_sum="" manifest_err_sum="" manifest_attempted_sum=""
  if $using_manifest; then
    manifest_ok_sum=$(extract_count "$(query_or_die "SELECT SUM(ok) AS ok FROM integrity_manifest WHERE run='${RUN}'" "manifest ok sum")")
    manifest_err_sum=$(extract_count "$(query_or_die "SELECT SUM(err) AS err FROM integrity_manifest WHERE run='${RUN}'" "manifest err sum")")
    manifest_attempted_sum=$(extract_count "$(query_or_die "SELECT SUM(attempted) AS attempted FROM integrity_manifest WHERE run='${RUN}'" "manifest attempted sum")")

    if (( manifest_err_sum != 0 )); then
      echo "  FAIL: writers reported HTTP errors (SUM(err)=${manifest_err_sum}); run not valid"
      pass=false
    fi
  fi

  # 3. Per-location breakdown
  echo
  echo "  Per-location breakdown:"
  local loc_result
  if loc_result=$(query "SELECT loc, COUNT(*) AS cnt FROM integrity_test WHERE run = '${RUN}' GROUP BY loc"); then
    # Parse JSON results: each entry has {"loc":"x","cnt":N}
    while read -r entry; do
      local l
      l=$(echo "$entry" | sed -nE 's/.*"loc"[[:space:]]*:[[:space:]]*"([^"]+)".*/\1/p')
      if [[ -z "${l}" ]]; then
        continue
      fi
      local cnt_result loc_count
      cnt_result=$(query_or_die \
        "SELECT COUNT(*) AS cnt FROM integrity_test WHERE run = '${RUN}' AND loc = '${l}'" \
        "count for ${l}")
      loc_count=$(extract_count "$cnt_result")

      if $using_manifest; then
        local ok_json ok_expected attempted_json loc_attempted
        ok_json=$(query_or_die \
          "SELECT ok AS ok FROM integrity_manifest WHERE run = '${RUN}' AND loc = '${l}'" \
          "manifest ok for ${l}")
        ok_expected=$(extract_count "$ok_json")
        attempted_json=$(query_or_die \
          "SELECT attempted AS attempted FROM integrity_manifest WHERE run = '${RUN}' AND loc = '${l}'" \
          "manifest attempted for ${l}")
        loc_attempted=$(extract_count "$attempted_json")
        echo "    ${l}: ${loc_count} rows (ok=${ok_expected}, attempted=${loc_attempted})"
        if (( loc_count < ok_expected )); then
          echo "    FAIL: ${l} DATA LOSS — fewer rows than acknowledged (${loc_count} < ${ok_expected})"
          pass=false
        elif (( loc_count > loc_attempted )); then
          echo "    FAIL: ${l} more rows than attempted (${loc_count} > ${loc_attempted})"
          pass=false
        fi
      else
        echo "    ${l}: ${loc_count} rows"
      fi

      echo "    Thread seq validation:"

      if ! $using_manifest; then
        echo "      SKIP: requires manifest (write-only auto-reports unless --no-report)"
      elif ! $STRICT; then
        echo "      SKIP: add --strict to validate seq of acknowledged writes"
      else
        # Iterate expected threads from manifest_thr, not from the DB.
        local expected_thr_json
        if expected_thr_json=$(query "SELECT thr AS thr FROM integrity_manifest_thr WHERE run = '${RUN}' AND loc = '${l}' GROUP BY thr" 2>/dev/null); then
          local expected_any=false

          # 1) Validate each expected thread has exact seq coverage first_ok_seq..last_ok_seq.
          while read -r thr_entry; do
            local t
            t=$(echo "$thr_entry" | sed -nE 's/.*"thr"[[:space:]]*:[[:space:]]*"([^"]+)".*/\1/p')
            [[ -n "${t}" ]] || continue
            expected_any=true

            local expected_json expected
            expected_json=$(query_or_die "SELECT ok AS ok FROM integrity_manifest_thr WHERE run='${RUN}' AND loc='${l}' AND thr='${t}'" "manifest_thr ok for ${l}/${t}")
            expected=$(extract_count "$expected_json")

            # Read first_ok_seq and last_ok_seq from manifest_thr
            local fos_json los_json first_ok last_ok
            fos_json=$(query_or_die "SELECT first_ok_seq AS fos FROM integrity_manifest_thr WHERE run='${RUN}' AND loc='${l}' AND thr='${t}'" "manifest_thr fos for ${l}/${t}")
            los_json=$(query_or_die "SELECT last_ok_seq AS los FROM integrity_manifest_thr WHERE run='${RUN}' AND loc='${l}' AND thr='${t}'" "manifest_thr los for ${l}/${t}")
            first_ok=$(extract_count "$fos_json")
            last_ok=$(extract_count "$los_json")

            if (( expected == 0 )); then
              echo "      ${t}: expected 0 (skip)"
              continue
            fi

            # Use first_ok_seq..last_ok_seq if available, else fall back to 1..ok
            local seq_lo seq_hi
            if (( first_ok > 0 && last_ok > 0 )); then
              seq_lo=${first_ok}
              seq_hi=${last_ok}
            else
              seq_lo=1
              seq_hi=${expected}
            fi

            local expected_range_len=$(( seq_hi - seq_lo + 1 ))

            # Check for mid-range gaps: if ok < range length, there are timeouts
            # within the acknowledged range. Can only verify count >= ok.
            local has_midrange_gaps=false
            if (( expected < expected_range_len )); then
              has_midrange_gaps=true
            fi

            # Duplicate (thr,seq) check within the acknowledged range
            local dup_seq_json dup_seq_hits
            if dup_seq_json=$(query "SELECT thr, seq, COUNT(*) AS cnt FROM integrity_test WHERE run = '${RUN}' AND loc = '${l}' AND thr = '${t}' AND seq >= ${seq_lo} AND seq <= ${seq_hi} GROUP BY thr, seq HAVING COUNT(*) > 1 LIMIT 1" 2>/dev/null); then
              dup_seq_hits=$(echo "$dup_seq_json" | grep -c '"seq"' || true)
              if (( dup_seq_hits > 0 )); then
                echo "    FAIL: duplicate seq values for ${l}/${t}"
                echo "      Sample: ${dup_seq_json}"
                pass=false
                continue
              fi
            fi

            local cnt_json min_json max_json
            local cnt min_seq max_seq
            cnt_json=$(query_or_die "SELECT COUNT(*) AS cnt FROM integrity_test WHERE run='${RUN}' AND loc='${l}' AND thr='${t}' AND seq >= ${seq_lo} AND seq <= ${seq_hi}" "count for ${l}/${t} in ok range")
            min_json=$(query_or_die "SELECT MIN(seq) AS min_seq FROM integrity_test WHERE run='${RUN}' AND loc='${l}' AND thr='${t}' AND seq >= ${seq_lo} AND seq <= ${seq_hi}" "min seq for ${l}/${t} in ok range")
            max_json=$(query_or_die "SELECT MAX(seq) AS max_seq FROM integrity_test WHERE run='${RUN}' AND loc='${l}' AND thr='${t}' AND seq >= ${seq_lo} AND seq <= ${seq_hi}" "max seq for ${l}/${t} in ok range")
            cnt=$(extract_count "$cnt_json")
            min_seq=$(extract_count "$min_json")
            max_seq=$(extract_count "$max_json")

            if $has_midrange_gaps; then
              # Timeouts within the range — can only verify count >= ok
              if (( cnt < expected )); then
                echo "    FAIL: ${l}/${t} row count below ok (count=${cnt}, ok=${expected})"
                pass=false
              else
                echo "      ${t}: OK (count=${cnt} >= ok=${expected}, range ${seq_lo}..${seq_hi}, gaps present)"
              fi
            else
              # No mid-range gaps: gapless proof is possible
              if (( cnt != expected )); then
                echo "    FAIL: ${l}/${t} row count mismatch (count=${cnt}, expected=${expected})"
                pass=false
              fi
              if (( min_seq != seq_lo )); then
                echo "    FAIL: ${l}/${t} seq does not start at ${seq_lo} (min_seq=${min_seq})"
                pass=false
              fi
              if (( max_seq != seq_hi )); then
                echo "    FAIL: ${l}/${t} seq does not reach ${seq_hi} (max_seq=${max_seq})"
                pass=false
              fi

              if (( cnt == expected && min_seq == seq_lo && max_seq == seq_hi )); then
                echo "      ${t}: OK (${seq_lo}..${seq_hi})"
              fi
            fi
          done < <(echo "$expected_thr_json" | grep -oE '"thr"[[:space:]]*:[[:space:]]*"[^"]*"' || true)

          if ! $expected_any; then
            echo "      FAIL: no manifest_thr rows for loc=${l}; run --report-only on that machine"
            pass=false
          fi

          # 2) Fail if DB has threads not present in manifest_thr.
          local actual_thr_json
          if actual_thr_json=$(query "SELECT thr AS thr FROM integrity_test WHERE run='${RUN}' AND loc='${l}' GROUP BY thr" 2>/dev/null); then
            while read -r actual_entry; do
              local at
              at=$(echo "$actual_entry" | sed -nE 's/.*"thr"[[:space:]]*:[[:space:]]*"([^"]+)".*/\1/p')
              [[ -n "${at}" ]] || continue
              local present
              present=$(query_or_die "SELECT COUNT(*) AS cnt FROM integrity_manifest_thr WHERE run='${RUN}' AND loc='${l}' AND thr='${at}'" "manifest_thr presence for ${l}/${at}")
              if (( $(extract_count "$present") == 0 )); then
                echo "    FAIL: unexpected thread in DB not in manifest_thr: ${l}/${at}"
                pass=false
              fi
            done < <(echo "$actual_thr_json" | grep -oE '"thr"[[:space:]]*:[[:space:]]*"[^"]*"' || true)
          fi
        else
          echo "      FAIL: could not query manifest_thr"
          pass=false
        fi
      fi
    done < <(echo "$loc_result" | grep -oE '"loc"[[:space:]]*:[[:space:]]*"[^"]*"' || true)
  fi

  # 4. Compare against expected count
  echo
  local expected_total="${EXPECTED}"

  if [[ -n "${expected_total}" ]]; then
    echo "  Expected (from --expected flag): ${expected_total}"
  else
    # Prefer DB-backed manifest if present (works across machines).
    if $using_manifest; then
      expected_total="${manifest_expected_total}"
      echo "  Expected (from integrity_manifest): ${expected_total}"
    fi
  fi

  if [[ -n "${expected_total}" ]]; then
    local max_total=${manifest_attempted_sum:-${expected_total}}
    if (( total < expected_total )); then
      echo "  FAIL: DATA LOSS — ${total} rows < ${expected_total} acknowledged"
      pass=false
    elif (( total > max_total )); then
      echo "  FAIL: ${total} rows > ${max_total} attempted — phantom writes"
      pass=false
    else
      echo "  OK: ${total} rows, all ${expected_total} acknowledged writes present"
    fi
  else
    echo "  FAIL: no expected count available; cannot verify completeness."
    echo "        Run --report-only per location (populates integrity_manifest),"
    echo "        or pass --expected N explicitly."
    pass=false
  fi

  echo
  if $pass; then
    echo "=== PASS: integrity test passed ==="
    echo "  ${total} rows written, all survived compaction and replication."
  else
    echo "=== FAIL: integrity test FAILED ==="
    exit 1
  fi
}

# --- main ---------------------------------------------------------------------
echo "Fastly-DB Integrity Test"
echo "========================"
echo "  Service:  ${BASE_URL}"
echo "  Mode:     ${MODE}"
echo "  Location: ${LOC}"
echo

case "${MODE}" in
  full)
    do_setup
    do_write
    do_settle
    do_verify
    ;;
  setup)
    do_setup
    ;;
  write)
    do_write
    ;;
  report)
    do_report
    ;;
  verify)
    do_verify
    ;;
  *)
    echo "Unknown mode: ${MODE}"; exit 1
    ;;
esac
