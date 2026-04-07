#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# --- defaults ----------------------------------------------------------------
DURATION="30s"
CONNECTIONS=10
RATE=50
THREADS=2
TIMEOUT="10s"
SETUP=false

usage() {
  cat <<EOF
Usage: $0 [OPTIONS] <service-url>

Options:
  --setup          Run setup.sh first to create/seed the test table
  -d, --duration   Test duration          (default: ${DURATION})
  -c, --connections Number of connections (default: ${CONNECTIONS})
  -R, --rate       Target req/s (wrk2)    (default: ${RATE})
  -t, --threads    Number of threads      (default: ${THREADS})
  --timeout        Request timeout         (default: ${TIMEOUT})
  -h, --help       Show this help
EOF
  exit 0
}

# --- parse args ---------------------------------------------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --setup)       SETUP=true; shift ;;
    -d|--duration) DURATION="$2"; shift 2 ;;
    -c|--connections) CONNECTIONS="$2"; shift 2 ;;
    -R|--rate)     RATE="$2"; shift 2 ;;
    -t|--threads)  THREADS="$2"; shift 2 ;;
    --timeout)     TIMEOUT="$2"; shift 2 ;;
    -h|--help)     usage ;;
    -*)            echo "Unknown option: $1"; usage ;;
    *)             BASE_URL="$1"; shift ;;
  esac
done

BASE_URL="${BASE_URL:?Error: service URL required. Run with --help for usage.}"
BASE_URL="${BASE_URL%/}"

# --- detect wrk variant ------------------------------------------------------
if command -v wrk2 &>/dev/null; then
  WRK=wrk2
  RATE_FLAG="-R ${RATE}"
elif command -v wrk &>/dev/null; then
  WRK=wrk
  RATE_FLAG=""
  echo "WARNING: wrk2 not found; falling back to wrk (no constant-rate control)."
  echo "         Install wrk2 for accurate latency percentiles: brew install wrk2"
  echo
else
  echo "ERROR: Neither wrk2 nor wrk found. Install one first:"
  echo "  brew install wrk2   # recommended"
  echo "  brew install wrk"
  exit 1
fi

# --- setup --------------------------------------------------------------------
if $SETUP; then
  echo "=== Running setup ==="
  bash "${SCRIPT_DIR}/setup.sh" "$BASE_URL"
  echo
fi

# --- read benchmark -----------------------------------------------------------
echo "=== Read benchmark (SELECT) ==="
echo "  ${WRK} -t${THREADS} -c${CONNECTIONS} -d${DURATION} ${RATE_FLAG} -s ${SCRIPT_DIR}/read.lua ${BASE_URL}"
echo
${WRK} -t${THREADS} -c${CONNECTIONS} -d${DURATION} --timeout ${TIMEOUT} ${RATE_FLAG} -s "${SCRIPT_DIR}/read.lua" "${BASE_URL}"
echo

# --- write benchmark ----------------------------------------------------------
echo "=== Write benchmark (INSERT) ==="
echo "  ${WRK} -t${THREADS} -c${CONNECTIONS} -d${DURATION} ${RATE_FLAG} -s ${SCRIPT_DIR}/write.lua ${BASE_URL}"
echo
${WRK} -t${THREADS} -c${CONNECTIONS} -d${DURATION} --timeout ${TIMEOUT} ${RATE_FLAG} -s "${SCRIPT_DIR}/write.lua" "${BASE_URL}"
echo

echo "=== Done ==="
