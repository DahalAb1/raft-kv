#!/usr/bin/env bash
# Run a Raft test N times, capture every run (and any failure trace) into out_3b.
# Usage:
#   ./run_3b.sh                  # 100 runs of all 3B tests
#   ./run_3b.sh 50               # 50 runs
#   ./run_3b.sh 50 TestFailNoAgree3B   # 50 runs of one test

cd "$(dirname "$0")" || exit 1
set -o pipefail   # exit status reflects go test, not tee

RUNS="${1:-100}"
TEST="${2:-3B}"
OUT=out_3b

: > "$OUT"        # clear the file

pass=0
fail=0
for i in $(seq 1 "$RUNS"); do
  echo "=== run $i ===" | tee -a "$OUT"
  if go test -run "$TEST" -count=1 2>&1 | tee -a "$OUT"; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    echo ">>> run $i FAILED <<<" | tee -a "$OUT"
  fi
done

echo "SUMMARY: pass=$pass fail=$fail (test=$TEST, runs=$RUNS)" | tee -a "$OUT"
