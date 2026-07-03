#!/usr/bin/env bash
# Run the 3B test suite N times in parallel, collect results into out_3b.txt,
# and finish with a diagnostics summary (total / passed / failed / which runs / time).
# Usage: ./run3b.sh [runs] [parallel]   (defaults: 100 runs, 5 at a time)
set -e
cd "$(dirname "$0")"

RUNS="${1:-100}"
PAR="${2:-5}"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

START=$(date +%s)

# the tester runs each raft peer as a subprocess of src/main/raft1d — rebuild it
# or every run silently tests whatever code raft1d was last built from
(cd .. && go build -race -o main/raft1d main/raft1d.go)

go test -c -o "$TMP/raft3b.test" .

seq 1 "$RUNS" | xargs -P "$PAR" -I{} sh -c "\"$TMP/raft3b.test\" -test.run 3B > \"$TMP/run{}.log\" 2>&1 || true"

PASSED=0
FAILED=0
FAILED_RUNS=()
for i in $(seq 1 "$RUNS"); do
    if grep -q '^--- FAIL' "$TMP/run$i.log"; then
        FAILED=$((FAILED + 1))
        FAILED_RUNS+=("$i")
    else
        PASSED=$((PASSED + 1))
    fi
done

ELAPSED=$(( $(date +%s) - START ))

{
    for i in $(seq 1 "$RUNS"); do
        echo "=== run $i ==="
        cat "$TMP/run$i.log"
    done
    echo ""
    echo "==================== SUMMARY ===================="
    echo "total runs : $RUNS  (parallelism $PAR)"
    echo "passed     : $PASSED"
    echo "failed     : $FAILED"
    if [ "$FAILED" -gt 0 ]; then
        echo "failed runs: ${FAILED_RUNS[*]}"
    fi
    echo "elapsed    : $((ELAPSED / 60))m $((ELAPSED % 60))s"
    echo "================================================="
} > out_3b.txt

tail -8 out_3b.txt
