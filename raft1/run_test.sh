#!/usr/bin/env bash
# Run a Raft test suite (3A/3B/3C/3D) N times in parallel, collect results into
# out_<suite>.txt, and finish with a diagnostics summary (total / passed / failed / time).
# Usage: ./run_test.sh [runs] [parallel]   (defaults: 100 runs, 5 at a time)
# The script prompts for which suite to run: A, B, C, or D.
set -e
cd "$(dirname "$0")"

RUNS="${1:-100}"
PAR="${2:-5}"

# ask which suite to run; re-ask on anything that isn't A/B/C/D
while true; do
    read -r -p "Which test? (A/B/C/D): " CHOICE
    case "$CHOICE" in
        [Aa]) SUITE="3A"; break ;;
        [Bb]) SUITE="3B"; break ;;
        [Cc]) SUITE="3C"; break ;;
        [Dd]) SUITE="3D"; break ;;
        *)    echo "Invalid choice '$CHOICE' — enter A, B, C, or D." ;;
    esac
done

OUT="out_$(echo "$SUITE" | tr '[:upper:]' '[:lower:]').txt"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

START=$(date +%s)

# the tester runs each raft peer as a subprocess of src/main/raft1d — rebuild it
# or every run silently tests whatever code raft1d was last built from
(cd .. && go build -race -o main/raft1d main/raft1d.go)

go test -c -o "$TMP/raft.test" .

seq 1 "$RUNS" | xargs -P "$PAR" -I{} sh -c "\"$TMP/raft.test\" -test.run $SUITE > \"$TMP/run{}.log\" 2>&1 || true"

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
    echo "suite      : $SUITE"
    echo "total runs : $RUNS  (parallelism $PAR)"
    echo "passed     : $PASSED"
    echo "failed     : $FAILED"
    if [ "$FAILED" -gt 0 ]; then
        echo "failed runs: ${FAILED_RUNS[*]}"
    fi
    echo "elapsed    : $((ELAPSED / 60))m $((ELAPSED % 60))s"
    echo "================================================="
} > "$OUT"

tail -9 "$OUT"
