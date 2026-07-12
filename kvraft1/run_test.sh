#!/usr/bin/env bash
# Run a Lab 4 test suite (4A/4B/4C, or ALL of them) N times in parallel,
# collect results into out_<suite>.txt, and finish with a diagnostics summary
# (total / passed / failed / time).
#
# Lab 4 tests are split across two packages:
#   4A: kvraft1/rsm        4B: kvraft1        4C: both
# Each run executes the matching tests from every package that has them.
#
# Usage: ./run_test.sh [suite] [runs] [parallel]
#   suite: A, B, C, or ALL (prompted for if omitted)
#   defaults: 100 runs, 5 at a time
# Examples:
#   ./run_test.sh              # prompts for suite
#   ./run_test.sh B            # 100 runs of 4B
#   ./run_test.sh B 30 10      # 30 runs of 4B, 10 in parallel
#   ./run_test.sh ALL 50       # 50 runs of the full 4A-4C gauntlet
set -e
cd "$(dirname "$0")"

# suite may come as the first argument; runs/parallel shift behind it
CHOICE="${1:-}"
RUNS="${2:-100}"
PAR="${3:-5}"

# ask which suite to run when no argument was given; re-ask on anything
# that isn't A/B/C/ALL
while true; do
    if [ -z "$CHOICE" ]; then
        read -r -p "Which test? (A/B/C/ALL): " CHOICE
    fi
    case "$CHOICE" in
        [Aa])         RSM_PAT="4A";    KV_PAT="";       NAME="4a";  break ;;
        [Bb])         RSM_PAT="";      KV_PAT="4B";     NAME="4b";  break ;;
        [Cc])         RSM_PAT="4C";    KV_PAT="4C";     NAME="4c";  break ;;
        [Aa][Ll][Ll]) RSM_PAT="4A|4C"; KV_PAT="4B|4C";  NAME="all"; break ;;
        *)  echo "Invalid choice '$CHOICE' — enter A, B, C, or ALL."
            CHOICE="" ;;
    esac
done

OUT="out_$NAME.txt"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

START=$(date +%s)

go test -race -c -o "$TMP/rsm.test" ./rsm
go test -race -c -o "$TMP/kvraft.test" .

# one run = matching tests from each package that has them, appended into a
# single log. The run number arrives as $1 (xargs -n 1) rather than via -I{},
# whose 255-byte replacement limit chokes on a command this long; patterns
# stay quoted because they can contain '|', which an unquoted expansion would
# turn into shell pipes
export TMP RSM_PAT KV_PAT
seq 1 "$RUNS" | xargs -P "$PAR" -n 1 sh -c '
    i=$1
    : > "$TMP/run$i.log"
    if [ -n "$RSM_PAT" ]; then
        "$TMP/rsm.test" -test.run "$RSM_PAT" >> "$TMP/run$i.log" 2>&1 || true
    fi
    if [ -n "$KV_PAT" ]; then
        "$TMP/kvraft.test" -test.run "$KV_PAT" >> "$TMP/run$i.log" 2>&1 || true
    fi
' _

PASSED=0
FAILED=0
FAILED_RUNS=()
for i in $(seq 1 "$RUNS"); do
    # tester's Fatalf exits via log.Fatalf, killing the binary before the
    # framework prints '--- FAIL:' — so a run passes only if it printed PASS
    # and never printed FAIL in any form
    if grep -q 'FAIL' "$TMP/run$i.log" || ! grep -q '^PASS' "$TMP/run$i.log"; then
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
    echo "suite      : $NAME"
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
