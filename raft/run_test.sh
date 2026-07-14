#!/usr/bin/env bash
# Run a Raft test suite (3A/3B/3C/3D, or ALL of them) N times in parallel,
# collect results into out_<suite>.txt, and finish with a diagnostics summary
# (total / passed / failed / time).
# Usage: ./run_test.sh [suite] [runs] [parallel]
#   suite: A, B, C, D, or ALL (prompted for if omitted)
#   defaults: 100 runs, 5 at a time
# Examples:
#   ./run_test.sh              # prompts for suite
#   ./run_test.sh D            # 100 runs of 3D
#   ./run_test.sh D 30 10      # 30 runs of 3D, 10 in parallel
#   ./run_test.sh ALL 50       # 50 runs of the full 3A-3D gauntlet
set -e
cd "$(dirname "$0")"

# suite may come as the first argument; runs/parallel shift behind it
CHOICE="${1:-}"
RUNS="${2:-100}"
PAR="${3:-5}"

# ask which suite to run when no argument was given; re-ask on anything
# that isn't A/B/C/D/ALL
while true; do
    if [ -z "$CHOICE" ]; then
        read -r -p "Which test? (A/B/C/D/ALL): " CHOICE
    fi
    case "$CHOICE" in
        [Aa])        SUITE="3A";             NAME="3a";  break ;;
        [Bb])        SUITE="3B";             NAME="3b";  break ;;
        [Cc])        SUITE="3C";             NAME="3c";  break ;;
        [Dd])        SUITE="3D";             NAME="3d";  break ;;
        [Aa][Ll][Ll]) SUITE="3A|3B|3C|3D";   NAME="all"; break ;;
        *)  echo "Invalid choice '$CHOICE' — enter A, B, C, D, or ALL."
            CHOICE="" ;;
    esac
done

OUT="out_$NAME.txt"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

START=$(date +%s)

# the tester runs each raft peer as a subprocess of src/main/raft1d — rebuild it
# or every run silently tests whatever code raft1d was last built from
(cd .. && go build -race -o main/raft1d main/raft1d.go)

go test -c -o "$TMP/raft.test" .

# SUITE is quoted inside the inner shell too: the ALL pattern contains '|',
# which an unquoted expansion would turn into shell pipes
seq 1 "$RUNS" | xargs -P "$PAR" -I{} sh -c "\"$TMP/raft.test\" -test.run \"$SUITE\" > \"$TMP/run{}.log\" 2>&1 || true"

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
