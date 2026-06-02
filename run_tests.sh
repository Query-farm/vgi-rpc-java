#!/bin/bash
# Build Java + run the pytest conformance suite + summarise.
#
# Usage:
#   ./run_tests.sh                # full suite against all transports
#   ./run_tests.sh pipe           # only pipe transport
#   ./run_tests.sh "echo_point"   # keyword filter (pytest -k)
#   ./run_tests.sh --no-build …   # skip gradle rebuild
#
# Full output lands in /tmp/pytest_java.txt; the summary is printed here.
set -u
# JDK 25 matches the build toolchain. The worker (a non-published module) is
# compiled at release 25, so it must run on a >= 25 runtime; the FFM shared-
# memory overlay is active here. (Java-21 baseline degradation is covered by
# the JUnit `-PtestJdk=21` lane, not this Python conformance suite.)
export JAVA_HOME=/opt/homebrew/opt/openjdk@25
cd "$(dirname "$0")"

OUT=/tmp/pytest_java.txt
PY=/Users/rusty/Development/vgi-rpc/.venv/bin/python

BUILD=1
if [[ "${1:-}" == "--no-build" ]]; then BUILD=0; shift; fi
FILTER="${1:-}"

if [[ $BUILD -eq 1 ]]; then
    ./gradlew -q installDist || { echo "BUILD FAILED"; exit 1; }
fi

# Kill stragglers from a previous run (HTTP/unix workers persist).
pkill -f conformance-worker 2>/dev/null
sleep 0.3

# -p no:cacheprovider keeps .pytest_cache out of the tree; --tb=line gives
# one-line tracebacks; -q keeps progress dots compact.
if [[ -n "$FILTER" ]]; then
    "$PY" -m pytest tests/test_java_conformance.py \
        -p no:cacheprovider --tb=short -q -k "$FILTER" \
        > "$OUT" 2>&1
else
    "$PY" -m pytest tests/test_java_conformance.py \
        -p no:cacheprovider --tb=line -q \
        > "$OUT" 2>&1
fi
PYTEST_EXIT=$?

echo "=== totals ==="
# Pytest final summary line like "X failed, Y passed, Z skipped in T s".
# Search the whole file since extra lines can follow in noisy runs.
grep -E '^[0-9]+ (failed|passed|error)' "$OUT" | tail -n 1

echo
echo "=== failures by test (deduped, first 30) ==="
grep -E '^FAILED' "$OUT" | sed -E 's/\[.*//' | sort | uniq -c | sort -rn | head -30

echo
echo "=== failures by transport ==="
grep -E '^FAILED' "$OUT" | sed -E 's/.*\[([^]]+)\].*/\1/' | sort | uniq -c

echo
echo "(full output: $OUT — use ./inspect.sh <test_id> for details)"
exit $PYTEST_EXIT
