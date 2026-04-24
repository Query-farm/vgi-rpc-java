#!/bin/bash
# Show the full long-form traceback for a single pytest test id (or pattern).
#
# Usage:
#   ./inspect.sh echo_point                 # keyword
#   ./inspect.sh "echo_point and pipe"      # pytest -k expression
#   ./inspect.sh TestUnaryLogging           # class name
#
# Assumes the Java worker is already built. If not, run ./run_tests.sh first.
set -u
export JAVA_HOME=/opt/homebrew/opt/openjdk@21
cd "$(dirname "$0")"

PY=/Users/rusty/Development/vgi-rpc/.venv/bin/python
PATTERN="${1:?usage: inspect.sh <test-pattern> [more pytest args...]}"
shift || true

pkill -f conformance-worker 2>/dev/null
sleep 0.3

"$PY" -m pytest test_java_conformance.py \
    -p no:cacheprovider --tb=long -q \
    -k "$PATTERN" "$@" 2>&1
