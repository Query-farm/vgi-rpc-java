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
# JDK 25 matches the build toolchain (the worker is compiled at release 25).
# Honor an existing JAVA_HOME; else pick JDK 25 via macOS java_home; else PATH.
if [[ -z "${JAVA_HOME:-}" ]] && [[ -x /usr/libexec/java_home ]]; then
    JAVA_HOME=$(/usr/libexec/java_home -v 25 2>/dev/null) && export JAVA_HOME
fi
cd "$(dirname "$0")"

# Python with vgi_rpc importable. Override with VGI_RPC_PYTHON; else prefer a
# local reference venv, else system python3.
PY="${VGI_RPC_PYTHON:-}"
if [[ -z "$PY" ]]; then
    if [[ -x "$HOME/Development/vgi-rpc/.venv/bin/python" ]]; then
        PY="$HOME/Development/vgi-rpc/.venv/bin/python"
    else
        PY="python3"
    fi
fi
PATTERN="${1:?usage: inspect.sh <test-pattern> [more pytest args...]}"
shift || true

pkill -f conformance-worker 2>/dev/null
sleep 0.3

"$PY" -m pytest tests/test_java_conformance.py \
    -p no:cacheprovider --tb=long -q \
    -k "$PATTERN" "$@" 2>&1
