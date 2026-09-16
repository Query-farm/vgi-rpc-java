#!/usr/bin/env bash
# Run the `native Iroh HTTP integration` CI lane locally.
#
# This is the one lane where Java is the *client* and the Python reference is
# the server, so it is the only gate that can catch this port emitting a
# request shape the reference does not serve. `./gradlew build` cannot: the
# JUnit test assumes its way out when VGI_IROH_HTTP_TEST_ENDPOINT is unset, so
# it reports green by not running. The conformance suite cannot either --
# there Java is the server.
#
# Usage:
#   scripts/run_iroh_integration.sh                 # build the bridge, then test
#   scripts/run_iroh_integration.sh --no-cargo      # reuse an existing bridge build
#
# Requirements: cargo, a Python with the reference `vgi_rpc[http]` + waitress
# importable (VGI_RPC_PYTHON, else ../vgi-rpc-python/.venv/bin/python), and a
# checkout of vgi-rpc-rust (VGI_RPC_RUST_DIR, else ../vgi-rpc-rust).
#
# The Rust bridge revision and the Python reference revision both come from
# .github/workflows/ci.yml, so this script cannot drift from the lane it
# reproduces. It does NOT check the local checkouts out to those revisions --
# it prints them, and leaves choosing to you.
set -euo pipefail
cd "$(dirname "$0")/.."

CARGO_BUILD=1
[[ "${1:-}" == "--no-cargo" ]] && CARGO_BUILD=0

pin() { sed -n "s/^  $1: *//p" .github/workflows/ci.yml | head -n 1; }
echo "ci.yml pins: rust=$(pin VGI_RPC_IROH_RUST_REV) python=$(pin VGI_RPC_PYTHON_REV)"

PY="${VGI_RPC_PYTHON:-$HOME/Development/vgi-rpc-python/.venv/bin/python}"
RUST_DIR="${VGI_RPC_RUST_DIR:-$HOME/Development/vgi-rpc-rust}"
[[ -x "$PY" ]] || { echo "no python at $PY (set VGI_RPC_PYTHON)"; exit 1; }
[[ -d "$RUST_DIR" ]] || { echo "no rust checkout at $RUST_DIR (set VGI_RPC_RUST_DIR)"; exit 1; }

if [[ $CARGO_BUILD -eq 1 ]]; then
    (cd "$RUST_DIR" && cargo build --locked --release -p vgi-iroh-bridge)
fi
BRIDGE="$RUST_DIR/target/release/vgi-iroh-bridge"
[[ -x "$BRIDGE" ]] || { echo "no bridge binary at $BRIDGE"; exit 1; }

tmp=$(mktemp -d)
worker_log="$tmp/worker.log"
bridge_log="$tmp/bridge.log"
worker_pid=
bridge_pid=
cleanup() {
    code=$?
    [[ -n "$bridge_pid" ]] && kill "$bridge_pid" 2>/dev/null || true
    [[ -n "$worker_pid" ]] && kill "$worker_pid" 2>/dev/null || true
    if [[ $code -ne 0 ]]; then cat "$worker_log" "$bridge_log" 2>/dev/null || true; fi
    exit $code
}
trap cleanup EXIT

"$PY" tests/serve_iroh_http.py >"$worker_log" 2>&1 &
worker_pid=$!
for _ in $(seq 1 30); do
    grep -q '^PORT:' "$worker_log" && break
    kill -0 "$worker_pid" 2>/dev/null || break
    sleep 1
done
port=$(sed -n 's/^PORT://p' "$worker_log" | head -n 1)
[[ -n "$port" ]] || { cat "$worker_log"; exit 1; }

"$BRIDGE" --ephemeral --no-relay --print-direct-addresses \
    --http-upstream "http://127.0.0.1:$port" >"$bridge_log" 2>&1 &
bridge_pid=$!
for _ in $(seq 1 30); do
    grep -q '^DIRECT:' "$bridge_log" && break
    kill -0 "$bridge_pid" 2>/dev/null || break
    sleep 1
done
endpoint=$(grep -m1 -E '^[0-9a-f]{64}$' "$bridge_log" || true)
direct=$(sed -n 's/^DIRECT://p' "$bridge_log" | paste -sd, -)
[[ -n "$endpoint" && -n "$direct" ]] || { cat "$bridge_log"; exit 1; }

VGI_IROH_HTTP_TEST_ENDPOINT="$endpoint" \
VGI_IROH_HTTP_TEST_DIRECT_ADDRESSES="$direct" \
    ./gradlew --no-daemon :vgirpc-iroh:test -PtestJdk=21 --rerun-tasks \
        --tests farm.query.vgirpc.iroh.OfficialIrohHttpIntegrationTest
