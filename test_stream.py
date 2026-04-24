"""Stream surface smoke test."""
import sys
import traceback

import pyarrow as pa

from vgi_rpc.conformance import ConformanceService, ConformanceHeader
from vgi_rpc.rpc import AnnotatedBatch, RpcError, SubprocessTransport, _RpcProxy

WORKER = "/Users/rusty/Development/vgi-rpc-java/conformance-worker/build/install/conformance-worker/bin/conformance-worker"


def ok(label, got, expected=None):
    if expected is not None and got != expected:
        print(f"FAIL {label}: expected {expected!r}, got {got!r}")
    else:
        print(f"OK   {label} -> {got!r}")


def main() -> int:
    transport = SubprocessTransport([WORKER])
    logs = []
    proxy = _RpcProxy(ConformanceService, transport, on_log=logs.append)
    try:
        # Producers
        rows = [ab.batch.to_pylist() for ab in proxy.produce_n(count=3)]
        ok("produce_n=3", rows, [[{"index": 0, "value": 0}], [{"index": 1, "value": 10}], [{"index": 2, "value": 20}]])

        ok("produce_empty", len(list(proxy.produce_empty())), 0)
        ok("produce_single count", len(list(proxy.produce_single())), 1)

        batches = list(proxy.produce_large_batches(rows_per_batch=4, batch_count=2))
        ok("produce_large_batches count", len(batches), 2)
        ok("produce_large_batches rows", sum(ab.batch.num_rows for ab in batches), 8)

        # Producer with logs
        logs.clear()
        rows = list(proxy.produce_with_logs(count=2))
        ok("produce_with_logs count", len(rows), 2)
        ok("produce_with_logs logs>=2", len(logs) >= 2, True)

        # Error mid-stream
        try:
            session = proxy.produce_error_mid_stream(emit_before_error=2)
            seen = []
            for ab in session:
                seen.append(ab.batch.to_pylist())
            ok("produce_error_mid_stream should raise", False, True)
        except RpcError as e:
            ok("produce_error_mid_stream raises RuntimeError", e.error_type, "RuntimeError")

        # Error on init
        try:
            session = proxy.produce_error_on_init()
            # Iteration may or may not raise depending on when init exception is surfaced
            list(session)
            ok("produce_error_on_init should raise", False, True)
        except RpcError as e:
            ok("produce_error_on_init raises", e.error_type, "RuntimeError")

        # Producer with header
        session = proxy.produce_with_header(count=2)
        header = session.typed_header(ConformanceHeader)
        ok("produce_with_header header", header.total_expected, 2)
        rows = list(session)
        ok("produce_with_header count", len(rows), 2)

        # Exchange
        session = proxy.exchange_scale(factor=2.0)
        with session:
            input_batch = AnnotatedBatch.from_pydict({"value": [1.0, 2.0, 3.0]})
            out = session.exchange(input_batch)
            ok("exchange_scale values", out.batch.column("value").to_pylist(), [2.0, 4.0, 6.0])

        # Accumulating exchange
        session = proxy.exchange_accumulate()
        with session:
            out1 = session.exchange(AnnotatedBatch.from_pydict({"value": [1.0, 2.0]}))
            out2 = session.exchange(AnnotatedBatch.from_pydict({"value": [3.0]}))
            ok("exchange_accumulate sum", out2.batch.column("running_sum")[0].as_py(), 6.0)
            ok("exchange_accumulate count", out2.batch.column("exchange_count")[0].as_py(), 2)

        return 0
    except Exception as e:
        traceback.print_exc()
        return 1
    finally:
        transport.close()


if __name__ == "__main__":
    raise SystemExit(main())
