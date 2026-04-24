"""Smoke test: benchmark worker boots and its 6 methods dispatch correctly.

Actual throughput measurement is driven by the vgi-rpc reference's
``test_benchmark_comparison.py`` harness — this file only proves the worker
is wire-compatible so the comparison can run.
"""
from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path
from typing import Protocol

import httpx

from vgi_rpc.http import http_connect
from vgi_rpc.rpc import AnnotatedBatch, SubprocessTransport, _RpcProxy, Stream, StreamState

WORKER = os.environ.get(
    "JAVA_BENCHMARK_WORKER",
    str(Path(__file__).parent / "benchmark-worker/build/install/benchmark-worker/bin/benchmark-worker"),
)


# Python-side protocol shape that matches the Java benchmark fixture.
from enum import Enum


class Color(Enum):
    RED = "RED"
    GREEN = "GREEN"
    BLUE = "BLUE"


class BenchmarkService(Protocol):
    def noop(self) -> None: ...
    def add(self, a: float, b: float) -> float: ...
    def greet(self, name: str) -> str: ...
    def roundtrip_types(self, color: Color, mapping: dict[str, int], tags: list[int]) -> str: ...
    def generate(self, count: int) -> Stream[StreamState]: ...
    def transform(self, factor: float) -> Stream[StreamState]: ...


def test_benchmark_worker_pipe_dispatches_all_methods():
    t = SubprocessTransport([WORKER])
    try:
        proxy = _RpcProxy(BenchmarkService, t)
        assert proxy.noop() is None
        assert proxy.add(a=1.5, b=2.5) == 4.0
        assert proxy.greet(name="Java") == "Hello, Java!"
        reply = proxy.roundtrip_types(color=Color.GREEN, mapping={"b": 2, "a": 1}, tags=[3, 1, 2])
        assert reply == "GREEN:true:{'a': 1, 'b': 2}:[1, 2, 3]"
        rows = [ab.batch.to_pylist()[0] for ab in proxy.generate(count=3)]
        assert rows == [{"i": 0, "value": 0}, {"i": 1, "value": 2}, {"i": 2, "value": 4}]
        with proxy.transform(factor=2.0) as s:
            out = s.exchange(AnnotatedBatch.from_pydict({"value": [1.0, 2.0]}))
            assert out.batch.column("value").to_pylist() == [2.0, 4.0]
    finally:
        t.close()


def test_benchmark_worker_http_dispatches_core_methods():
    proc = subprocess.Popen([WORKER, "--http"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        line = proc.stdout.readline().decode().strip()
        port = int(line.split(":", 1)[1])
        # Wait for HTTP to come up
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            try:
                httpx.get(f"http://127.0.0.1:{port}/health", timeout=2)
                break
            except (httpx.ConnectError, httpx.ConnectTimeout):
                time.sleep(0.1)

        with http_connect(BenchmarkService, f"http://127.0.0.1:{port}", compression_level=None) as proxy:
            assert proxy.noop() is None
            assert proxy.add(a=1.0, b=2.0) == 3.0
            assert proxy.greet(name="http") == "Hello, http!"
    finally:
        proc.terminate()
        proc.wait(timeout=5)
