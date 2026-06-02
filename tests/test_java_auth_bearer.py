"""Verify Bearer-token auth on the Java HTTP worker.

Starts the worker with a static token map via ``--auth-bearer`` and asserts:

* Requests without an ``Authorization`` header receive 401.
* Wrong-token requests receive 401.
* Valid-token requests succeed end-to-end.
"""
from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path

import httpx
import pytest

from vgi_rpc.conformance import ConformanceService
from vgi_rpc.http import http_connect
from vgi_rpc.rpc import RpcError

WORKER = os.environ.get(
    "JAVA_CONFORMANCE_WORKER",
    str(Path(__file__).parent.parent / "conformance-worker/build/install/conformance-worker/bin/conformance-worker"),
)


def _wait_for_http(port: int, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            httpx.get(f"http://127.0.0.1:{port}/health", timeout=5.0)
            return
        except (httpx.ConnectError, httpx.ConnectTimeout):
            time.sleep(0.1)
    raise TimeoutError(f"HTTP server on port {port} did not start within {timeout}s")


def _spawn_bearer(token_spec: str) -> tuple[subprocess.Popen, int]:
    proc = subprocess.Popen(
        [WORKER, "--http", "--auth-bearer", token_spec],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert proc.stdout is not None
    line = proc.stdout.readline().decode().strip()
    assert line.startswith("PORT:"), f"expected PORT:<n>, got {line!r}"
    port = int(line.split(":", 1)[1])
    _wait_for_http(port)
    return proc, port


def test_bearer_no_header_is_unauthorized():
    proc, port = _spawn_bearer("secret=alice")
    try:
        r = httpx.post(
            f"http://127.0.0.1:{port}/echo_string",
            content=b"",
            headers={"Content-Type": "application/vnd.apache.arrow.stream"},
        )
        assert r.status_code == 401, r.status_code
        assert "www-authenticate" in {k.lower() for k in r.headers}
    finally:
        proc.terminate()
        proc.wait(timeout=5)


def test_bearer_wrong_token_is_unauthorized():
    proc, port = _spawn_bearer("secret=alice")
    try:
        r = httpx.post(
            f"http://127.0.0.1:{port}/echo_string",
            content=b"",
            headers={
                "Content-Type": "application/vnd.apache.arrow.stream",
                "Authorization": "Bearer not-the-token",
            },
        )
        assert r.status_code == 401, r.status_code
    finally:
        proc.terminate()
        proc.wait(timeout=5)


def test_bearer_valid_token_succeeds():
    proc, port = _spawn_bearer("secret=alice,other=bob")
    try:
        client = httpx.Client(
            base_url=f"http://127.0.0.1:{port}",
            headers={"Authorization": "Bearer secret"},
        )
        try:
            with http_connect(ConformanceService, client=client, compression_level=None) as proxy:
                assert proxy.echo_string(value="hello") == "hello"
                assert proxy.echo_int(value=42) == 42
        finally:
            client.close()
    finally:
        proc.terminate()
        proc.wait(timeout=5)
