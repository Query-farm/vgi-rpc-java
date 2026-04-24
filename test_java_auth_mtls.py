"""Verify XFCC mTLS header auth on the Java HTTP worker."""
from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path

import httpx
import pytest

from vgi_rpc.conformance import ConformanceService
from vgi_rpc.http import http_connect

WORKER = os.environ.get(
    "JAVA_CONFORMANCE_WORKER",
    str(Path(__file__).parent / "conformance-worker/build/install/conformance-worker/bin/conformance-worker"),
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


def _spawn_mtls() -> tuple[subprocess.Popen, int]:
    proc = subprocess.Popen(
        [WORKER, "--http", "--auth-mtls", "xfcc"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    assert proc.stdout is not None
    line = proc.stdout.readline().decode().strip()
    port = int(line.split(":", 1)[1])
    _wait_for_http(port)
    return proc, port


def test_mtls_no_header_is_unauthorized():
    proc, port = _spawn_mtls()
    try:
        r = httpx.post(
            f"http://127.0.0.1:{port}/echo_string",
            content=b"",
            headers={"Content-Type": "application/vnd.apache.arrow.stream"},
        )
        assert r.status_code == 401, r.status_code
    finally:
        proc.terminate()
        proc.wait(timeout=5)


def test_mtls_xfcc_header_authenticates():
    proc, port = _spawn_mtls()
    try:
        client = httpx.Client(
            base_url=f"http://127.0.0.1:{port}",
            headers={"x-forwarded-client-cert": 'Hash=abc;Subject="CN=alice,O=acme"'},
        )
        try:
            with http_connect(ConformanceService, client=client, compression_level=None) as proxy:
                assert proxy.echo_string(value="x") == "x"
        finally:
            client.close()
    finally:
        proc.terminate()
        proc.wait(timeout=5)
