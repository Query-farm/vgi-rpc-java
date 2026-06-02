"""Verify JWT/JWKS auth on the Java HTTP worker.

Boots an in-process JWKS endpoint (stdlib http.server) with a freshly
generated RSA key, mints a signed token with authlib, then spawns the Java
worker with ``--auth-jwt`` and asserts:

* Valid tokens let calls through.
* Expired tokens are rejected (401).
* Tokens for a different issuer/audience are rejected (401).

Requires the conformance environment's authlib install.
"""
from __future__ import annotations

import json
import os
import socket
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import httpx
import pytest

from vgi_rpc.conformance import ConformanceService
from vgi_rpc.http import http_connect

pytest.importorskip("joserfc")
from joserfc import jwt as jose_jwt  # noqa: E402
from joserfc.jwk import generate_key  # noqa: E402

WORKER = os.environ.get(
    "JAVA_CONFORMANCE_WORKER",
    str(Path(__file__).parent.parent / "conformance-worker/build/install/conformance-worker/bin/conformance-worker"),
)

ISSUER = "https://issuer.example"
AUDIENCE = "my-api"


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class _JwksServer:
    """Tiny stdlib HTTP server serving /jwks.json for the lifetime of a test."""

    def __init__(self) -> None:
        self.key = generate_key("RSA", parameters={"kid": "test-kid"}, private=True)
        pub = self.key.as_dict(private=False)
        jwks_doc = {"keys": [pub]}
        body = json.dumps(jwks_doc).encode()

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self_):  # noqa: N802
                self_.send_response(200)
                self_.send_header("Content-Type", "application/json")
                self_.send_header("Content-Length", str(len(body)))
                self_.end_headers()
                self_.wfile.write(body)

            def log_message(self_, *a, **kw):  # noqa: N802, D401
                """Silence the default stderr spam."""

        self.port = _free_port()
        self.server = HTTPServer(("127.0.0.1", self.port), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def mint(self, *, iss: str = ISSUER, aud: str = AUDIENCE, sub: str = "alice",
             exp_delta: int = 300) -> str:
        now = int(time.time())
        claims = {"iss": iss, "aud": aud, "sub": sub, "iat": now, "exp": now + exp_delta}
        header = {"alg": "RS256", "kid": self.key.kid}
        return jose_jwt.encode(header, claims, self.key)

    def shutdown(self) -> None:
        self.server.shutdown()
        self.server.server_close()


def _wait_for_http(port: int, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            httpx.get(f"http://127.0.0.1:{port}/health", timeout=5.0)
            return
        except (httpx.ConnectError, httpx.ConnectTimeout):
            time.sleep(0.1)
    raise TimeoutError(f"HTTP server on port {port} did not start within {timeout}s")


def _spawn_jwt_worker(jwks_uri: str) -> tuple[subprocess.Popen, int]:
    spec = f"issuer={ISSUER},audience={AUDIENCE},jwks={jwks_uri}"
    proc = subprocess.Popen(
        [WORKER, "--http", "--auth-jwt", spec],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    assert proc.stdout is not None
    line = proc.stdout.readline().decode().strip()
    assert line.startswith("PORT:"), f"expected PORT:<n>, got {line!r}"
    port = int(line.split(":", 1)[1])
    _wait_for_http(port)
    return proc, port


@pytest.fixture()
def jwks() -> "_JwksServer":
    server = _JwksServer()
    try:
        yield server
    finally:
        server.shutdown()


def test_valid_jwt_authenticates(jwks):
    proc, port = _spawn_jwt_worker(f"http://127.0.0.1:{jwks.port}/jwks.json")
    try:
        token = jwks.mint()
        client = httpx.Client(base_url=f"http://127.0.0.1:{port}",
                               headers={"Authorization": f"Bearer {token}"})
        try:
            with http_connect(ConformanceService, client=client, compression_level=None) as proxy:
                assert proxy.echo_string(value="hi") == "hi"
        finally:
            client.close()
    finally:
        proc.terminate(); proc.wait(timeout=5)


def test_expired_jwt_rejected(jwks):
    proc, port = _spawn_jwt_worker(f"http://127.0.0.1:{jwks.port}/jwks.json")
    try:
        token = jwks.mint(exp_delta=-60)  # already expired
        r = httpx.post(f"http://127.0.0.1:{port}/echo_string",
                       content=b"",
                       headers={"Content-Type": "application/vnd.apache.arrow.stream",
                                "Authorization": f"Bearer {token}"})
        assert r.status_code == 401, r.status_code
    finally:
        proc.terminate(); proc.wait(timeout=5)


def test_wrong_audience_rejected(jwks):
    proc, port = _spawn_jwt_worker(f"http://127.0.0.1:{jwks.port}/jwks.json")
    try:
        token = jwks.mint(aud="some-other-api")
        r = httpx.post(f"http://127.0.0.1:{port}/echo_string",
                       content=b"",
                       headers={"Content-Type": "application/vnd.apache.arrow.stream",
                                "Authorization": f"Bearer {token}"})
        assert r.status_code == 401, r.status_code
    finally:
        proc.terminate(); proc.wait(timeout=5)
