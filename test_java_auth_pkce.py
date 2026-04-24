"""End-to-end test of the Java PKCE flow.

Spins up:

1. A mock OIDC provider (stdlib http.server) that serves
   ``/.well-known/openid-configuration``, ``/authorize`` (immediately
   redirects back to the Java callback with a fake auth code), ``/token``
   (exchanges the code for a signed id_token), and ``/jwks.json`` (public
   key).

2. The Java conformance worker with ``--auth-pkce`` configured to talk
   to that OIDC provider, using a fixed session / auth HMAC key the test
   also knows.

The test then drives the full browser handshake with httpx and asserts
that the final authenticated request succeeds.
"""
from __future__ import annotations

import base64
import hashlib
import hmac as hmac_mod
import json
import os
import socket
import struct
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import httpx
import pytest

from vgi_rpc.conformance import ConformanceService
from vgi_rpc.http import http_connect

pytest.importorskip("joserfc")
from joserfc import jwt as jose_jwt  # noqa: E402
from joserfc.jwk import generate_key  # noqa: E402

WORKER = os.environ.get(
    "JAVA_CONFORMANCE_WORKER",
    str(Path(__file__).parent / "conformance-worker/build/install/conformance-worker/bin/conformance-worker"),
)

CLIENT_ID = "test-client"
AUDIENCE = "my-api"
# Fixed keys so the pytest process can reconstruct / inspect cookies signed
# with the same HMAC as the Java worker.
SESSION_KEY = bytes(range(32))
AUTH_KEY = bytes(range(32, 64))


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _hex(b: bytes) -> str:
    return b.hex()


class _MockOidc:
    """Minimal OIDC provider for PKCE: discovery + authorize + token + jwks."""

    def __init__(self, issuer_port: int, callback_url: str) -> None:
        self.port = issuer_port
        self.issuer = f"http://127.0.0.1:{issuer_port}"
        self.callback_url = callback_url
        self.key = generate_key("RSA", parameters={"kid": "pkce-kid"}, private=True)
        pub_doc = self.key.as_dict(private=False)
        jwks_body = json.dumps({"keys": [pub_doc]}).encode()
        metadata = {
            "issuer": self.issuer,
            "authorization_endpoint": f"{self.issuer}/authorize",
            "token_endpoint": f"{self.issuer}/token",
            "jwks_uri": f"{self.issuer}/jwks.json",
        }
        metadata_body = json.dumps(metadata).encode()
        signer = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self_, *a, **kw):  # noqa: N802
                pass

            def do_GET(self_):  # noqa: N802
                parsed = urlparse(self_.path)
                path = parsed.path
                if path == "/.well-known/openid-configuration":
                    self_.send_response(200)
                    self_.send_header("Content-Type", "application/json")
                    self_.send_header("Content-Length", str(len(metadata_body)))
                    self_.end_headers()
                    self_.wfile.write(metadata_body)
                    return
                if path == "/jwks.json":
                    self_.send_response(200)
                    self_.send_header("Content-Type", "application/json")
                    self_.send_header("Content-Length", str(len(jwks_body)))
                    self_.end_headers()
                    self_.wfile.write(jwks_body)
                    return
                if path == "/authorize":
                    # Immediately redirect back with a fake code + echoed state
                    qs = parse_qs(parsed.query)
                    state = qs.get("state", [""])[0]
                    redir = f"{signer.callback_url}?code=FAKE-AUTH-CODE&state={state}"
                    self_.send_response(302)
                    self_.send_header("Location", redir)
                    self_.end_headers()
                    return
                self_.send_response(404)
                self_.end_headers()

            def do_POST(self_):  # noqa: N802
                parsed = urlparse(self_.path)
                if parsed.path != "/token":
                    self_.send_response(404); self_.end_headers(); return
                n = int(self_.headers.get("Content-Length", "0"))
                body = self_.rfile.read(n).decode()
                params = parse_qs(body)
                if params.get("grant_type", [""])[0] != "authorization_code":
                    self_.send_response(400); self_.end_headers(); return
                # Issue an id_token.
                now = int(time.time())
                claims = {"iss": signer.issuer, "aud": AUDIENCE, "sub": "alice",
                          "iat": now, "exp": now + 300}
                header = {"alg": "RS256", "kid": signer.key.kid}
                id_token = jose_jwt.encode(header, claims, signer.key)
                resp = json.dumps({
                    "id_token": id_token,
                    "access_token": id_token,
                    "token_type": "Bearer",
                    "expires_in": 300,
                }).encode()
                self_.send_response(200)
                self_.send_header("Content-Type", "application/json")
                self_.send_header("Content-Length", str(len(resp)))
                self_.end_headers()
                self_.wfile.write(resp)

        self.server = HTTPServer(("127.0.0.1", issuer_port), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def shutdown(self) -> None:
        self.server.shutdown()
        self.server.server_close()


def _urlsafe_b64(data: bytes) -> bytes:
    return base64.urlsafe_b64encode(data).rstrip(b"=")


def _build_session_cookie(verifier: str, state: str, return_to: str, key: bytes) -> str:
    """Reconstruct Java's SignedCookie.sign(TimestampedPayload.pack(json)) in Python."""
    payload = {"verifier": verifier, "state": state, "return_to": return_to}
    body = json.dumps(payload).encode()
    now = int(time.time())
    ts_prefix = struct.pack("<Q", now)
    wrapped = ts_prefix + body
    payload_b64 = _urlsafe_b64(wrapped)
    mac = hmac_mod.new(key, payload_b64, hashlib.sha256).digest()
    mac_b64 = _urlsafe_b64(mac).decode()
    return payload_b64.decode() + "." + mac_b64


def _wait_for_http(port: int, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            httpx.get(f"http://127.0.0.1:{port}/health", timeout=2)
            return
        except (httpx.ConnectError, httpx.ConnectTimeout):
            time.sleep(0.1)
    raise TimeoutError(f"worker on port {port} did not start")


@pytest.fixture
def pkce_env():
    # Start mock OIDC first, then the Java worker pointing at it.
    issuer_port = _free_port()
    # The Java worker chooses its own port so the redirect_uri comes from that.
    # Reserve a port for the worker up front so the mock provider can target it.
    worker_port = _free_port()
    callback_url = f"http://127.0.0.1:{worker_port}/_oauth/callback"
    oidc = _MockOidc(issuer_port, callback_url)

    spec = ",".join([
        f"client_id={CLIENT_ID}",
        f"redirect_uri={callback_url}",
        f"issuer={oidc.issuer}",
        f"audience={AUDIENCE}",
        f"session_key_hex={_hex(SESSION_KEY)}",
        f"auth_key_hex={_hex(AUTH_KEY)}",
    ])
    # --http-port so the worker binds the predetermined port — but we don't have
    # that flag, so we spawn the worker and read whatever it binds, then the test
    # uses the actual port for callback-url construction when re-running.
    # Workaround: spawn worker, read its port, point the mock at that port.
    oidc.shutdown()
    oidc = _MockOidc(issuer_port, f"http://127.0.0.1:{worker_port}/_oauth/callback")

    # Re-bind the worker port explicitly via Java's --http doesn't support that;
    # instead we spawn the worker, read its port, and restart the mock provider
    # with the real callback URL.
    proc = subprocess.Popen([WORKER, "--http", "--auth-pkce", spec],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        line = proc.stdout.readline().decode().strip()
        port = int(line.split(":", 1)[1])
        _wait_for_http(port)
        # Rebuild the mock so its /authorize redirects to the worker's real port.
        oidc.shutdown()
        oidc = _MockOidc(issuer_port, f"http://127.0.0.1:{port}/_oauth/callback")
        yield {
            "worker_port": port,
            "oidc": oidc,
        }
    finally:
        proc.terminate(); proc.wait(timeout=5)
        oidc.shutdown()


def test_pkce_full_flow(pkce_env):
    """Simulate the browser round-trip: build session cookie, post to callback,
    receive auth cookie, call an RPC method with the cookie set."""
    oidc = pkce_env["oidc"]
    worker_port = pkce_env["worker_port"]

    # Sync with the Java server's expected session-cookie shape.
    verifier = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"  # RFC 7636 Appendix B
    state = "test-state-nonce"
    session_cookie = _build_session_cookie(verifier, state, "/", SESSION_KEY)

    client = httpx.Client(follow_redirects=False)
    # Step 1: The callback with the session cookie + fake code + matching state.
    r = client.get(
        f"http://127.0.0.1:{worker_port}/_oauth/callback?code=FAKE-AUTH-CODE&state={state}",
        cookies={"_vgi_oauth_session": session_cookie},
    )
    assert r.status_code == 302, f"expected redirect, got {r.status_code}: {r.text[:400]}"
    # Extract the auth cookie the Java side set.
    auth_cookie = r.cookies.get("_vgi_auth")
    assert auth_cookie, f"no auth cookie set; response headers: {r.headers}"

    # Step 2: Authenticated RPC call.
    client2 = httpx.Client(base_url=f"http://127.0.0.1:{worker_port}",
                             cookies={"_vgi_auth": auth_cookie})
    try:
        with http_connect(ConformanceService, client=client2, compression_level=None) as proxy:
            assert proxy.echo_string(value="ok") == "ok"
    finally:
        client2.close()
    client.close()


def test_pkce_rejects_unauthenticated(pkce_env):
    """Without any cookies, requests to RPC methods should get 401."""
    worker_port = pkce_env["worker_port"]
    r = httpx.post(f"http://127.0.0.1:{worker_port}/echo_string",
                   content=b"",
                   headers={"Content-Type": "application/vnd.apache.arrow.stream"})
    assert r.status_code == 401, r.status_code
