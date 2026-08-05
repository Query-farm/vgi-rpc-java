"""Run the reference pytest conformance suite against the Java worker.

Mirrors test_go_conformance.py from vgi-rpc-go, parametrising by transport
(pipe / subprocess / http / unix / tcp) so the entire wire surface is exercised.
"""
from __future__ import annotations

import contextlib
import os
import re
import socket
import subprocess
import tempfile
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import httpx2 as httpx  # reference renamed httpx -> httpx2 in vgi-rpc 0.40.0
import pytest

from vgi_rpc.conformance import ConformanceService
from vgi_rpc.http import http_connect
from vgi_rpc.log import Message
from vgi_rpc.rpc import ShmPipeTransport, SubprocessTransport, _RpcProxy, tcp_connect, unix_connect
from vgi_rpc.shm import ShmSegment

JAVA_WORKER = os.environ.get(
    "JAVA_CONFORMANCE_WORKER",
    str(Path(__file__).parent.parent / "conformance-worker/build/install/conformance-worker/bin/conformance-worker"),
)

# Size of the per-connection POSIX shm segment for the "subprocess_shm"
# transport. Large enough that conformance batches ride the side-channel;
# anything that overflows falls back to inline transfer (never an error).
SHM_SEGMENT_BYTES = 128 * 1024 * 1024


@pytest.fixture(scope="session")
def java_transport() -> Iterator[SubprocessTransport]:
    transport = SubprocessTransport([JAVA_WORKER])
    yield transport
    transport.close()


@pytest.fixture(scope="session")
def conformance_describe() -> Iterator[Any]:
    """Real ``__describe__`` against the Java worker for TestDescribeConformance.

    The upstream suite requires the host harness to supply the worker's
    ``ServiceDescription`` (rather than a throwaway in-process Python server), so
    introspection is validated against the actual Java implementation. Uses its
    own subprocess transport to stay isolated from the shared ``java_transport``
    stream state. The describe payload is transport-independent server-side, so a
    single transport exercises ``Introspect``/``serveDescribe`` fully.
    """
    from vgi_rpc.introspect import introspect

    transport = SubprocessTransport([JAVA_WORKER])
    try:
        yield introspect(transport)
    finally:
        transport.close()


def _wait_for_http(port: int, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            _ = httpx.get(f"http://127.0.0.1:{port}/health", timeout=5.0)
            return
        except (httpx.ConnectError, httpx.ConnectTimeout):
            time.sleep(0.1)
    raise TimeoutError(f"HTTP server on port {port} did not start within {timeout}s")


@pytest.fixture(scope="session")
def java_http_port() -> Iterator[int]:
    proc = subprocess.Popen([JAVA_WORKER, "--http"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        assert proc.stdout is not None
        line = proc.stdout.readline().decode().strip()
        assert line.startswith("PORT:"), f"Expected PORT:<n>, got: {line!r}"
        port = int(line.split(":", 1)[1])
        _wait_for_http(port)
        yield port
    finally:
        proc.terminate()
        proc.wait(timeout=5)


def _short_unix_path(name: str) -> str:
    fd, path = tempfile.mkstemp(prefix=f"vgi-java-{name}-", suffix=".sock", dir="/tmp")
    os.close(fd)
    os.unlink(path)
    return path


def _wait_for_unix(path: str, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                sock.connect(path)
                return
            finally:
                sock.close()
        except (FileNotFoundError, ConnectionRefusedError, OSError):
            time.sleep(0.1)
    raise TimeoutError(f"Unix socket at {path} did not start within {timeout}s")


@pytest.fixture(scope="session")
def java_unix_path() -> Iterator[str]:
    path = _short_unix_path("conf")
    proc = subprocess.Popen([JAVA_WORKER, "--unix", path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        assert proc.stdout is not None
        line = proc.stdout.readline().decode().strip()
        assert line == f"UNIX:{path}", f"Expected UNIX:{path}, got: {line!r}"
        _wait_for_unix(path)
        yield path
    finally:
        proc.terminate()
        proc.wait(timeout=5)


def _wait_for_tcp(host: str, port: int, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            sock = socket.create_connection((host, port), timeout=1.0)
            sock.close()
            return
        except (ConnectionRefusedError, OSError):
            time.sleep(0.1)
    raise TimeoutError(f"TCP {host}:{port} did not start within {timeout}s")


@pytest.fixture(scope="session")
def java_tcp_addr() -> Iterator[tuple[str, int]]:
    # port 0 ⇒ the worker asks the OS for a free loopback port and reports it
    # back on the TCP:<host>:<port> discovery line.
    proc = subprocess.Popen(
        [JAVA_WORKER, "--tcp", "127.0.0.1:0"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        assert proc.stdout is not None
        line = proc.stdout.readline().decode().strip()
        assert line.startswith("TCP:"), f"Expected TCP:<host>:<port>, got: {line!r}"
        host, _, port_part = line[len("TCP:") :].rpartition(":")
        port = int(port_part)
        _wait_for_tcp(host, port)
        yield (host, port)
    finally:
        proc.terminate()
        proc.wait(timeout=5)


@pytest.fixture(scope="session")
def conformance_http_port(java_http_port: int) -> int:
    """Reuse the no-auth HTTP worker for the TestHealth conformance contract."""
    return java_http_port


@pytest.fixture(scope="session")
def conformance_http_auth_port() -> Iterator[int]:
    """Spawn a reject-all HTTP worker, so every RPC POST returns 401."""
    proc = subprocess.Popen(
        [JAVA_WORKER, "--http-auth"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        assert proc.stdout is not None
        line = proc.stdout.readline().decode().strip()
        assert line.startswith("PORT:"), f"Expected PORT:<n>, got: {line!r}"
        port = int(line.split(":", 1)[1])
        _wait_for_http(port)
        yield port
    finally:
        proc.terminate()
        proc.wait(timeout=5)


@pytest.fixture(scope="session")
def conformance_http_auth_reason_port(conformance_http_auth_port: int) -> int:
    """Port of a worker that honours ``X-Conformance-Auth-Reason``.

    Backs the shared ``TestUnauthorized`` reason-code tests. Membership in
    the closed set is not enough on its own — a server answering every 401
    with ``unauthorized`` satisfies that. These tests prove the codes are
    *discriminated*, which is what makes them worth branching on.

    ``--http-auth`` already reads the header, so this is the same worker
    under the name the shared suite looks up.
    """
    return conformance_http_auth_port


@pytest.fixture(scope="session")
def conformance_http_no_compression_port() -> Iterator[int]:
    """Spawn an HTTP worker with response compression disabled.

    Backs the shared ``test_empty_advertisement_means_never_compressed``
    case.  It needs its own worker because the state under test is a
    *server configuration* -- "I can produce no codecs" -- which no client
    request can induce.  ``identity`` covers the client-side ability to
    demand an uncompressed body; only a server booted this way emits the
    present-but-empty ``VGI-Supported-Encodings`` that distinguishes
    "speaks no compression" from an absent header on a legacy server.
    """
    proc = subprocess.Popen(
        [JAVA_WORKER, "--http", "--no-compression"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        assert proc.stdout is not None
        line = proc.stdout.readline().decode().strip()
        assert line.startswith("PORT:"), f"Expected PORT:<n>, got: {line!r}"
        port = int(line.split(":", 1)[1])
        _wait_for_http(port)
        yield port
    finally:
        proc.terminate()
        proc.wait(timeout=5)


@pytest.fixture(scope="session")
def conformance_http_strict_cap_port() -> Iterator[int]:
    """Spawn an HTTP worker with strict response caps (1 MiB) for cap-overshoot tests."""
    proc = subprocess.Popen(
        [JAVA_WORKER, "--http", "--strict"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        assert proc.stdout is not None
        line = proc.stdout.readline().decode().strip()
        assert line.startswith("PORT:"), f"Expected PORT:<n>, got: {line!r}"
        port = int(line.split(":", 1)[1])
        _wait_for_http(port)
        yield port
    finally:
        proc.terminate()
        proc.wait(timeout=5)


def _start_http_worker(*extra_args: str) -> Iterator[int]:
    """Spawn a Java HTTP conformance worker and yield the port it reports."""
    proc = subprocess.Popen(
        [JAVA_WORKER, *extra_args],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        assert proc.stdout is not None
        line = proc.stdout.readline().decode().strip()
        assert line.startswith("PORT:"), f"Expected PORT:<n>, got: {line!r}"
        port = int(line.split(":", 1)[1])
        _wait_for_http(port)
        yield port
    finally:
        proc.terminate()
        proc.wait(timeout=5)


@pytest.fixture(scope="session")
def conformance_http_cold_call_cache_port() -> Iterator[int]:
    """Spawn an HTTP worker with the per-process call-state cache disabled.

    Backs the shared ``TestColdCallStateCache`` group, which pins the rule
    that a client echoes the call token on **every** continuation. With the
    cache warm a client that never echoes still works, and only breaks once
    a continuation lands on a process that never saw the stream's ``/init``
    — a restarted worker, an evicted entry, a load-balanced relay. Booting
    with the cache off turns that load-dependent bug into a deterministic
    one: every turn takes the miss path.
    """
    yield from _start_http_worker("--http", "--no-call-state-cache")


@pytest.fixture(scope="session")
def conformance_http_access_log(
    tmp_path_factory: pytest.TempPathFactory,
) -> Iterator[tuple[int, Path]]:
    """Spawn an HTTP worker writing a JSONL access log, yielding ``(port, path)``.

    Backs the shared ``TestRequestId`` correlation case, which is the one
    assertion the ``X-Request-ID`` field exists for: an id that appears on the
    response but not in the log, or differs between them, looks like a working
    trail right up to the moment somebody follows it. Checking that needs to
    read back what the server logged for a request the suite itself made, which
    no amount of poking at the wire substitutes for.

    Its own worker, because ``--access-log`` appends for the process's whole
    life and the shared one is used by every other HTTP group.
    """
    log_path = tmp_path_factory.mktemp("accesslog") / "conformance.jsonl"
    gen = _start_http_worker("--http", "--access-log", str(log_path))
    port = next(gen)
    try:
        yield port, log_path
    finally:
        next(gen, None)


@pytest.fixture(scope="session")
def conformance_http_capped_access_log(
    tmp_path_factory: pytest.TempPathFactory,
) -> Iterator[tuple[int, Path]]:
    """Spawn an HTTP worker with strict response caps *and* a JSONL access log.

    Backs ``TestHttpResponseCapAccessLog``. Neither existing worker can: the
    strict-cap one writes no log, and the access-log one has no cap to
    overshoot, so the one state where the two interact -- a response the server
    threw away for being oversize -- is reachable from neither. Its own process
    for the same reason ``conformance_http_access_log`` has one: ``--access-log``
    appends for the process's whole life.
    """
    log_path = tmp_path_factory.mktemp("capaccesslog") / "conformance.jsonl"
    gen = _start_http_worker("--http", "--strict", "--access-log", str(log_path))
    port = next(gen)
    try:
        yield port, log_path
    finally:
        next(gen, None)


@pytest.fixture(scope="session")
def conformance_http_introspect_port() -> Iterator[int]:
    """Spawn an HTTP worker with token introspection enabled.

    Backs the shared ``TestTokenIntrospection`` group. It needs its own worker
    because the endpoint resolves nothing unless explicitly enabled -- which
    ``TestTokenIntrospectionOffMode`` asserts against the default one. The
    worker is configured with the exact introspector / subject / JWS-trap
    constants the shared suite posts; anything else reads as "did not resolve".
    """
    yield from _start_http_worker("--http", "--introspect")


@pytest.fixture(scope="session")
def conformance_http_cors_port(conformance_fake_storage: str) -> Iterator[int]:
    """Spawn an HTTP worker that grants browser access to one fixed origin.

    Backs the shared ``TestCors`` group. It needs its own worker because CORS
    is opt-in and the default one must stay header-free -- ``TestCorsOffMode``
    runs against that one and checks exactly that. The origin is the constant
    the shared suite preflights with; a mismatch reads as "origin refused".

    Storage mode is deliberate: the derived exposure check can only catch a
    missing entry for a header the worker actually advertises, so a *plain*
    worker here would silently skip the conditional half of the capability
    set -- the size caps and the upload-URL trio -- which are exactly the
    exposures a port is most likely to miss.
    """
    yield from _start_http_worker(
        "--http",
        "--fake-storage",
        conformance_fake_storage,
        "--cors-origin",
        "https://conformance.example",
    )


# ---------------------------------------------------------------------------
# Sticky failure-path fixtures (upstream TestSticky; see
# vgi-rpc docs/sticky-sessions-spec.md §9.1)
# ---------------------------------------------------------------------------

# Shared AEAD key for the peer pair. Both workers can open each other's
# session tokens, which is the point: the rejection under test has to come
# from the server_id comparison, not from a decrypt failure.
_STICKY_PEER_TOKEN_KEY = "5f" * 32


@pytest.fixture(scope="session")
def conformance_http_sticky_short_ttl_port() -> Iterator[int]:
    """A sticky worker whose default session TTL is short enough to outwait.

    Backs ``TestSticky::test_expired_session_surfaces_session_lost``; the
    main worker's 300s default is not something a test can sit out.
    """
    yield from _start_http_worker("--http", "--sticky-ttl", "1")


@pytest.fixture(scope="session")
def conformance_http_sticky_peer_ports() -> Iterator[tuple[int, int]]:
    """Two sticky workers sharing one AEAD key, for the wrong-worker check.

    Backs ``TestSticky::test_token_from_other_worker_rejected``. RpcServer
    mints a random server_id per process, so the two peers differ without
    any extra flag — which is what makes the shared key safe to use here.
    """
    gen_a = _start_http_worker("--http", "--token-key", _STICKY_PEER_TOKEN_KEY)
    gen_b = _start_http_worker("--http", "--token-key", _STICKY_PEER_TOKEN_KEY)
    port_a = next(gen_a)
    try:
        port_b = next(gen_b)
        try:
            yield port_a, port_b
        finally:
            next(gen_b, None)
    finally:
        next(gen_a, None)


@pytest.fixture(scope="session")
def conformance_http_sticky_auth_port() -> Iterator[int]:
    """A sticky worker that authenticates the ``X-Conformance-Principal`` header.

    Backs ``TestSticky::test_cross_principal_replay_rejected``, which needs
    one worker reachable as two identities.
    """
    yield from _start_http_worker("--http", "--sticky-auth")


@pytest.fixture(scope="session")
def proof_worker_factory() -> Iterator[Callable[..., Any]]:
    """Spawn Java workers gated on proxy proof, for the shared TestProxyProof group.

    The shared suite owns the matrix; this only has to know how to start one
    worker for a given configuration.
    """
    from vgi_rpc.conformance.proof_harness import ProofWorker, ProofWorkerConfig

    @contextlib.contextmanager
    def spawn(config: ProofWorkerConfig) -> Iterator[ProofWorker]:
        args = [
            "--http-proof",
            "--proof-mode",
            config.mode,
            "--proof-origin-id",
            config.origin_id,
            "--proof-secrets",
            config.secrets,
            "--proof-skew",
            str(config.skew_seconds),
        ]
        if not config.replay_cache:
            args.append("--proof-no-replay-cache")
        gen = _start_http_worker(*args)
        port = next(gen)
        try:
            # The Java worker mounts every HTTP route at the root, as its
            # --auth-* modes do; prefix="" makes the shared suite address the
            # real endpoints rather than a path that does not exist.
            yield ProofWorker(port=port, prefix="", config=config)
        finally:
            with contextlib.suppress(StopIteration):
                next(gen)

    yield spawn


@pytest.fixture(scope="session")
def conformance_fake_storage() -> Iterator[str]:
    """Run the in-process Python fake-storage HTTP service."""
    from vgi_rpc.conformance.fake_storage import serve_in_thread

    base_url, shutdown = serve_in_thread()
    try:
        yield base_url
    finally:
        shutdown()


@pytest.fixture(scope="session")
def conformance_http_with_storage_port(conformance_fake_storage: str) -> Iterator[int]:
    """Spawn a Java HTTP worker wired to the fake-storage service (no compression)."""
    proc = subprocess.Popen(
        [JAVA_WORKER, "--http", "--fake-storage", conformance_fake_storage],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        assert proc.stdout is not None
        line = proc.stdout.readline().decode().strip()
        assert line.startswith("PORT:"), f"Expected PORT:<n>, got: {line!r}"
        port = int(line.split(":", 1)[1])
        _wait_for_http(port)
        yield port
    finally:
        proc.terminate()
        proc.wait(timeout=5)


@pytest.fixture(scope="session")
def conformance_http_externalized_cap_port(conformance_fake_storage: str) -> Iterator[int]:
    """Spawn a Java HTTP worker whose *external-channel* cap is the one that bites.

    Backs the shared ``TestExternalizedResponseCap`` group. Two settings make
    this fixture mean what it says:

    * ``--max-externalized-response-bytes`` is tight (64 KiB) so an
      externalised response overshoots it.
    * ``--max-response-bytes`` is deliberately *generous* (8 MiB). An
      externalised payload leaves only a pointer batch on the wire, so the
      body cap must never be what fails here — if it were tight too, the
      group would pass while proving nothing about the external cap.

    ``--externalize-threshold`` stays at its 4 KiB default so a modest
    payload still externalises, which is what lets the under-cap control
    exercise the same channel without tripping the cap.
    """
    proc = subprocess.Popen(
        [
            JAVA_WORKER,
            "--http",
            "--fake-storage",
            conformance_fake_storage,
            "--max-externalized-response-bytes",
            str(64 * 1024),
            "--max-response-bytes",
            str(8 * 1024 * 1024),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        assert proc.stdout is not None
        line = proc.stdout.readline().decode().strip()
        assert line.startswith("PORT:"), f"Expected PORT:<n>, got: {line!r}"
        port = int(line.split(":", 1)[1])
        _wait_for_http(port)
        yield port
    finally:
        proc.terminate()
        proc.wait(timeout=5)


@pytest.fixture(scope="session")
def conformance_http_externalize_always_port(conformance_fake_storage: str) -> Iterator[int]:
    """Spawn a Java HTTP worker that externalizes EVERY non-empty response batch.

    Sets ``--externalize-threshold 1`` so every data-bearing response batch
    routes through the upload-URL pointer flow, while keeping
    ``--max-request-bytes 1048576`` loose enough that normal-sized inline
    *requests* still flow through.  Used as a transport variant in
    ``conformance_conn`` so the entire conformance suite double-checks that
    externalization is observationally indistinguishable from inline
    transmission for every protocol method.
    """
    proc = subprocess.Popen(
        [
            JAVA_WORKER,
            "--http",
            "--fake-storage",
            conformance_fake_storage,
            "--externalize-threshold",
            "1",
            "--max-request-bytes",
            "1048576",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        assert proc.stdout is not None
        line = proc.stdout.readline().decode().strip()
        assert line.startswith("PORT:"), f"Expected PORT:<n>, got: {line!r}"
        port = int(line.split(":", 1)[1])
        _wait_for_http(port)
        yield port
    finally:
        proc.terminate()
        proc.wait(timeout=5)


@pytest.fixture(scope="session")
def conformance_http_with_zstd_storage_port(conformance_fake_storage: str) -> Iterator[int]:
    """Spawn a Java HTTP worker wired to fake-storage with zstd upload compression."""
    proc = subprocess.Popen(
        [
            JAVA_WORKER,
            "--http",
            "--fake-storage",
            conformance_fake_storage,
            "--compression",
            "zstd",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        assert proc.stdout is not None
        line = proc.stdout.readline().decode().strip()
        assert line.startswith("PORT:"), f"Expected PORT:<n>, got: {line!r}"
        port = int(line.split(":", 1)[1])
        _wait_for_http(port)
        yield port
    finally:
        proc.terminate()
        proc.wait(timeout=5)


ConnFactory = Callable[..., contextlib.AbstractContextManager[Any]]


# The transport set is filterable via the CONFORMANCE_TRANSPORTS env var
# (comma-separated) so CI can fan the suite out across parallel jobs by
# transport group — e.g. "pipe,subprocess,unix" for the launcher lanes vs
# "http,http_externalize_always" for the HTTP lane. Unset = all (local default).
_ALL_CONNS = ["pipe", "subprocess", "subprocess_shm", "http", "http_externalize_always", "unix", "tcp"]
_CONN_SEL = os.environ.get("CONFORMANCE_TRANSPORTS")
_CONN_PARAMS = (
    [c for c in _ALL_CONNS if c in {s.strip() for s in _CONN_SEL.split(",")}]
    if _CONN_SEL
    else _ALL_CONNS
)


@pytest.fixture(params=_CONN_PARAMS)
def conformance_conn(
    request: pytest.FixtureRequest,
    java_transport: SubprocessTransport,
    java_http_port: int,
    java_unix_path: str,
    java_tcp_addr: tuple[str, int],
) -> ConnFactory:
    def factory(
        on_log: Callable[[Message], None] | None = None,
    ) -> contextlib.AbstractContextManager[Any]:
        if request.param == "pipe":
            @contextlib.contextmanager
            def _pipe_conn() -> Iterator[_RpcProxy]:
                transport = SubprocessTransport([JAVA_WORKER])
                try:
                    yield _RpcProxy(ConformanceService, transport, on_log)
                finally:
                    transport.close()
            return _pipe_conn()
        elif request.param == "subprocess":
            # Share the session-scoped transport (mimics test_go_conformance's subprocess mode)
            @contextlib.contextmanager
            def _shared_subproc() -> Iterator[_RpcProxy]:
                yield _RpcProxy(ConformanceService, java_transport, on_log)
            return _shared_subproc()
        elif request.param == "subprocess_shm":
            # Co-located subprocess worker with the POSIX shared-memory
            # side-channel active: the client owns a segment and advertises it,
            # so batches transfer through shm (bidirectionally) instead of the
            # pipe. The worker attaches on JDK >= 22; on a runtime without shm
            # it transparently falls back to inline transfer.
            @contextlib.contextmanager
            def _shm_conn() -> Iterator[_RpcProxy]:
                segment = ShmSegment.create(SHM_SEGMENT_BYTES)
                transport = ShmPipeTransport(SubprocessTransport([JAVA_WORKER]), segment)
                try:
                    yield _RpcProxy(ConformanceService, transport, on_log)
                finally:
                    transport.close()       # closes the pipe; not the segment
                    segment.close()
                    segment.unlink()
            return _shm_conn()
        elif request.param == "http":
            return http_connect(
                ConformanceService,
                f"http://127.0.0.1:{java_http_port}",
                on_log=on_log,
            )
        elif request.param == "http_externalize_always":
            from vgi_rpc.external import ExternalLocationConfig

            ext_port: int = request.getfixturevalue("conformance_http_externalize_always_port")
            return http_connect(
                ConformanceService,
                f"http://127.0.0.1:{ext_port}",
                on_log=on_log,
                # Server uses http://127.0.0.1 download URLs from the
                # in-process fake storage; disable the HTTPS-only validator.
                external_location=ExternalLocationConfig(url_validator=None),
            )
        elif request.param == "unix":
            return unix_connect(ConformanceService, java_unix_path, on_log=on_log)
        elif request.param == "tcp":
            tcp_host, tcp_port = java_tcp_addr
            return tcp_connect(ConformanceService, tcp_host, tcp_port, on_log=on_log)
        raise ValueError(request.param)

    return factory


# Import the canonical pytest suite from the vgi-rpc package.
from vgi_rpc.conformance._pytest_suite import *  # noqa: F401,F403,E402


@pytest.fixture(scope="session")
def java_http_shared_key_ports() -> Iterator[tuple[int, int]]:
    """Two HTTP workers sharing one --token-key, so tokens minted by one
    decrypt on the other — the load-balanced / relay topology that
    continuation-only resume exists for."""
    key = "00" * 32
    procs: list[subprocess.Popen[bytes]] = []
    ports: list[int] = []
    try:
        for _ in range(2):
            proc = subprocess.Popen(
                [JAVA_WORKER, "--http", "--token-key", key],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            procs.append(proc)
            assert proc.stdout is not None
            line = proc.stdout.readline().decode().strip()
            assert line.startswith("PORT:"), f"Expected PORT:<n>, got: {line!r}"
            port = int(line.split(":", 1)[1])
            _wait_for_http(port)
            ports.append(port)
        yield (ports[0], ports[1])
    finally:
        for proc in procs:
            proc.terminate()
            proc.wait(timeout=5)


class TestContinuationOnlyResume:
    """Java worker mirror of the 0.20.0 ``_HttpProxy.resume_stream`` contract.

    A producer continuation token minted by worker A must resume on worker B
    (same token key) with no ``/init`` round-trip on B: the server recovers
    state, schemas, and the state class from the signed token plus
    construction-time introspection alone.
    """

    def test_resume_stream_on_fresh_worker(
        self, java_http_shared_key_ports: tuple[int, int]
    ) -> None:
        port_a, port_b = java_http_shared_key_ports
        with (
            http_connect(ConformanceService, f"http://127.0.0.1:{port_a}") as proxy_a,
            http_connect(ConformanceService, f"http://127.0.0.1:{port_b}") as proxy_b,
        ):
            session = proxy_a.produce_n(count=4)
            first, token = session.next_with_token()
            assert first is not None and token is not None
            assert first.batch.column("index").to_pylist() == [0]

            resumed = proxy_b.resume_stream("produce_n", token)
            rest = [ab.batch.column("index").to_pylist() for ab in resumed]
            assert rest == [[1], [2], [3]]

    def test_next_with_token_walks_whole_stream(
        self, java_http_shared_key_ports: tuple[int, int]
    ) -> None:
        """Every per-batch token is a valid resume point on the other worker."""
        port_a, port_b = java_http_shared_key_ports
        with (
            http_connect(ConformanceService, f"http://127.0.0.1:{port_a}") as proxy_a,
            http_connect(ConformanceService, f"http://127.0.0.1:{port_b}") as proxy_b,
        ):
            session = proxy_a.produce_n(count=3)
            tokens: list[bytes] = []
            values: list[int] = []
            while True:
                ab, token = session.next_with_token()
                if ab is None:
                    break
                values.append(ab.batch.column("value")[0].as_py())
                if token is not None:
                    tokens.append(token)
            assert values == [0, 10, 20]

            # Resume from the first token: replays everything after batch 0.
            resumed = proxy_b.resume_stream("produce_n", tokens[0])
            assert [ab.batch.column("value")[0].as_py() for ab in resumed] == [10, 20]


class TestHttpStreamAccessLog:
    """Every HTTP turn of a stream call must produce an access-log record.

    ``vgi-rpc-test --access-log`` validates the *launcher* worker's log, where a
    whole stream call is a single dispatch and therefore a single record. Over
    HTTP a stream is a chain of independent requests — one ``/init`` and one
    ``/exchange`` per continuation — and the Java server fired its dispatch hook
    only on the unary path, so streams produced **no records at all**. The
    validator passed anyway: it checks the records it is given, and there is
    nothing wrong with a log that is merely missing the traffic that carries the
    bytes.

    So this asserts presence and shape, which a schema check structurally
    cannot: an init record carrying ``request_data``, at least one continuation
    carrying none, and one ``stream_id`` joining them — plus the reference
    validator over exactly those records, so shape and conformance are both
    covered.

    ``docs/access-log-spec.md`` §1 ("Stream calls produce one record per init
    and one per exchange/produce continuation") and §5.
    """

    #: A stream's lifecycle id: 32 lowercase hex, per the schema.
    _STREAM_ID = re.compile(r"^[0-9a-f]{32}$")

    @staticmethod
    def _await_records(
        log_path: Path, method: str, minimum: int, timeout: float = 1.5
    ) -> list[dict[str, Any]]:
        """Poll the log for at least *minimum* stream records naming *method*.

        The record is written as the response completes, so this waits on the
        writer rather than racing it — a short wait, because the writer is
        synchronous and has effectively already run by the time the client sees
        the last response. It has to be short: the shared suite's module-level
        ``pytest.mark.timeout(5)`` arrives here through the star-import and
        outranks any ``--timeout`` on the command line, so a generous poll would
        turn a missing-record failure into an unreadable timeout.

        Returns whatever it has at the deadline — the assertions, not this
        helper, decide whether that is enough.
        """
        import json

        found: list[dict[str, Any]] = []
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if log_path.exists():
                found = [
                    rec
                    for line in log_path.read_text().splitlines()
                    if line.strip()
                    for rec in [json.loads(line)]
                    if rec.get("logger") == "vgi_rpc.access"
                    and rec.get("method") == method
                    and rec.get("method_type") == "stream"
                ]
                if len(found) >= minimum:
                    break
            time.sleep(0.05)
        return found

    def _assert_stream_shape(self, records: list[dict[str, Any]], method: str) -> None:
        """Assert one init + at least one continuation, sharing a stream id."""
        from vgi_rpc.access_log_conformance import validate_access_logs

        assert records, (
            f"no access-log records for the {method!r} stream; HTTP stream turns must be "
            f"logged like unary calls are — a validator over a log that omits them passes "
            f"on nothing"
        )
        stream_ids = {r.get("stream_id") for r in records}
        assert len(stream_ids) == 1, (
            f"records for one {method!r} call must share one stream_id, saw {stream_ids}; "
            f"without that the turns of a stream cannot be joined"
        )
        stream_id = stream_ids.pop()
        assert isinstance(stream_id, str) and self._STREAM_ID.match(stream_id), (
            f"stream_id must be 32 lowercase hex characters, got {stream_id!r}"
        )

        inits = [r for r in records if "request_data" in r]
        conts = [r for r in records if "request_data" not in r]
        assert len(inits) == 1, (
            f"exactly one {method!r} record must carry request_data (the /init turn), got "
            f"{len(inits)} of {len(records)}"
        )
        assert conts, (
            f"no continuation record for {method!r}: /exchange turns are where a stream's "
            f"bytes actually move, and they were the half that went unlogged"
        )
        for rec in records:
            assert rec.get("http_status") == 200, f"expected http_status 200, got {rec.get('http_status')}"
            assert rec.get("request_bytes", -1) >= 0, "stream turns must report request_bytes"

        violations = validate_access_logs(records)
        assert not violations, f"stream records violate the access-log schema: {violations}"

    def test_producer_stream_logs_every_turn(
        self, conformance_http_access_log: tuple[int, Path]
    ) -> None:
        """A producer stream logs its init and each continuation."""
        port, log_path = conformance_http_access_log
        with http_connect(ConformanceService, f"http://127.0.0.1:{port}") as proxy:
            values = [ab.batch.column("value")[0].as_py() for ab in proxy.produce_n(count=3)]
        assert values == [0, 10, 20]

        # init + one continuation per remaining batch.
        records = self._await_records(log_path, "produce_n", 4)
        self._assert_stream_shape(records, "produce_n")
        # The init mints the first cursor; the turn that closes the stream mints
        # none, and its absence is the record saying so.
        assert any("response_state" in r for r in records), "a turn that mints a cursor must log it"
        assert any("request_state" in r for r in records), (
            "a continuation must log the decrypted state the client sent, not the "
            "AEAD ciphertext a reader cannot open"
        )

    def test_exchange_stream_logs_every_turn(
        self, conformance_http_access_log: tuple[int, Path]
    ) -> None:
        """A bidirectional exchange stream logs its init and each exchange."""
        from vgi_rpc.rpc import AnnotatedBatch

        port, log_path = conformance_http_access_log
        with (
            http_connect(ConformanceService, f"http://127.0.0.1:{port}") as proxy,
            proxy.exchange_accumulate() as session,
        ):
            first = session.exchange(AnnotatedBatch.from_pydict({"value": [1.0, 2.0]}))
            second = session.exchange(AnnotatedBatch.from_pydict({"value": [10.0]}))
        assert first.batch.column("running_sum")[0].as_py() == pytest.approx(3.0)
        assert second.batch.column("running_sum")[0].as_py() == pytest.approx(13.0)

        records = self._await_records(log_path, "exchange_accumulate", 3)
        self._assert_stream_shape(records, "exchange_accumulate")

    def test_failing_stream_turn_is_logged_as_an_error(
        self, conformance_http_access_log: tuple[int, Path]
    ) -> None:
        """A raising turn answers 200, so only the record says it failed.

        The status line cannot: the exception rides the body as an EXCEPTION
        batch. A record reporting ``ok`` for it would hide the failure from the
        one place an operator looks for it.
        """
        port, log_path = conformance_http_access_log
        with http_connect(ConformanceService, f"http://127.0.0.1:{port}") as proxy:
            with pytest.raises(Exception):
                list(proxy.produce_error_on_init())

        records = self._await_records(log_path, "produce_error_on_init", 1)
        assert records, "a stream that raised on init produced no access-log record"
        rec = records[-1]
        assert rec["status"] == "error", f"expected status=error, got {rec['status']!r}"
        assert rec["error_type"], "an error record must name the error type"
        assert rec["error_message"], "an error record must carry the server-side message"


class TestHttpResponseCapAccessLog:
    """A response-cap overshoot must be logged as the failure it is.

    The overshoot is detected *after* dispatch has returned: the body exists,
    it is too big, so the server discards it and answers an EXCEPTION batch
    instead. Every wire-visible signal agrees the call failed --
    ``X-VGI-RPC-Error: true``, and an ``RpcError`` on the client -- while the
    access record, whose ``status`` was settled when dispatch ended, said
    ``ok``. That is worse than a missing record: an operator diffing "errors
    the clients saw" against "errors the server logged" gets a clean log and
    concludes the clients are wrong.

    ``docs/access-log-spec.md`` §3 (``status`` is ``"error"`` for any failure)
    and §4.1 (``error_message`` required and non-empty when it is).
    """

    @staticmethod
    def _await_records(
        log_path: Path, method: str, minimum: int, timeout: float = 2.0
    ) -> list[dict[str, Any]]:
        """Poll the log for at least *minimum* records naming *method*.

        Short by design: the shared suite's module-level ``pytest.mark.timeout(5)``
        arrives here through the star-import, so a generous poll would turn a
        missing-record failure into an unreadable timeout. Returns whatever it
        has at the deadline -- the assertions decide whether that is enough.
        """
        import json

        found: list[dict[str, Any]] = []
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if log_path.exists():
                found = [
                    rec
                    for line in log_path.read_text().splitlines()
                    if line.strip()
                    for rec in [json.loads(line)]
                    if rec.get("logger") == "vgi_rpc.access" and rec.get("method") == method
                ]
                if len(found) >= minimum:
                    break
            time.sleep(0.05)
        return found

    @staticmethod
    def _assert_cap_error(rec: dict[str, Any], method: str) -> None:
        """Assert one record reports the cap overshoot the wire reported."""
        from vgi_rpc.access_log_conformance import validate_access_logs

        assert rec["status"] == "error", (
            f"{method!r} overshot max_response_bytes -- the client got an RpcError and the "
            f"response carried X-VGI-RPC-Error -- but the access record says "
            f"status={rec['status']!r}; the log is the only place that failure is visible "
            f"after the fact"
        )
        assert rec["error_type"], "an error record must name the error type"
        assert "max_response_bytes" in rec.get("error_message", ""), (
            f"the record must say which cap was overshot, got "
            f"error_message={rec.get('error_message')!r}"
        )
        assert rec["message"].endswith(" error"), (
            f"the human-readable summary must agree with the structured status, got "
            f"{rec['message']!r}"
        )
        violations = validate_access_logs([rec])
        assert not violations, f"cap-overshoot record violates the access-log schema: {violations}"

    def _cap(self, port: int) -> int:
        """The wire cap this worker advertises."""
        from vgi_rpc.http import http_capabilities

        caps = http_capabilities(base_url=f"http://127.0.0.1:{port}")
        assert caps.max_response_bytes is not None, "fixture must advertise a wire cap"
        return int(caps.max_response_bytes)

    def test_unary_overshoot_is_logged_as_an_error(
        self, conformance_http_capped_access_log: tuple[int, Path]
    ) -> None:
        """A unary response discarded for overshooting the cap logs status=error."""
        from vgi_rpc.rpc import RpcError

        port, log_path = conformance_http_capped_access_log
        with (
            http_connect(ConformanceService, f"http://127.0.0.1:{port}") as proxy,
            pytest.raises(RpcError, match=r"max_response_bytes"),
        ):
            proxy.oversized_unary(target_bytes=self._cap(port) * 4)

        records = self._await_records(log_path, "oversized_unary", 1)
        assert records, "the overshooting unary call produced no access-log record at all"
        self._assert_cap_error(records[-1], "oversized_unary")

    def test_exchange_overshoot_is_logged_as_an_error(
        self, conformance_http_capped_access_log: tuple[int, Path]
    ) -> None:
        """The overshooting stream turn logs error; the init turn that succeeded stays ok.

        Streams only began producing records at all in the commit before this
        one, so this path has never been exercised for a cap overshoot. Both
        halves matter: promoting the whole call to ``error`` would blame the
        ``/init`` turn, which genuinely succeeded.
        """
        from vgi_rpc.rpc import AnnotatedBatch, RpcError

        port, log_path = conformance_http_capped_access_log
        target_rows = max(1024, (self._cap(port) * 4) // 16)
        with (
            http_connect(ConformanceService, f"http://127.0.0.1:{port}") as proxy,
            pytest.raises(RpcError, match=r"max_response_bytes"),
            proxy.exchange_oversized(rows_per_batch=target_rows) as session,
        ):
            session.exchange(AnnotatedBatch.from_pydict({"value": [1.0]}))

        records = self._await_records(log_path, "exchange_oversized", 2)
        assert len(records) >= 2, (
            f"expected an /init record and the overshooting /exchange record, got {len(records)}"
        )
        inits = [r for r in records if "request_data" in r]
        conts = [r for r in records if "request_data" not in r]
        assert inits and conts, f"expected both an init and a continuation record, got {records}"
        assert all(r["status"] == "ok" for r in inits), (
            "the /init turn answered a well-formed response under the cap; marking it failed "
            "attributes the overshoot to the wrong turn"
        )
        self._assert_cap_error(conts[-1], "exchange_oversized")

    def test_producer_soft_cap_is_not_logged_as_an_error(
        self, conformance_http_capped_access_log: tuple[int, Path]
    ) -> None:
        """A producer overshoot is covered by a continuation, so nothing failed.

        The negative control on the other two: the wire cap is *soft* for
        producer streams -- the framework mints a continuation token instead of
        failing -- so no error reaches the wire and every record must still say
        ``ok``. An implementation that re-stated status from the mere presence
        of a cap overshoot would fail here.
        """
        port, log_path = conformance_http_capped_access_log
        target_rows = max(1024, (self._cap(port) * 2) // 16)
        with http_connect(ConformanceService, f"http://127.0.0.1:{port}") as proxy:
            batches = list(proxy.produce_oversized_batch(rows_per_batch=target_rows))
        assert sum(b.batch.num_rows for b in batches) == target_rows

        records = self._await_records(log_path, "produce_oversized_batch", 1)
        assert records, "the producer stream produced no access-log record"
        for rec in records:
            assert rec["status"] == "ok", (
                f"a producer overshoot is absorbed by a continuation token, not an error; "
                f"got status={rec['status']!r} error_message={rec.get('error_message')!r}"
            )
