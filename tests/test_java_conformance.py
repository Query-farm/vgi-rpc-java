"""Run the reference pytest conformance suite against the Java worker.

Mirrors test_go_conformance.py from vgi-rpc-go, parametrising by transport
(pipe / subprocess / http / unix) so the entire wire surface is exercised.
"""
from __future__ import annotations

import contextlib
import os
import socket
import subprocess
import tempfile
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest

from vgi_rpc.conformance import ConformanceService
from vgi_rpc.http import http_connect
from vgi_rpc.log import Message
from vgi_rpc.rpc import ShmPipeTransport, SubprocessTransport, _RpcProxy, unix_connect
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


@pytest.fixture(scope="session")
def conformance_http_port(java_http_port: int) -> int:
    """Reuse the no-auth HTTP worker for the TestHealth conformance contract."""
    return java_http_port


@pytest.fixture(scope="session")
def conformance_http_auth_port() -> Iterator[int]:
    """Spawn an HTTP worker with bearer auth so every RPC POST returns 401."""
    proc = subprocess.Popen(
        [JAVA_WORKER, "--http", "--auth-bearer", "secret=alice"],
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
_ALL_CONNS = ["pipe", "subprocess", "subprocess_shm", "http", "http_externalize_always", "unix"]
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
        raise ValueError(request.param)

    return factory


# Import the canonical pytest suite from the vgi-rpc package.
from vgi_rpc.conformance._pytest_suite import *  # noqa: F401,F403,E402
