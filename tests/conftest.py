"""Make the vgi_rpc reference package importable for the conformance driver.

The suite is re-exported from the Python `vgi_rpc` package. Normally that
package is installed in the interpreter running pytest (CI pip-installs it, or
locally you run via the reference venv's python), so nothing extra is needed.

As an escape hatch, set VGI_RPC_SITE to a site-packages directory and it will
be prepended to sys.path — useful when running under an interpreter that
doesn't have vgi_rpc installed but can borrow it from elsewhere.
"""
import os
import sys

_site = os.environ.get("VGI_RPC_SITE")
if _site and os.path.isdir(_site) and _site not in sys.path:
    sys.path.insert(0, _site)


def pytest_collection_modifyitems(items):
    """Raise the shared suite's short per-test timeouts in the client role.

    Several groups carry a module-wide ``pytest.mark.timeout(5)``. That marker is
    a liveness guard — it exists so a hung call fails as a test rather than
    stalling the run — and five seconds is generous when the client is an
    in-process Python object.

    In the client role it is not. Every connection is a fresh driver process, so
    a test that opens three of them pays three JVM starts inside its own budget,
    and the sticky expiry case additionally sleeps ``ttl + 1.5`` seconds on
    purpose. Measured end to end those land at 4.3–5.3s: green on an idle
    machine, red under a full-suite load, on a two-core CI runner reliably red.
    A conformance lane that fails on wall-clock noise reports nothing about the
    wire, and teaches everyone to ignore it.

    So the guard is kept and widened, not removed: 30s still turns a genuine hang
    into a failed test within the job's budget, while no longer failing a correct
    client for starting a JVM. Only markers *below* the floor are raised, so a
    group that deliberately asks for longer keeps what it asked for. Server role
    is untouched — it has never been near the limit.
    """
    if os.environ.get("VGI_CONFORMANCE_ROLE", "server") != "client":
        return
    import pytest

    floor = 30
    for item in items:
        marker = item.get_closest_marker("timeout")
        if marker is None or not marker.args:
            continue
        try:
            current = float(marker.args[0])
        except (TypeError, ValueError):
            continue
        if current < floor:
            item.add_marker(pytest.mark.timeout(floor))


#: Budget for the teardown that finalises the session's fixtures (see below).
_SESSION_TEARDOWN_TIMEOUT = 120


def pytest_runtest_teardown(item, nextitem):
    """Give the session-fixture teardown its own budget, not the last test's.

    pytest finalises every session-scoped fixture inside the teardown of the
    *last* item a process runs, and pytest-timeout times setup, call and
    teardown as one budget. The shared suite's ``pytest.mark.timeout(5)`` is a
    per-test liveness guard, but that last item's teardown also stops every
    worker the session started -- here some thirty fixtures, each a
    ``terminate()`` + ``wait()`` on a JVM -- so whichever test happened to run
    last could fail with ``Timeout (>5.0s)`` for work that was never its own.

    Serially that is one item per run; under pytest-xdist it is one per xdist
    worker, all shutting their JVMs down at once, and on a loaded machine it
    failed for real. So for that one teardown the timer is restarted with a
    budget sized for the shutdown: still a hang guard, no longer a coin flip.
    """
    if nextitem is not None or not item.config.pluginmanager.hasplugin("timeout"):
        return
    try:
        from pytest_timeout import _get_item_settings
    except ImportError:
        return
    settings = _get_item_settings(item)
    if not settings.timeout or settings.func_only or settings.timeout >= _SESSION_TEARDOWN_TIMEOUT:
        return
    hook = item.config.hook
    hook.pytest_timeout_cancel_timer(item=item)
    hook.pytest_timeout_set_timer(item=item, settings=settings._replace(timeout=_SESSION_TEARDOWN_TIMEOUT))
