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
