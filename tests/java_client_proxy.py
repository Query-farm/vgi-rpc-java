"""Point the shared conformance suite at this port's *client*.

The suite drives a server by default. To drive a foreign client instead, a port
ships one executable — a **driver** — speaking the newline-delimited JSON
control protocol written down in the reference repository's
``tools/cross-port/specs/CLIENT_DRIVER_PROTOCOL.md``. The Python half of that
bridge is port-agnostic and already written
(``vgi_rpc.conformance.client_driver``); this module is the only genuinely
Java-shaped part — where this repository's driver lives, and nothing else.

The driver is launched as an **installed distribution**, never ``gradlew run``:
stdout is the control channel, and Gradle writes its progress there.
"""
from __future__ import annotations

from pathlib import Path

from vgi_rpc.conformance.client_driver import ClientDriver

#: Developer-machine fallback. ``VGI_CLIENT_DRIVER`` wins when set, and every CI
#: leg sets it; ``ClientDriver.from_env`` splits it with ``shlex.split`` so an
#: interpreted driver needs no wrapper script.
_DEFAULT_DRIVER = str(
    Path(__file__).parent.parent
    / "conformance-client-driver/build/install/conformance-client-driver/bin/conformance-client-driver"
)

DRIVER = ClientDriver.from_env(default=[_DEFAULT_DRIVER])
