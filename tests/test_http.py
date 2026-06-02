"""HTTP transport smoke test against Java worker."""
import subprocess
import time

from vgi_rpc.conformance import ConformanceService, Point
from vgi_rpc.http import http_connect

from pathlib import Path
WORKER = str(Path(__file__).parent.parent / "conformance-worker/build/install/conformance-worker/bin/conformance-worker")

proc = subprocess.Popen([WORKER, "--http"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
try:
    line = proc.stdout.readline().decode().strip()
    assert line.startswith("PORT:"), f"expected PORT:<n>, got {line!r}"
    port = int(line.split(":", 1)[1])
    print(f"server on port {port}")
    time.sleep(0.2)

    with http_connect(ConformanceService, f"http://127.0.0.1:{port}") as proxy:
        print("echo_string ->", proxy.echo_string(value="hello"))
        print("echo_int ->", proxy.echo_int(value=42))
        print("echo_point ->", proxy.echo_point(point=Point(x=1.5, y=2.5)))
        print("add_floats ->", proxy.add_floats(a=1.5, b=2.5))
finally:
    proc.terminate()
    proc.wait(timeout=5)
