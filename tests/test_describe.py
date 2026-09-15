"""Introspection smoke test against the Java pipe worker.

``__describe__`` is retired; introspection is the ``vgi_rpc.Reflection.v1``
protocol -- ``list_protocols`` for what the server hosts, then ``describe`` for
one protocol's methods. ``introspect()`` is exactly those two calls, so this
script still reads the same but reaches the worker a different way.
"""
from vgi_rpc.introspect import introspect
from vgi_rpc.rpc import SubprocessTransport

from pathlib import Path
WORKER = str(Path(__file__).parent.parent / "conformance-worker/build/install/conformance-worker/bin/conformance-worker")

transport = SubprocessTransport([WORKER])
try:
    desc = introspect(transport)
    print(f"protocol={desc.protocol_name!r}, server_id={desc.server_id}, methods={len(desc.methods)}")
    print(f"protocol_hash={desc.protocol_hash!r}, request_version={desc.request_version!r}")
    for name in sorted(list(desc.methods.keys()))[:5]:
        md = desc.methods[name]
        print(f"  {name}({md.method_type.value}): has_return={md.has_return} has_header={md.has_header}")
finally:
    transport.close()
