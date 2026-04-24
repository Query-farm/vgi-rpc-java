"""Reproduce the AllTypes round-trip and surface the Java traceback."""
from vgi_rpc.conformance import (
    AllTypes, ConformanceService, Point, Status,
)
from vgi_rpc.rpc import SubprocessTransport, _RpcProxy, RpcError

WORKER = "/Users/rusty/Development/vgi-rpc-java/conformance-worker/build/install/conformance-worker/bin/conformance-worker"


def make_all_types() -> AllTypes:
    return AllTypes(
        str_field="hello",
        bytes_field=b"world",
        int_field=42,
        float_field=3.14,
        bool_field=True,
        list_of_int=[1, 2, 3],
        list_of_str=["a", "b"],
        dict_field={"x": 1, "y": 2},
        enum_field=Status.ACTIVE,
        nested_point=Point(x=1.0, y=2.0),
        optional_str="present",
        optional_int=7,
        optional_nested=Point(x=3.0, y=4.0),
        list_of_nested=[Point(x=1.0, y=1.0), Point(x=2.0, y=2.0)],
        annotated_int32=100,
        annotated_float32=1.5,
        nested_list=[[1, 2], [3, 4, 5]],
        dict_str_str={"k": "v"},
    )


transport = SubprocessTransport([WORKER])
proxy = _RpcProxy(ConformanceService, transport)
try:
    result = proxy.echo_all_types(data=make_all_types())
    print("OK", result)
except RpcError as e:
    print("error_type=", e.error_type)
    print("message=", e.error_message)
    print("--- REMOTE TRACEBACK ---")
    print(e.remote_traceback)
finally:
    transport.close()
