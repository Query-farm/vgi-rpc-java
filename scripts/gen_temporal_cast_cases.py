#!/usr/bin/env python3
# Copyright 2025-2026 Query.Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""Record how pyarrow casts strings to and from temporal types.

Writes vgirpc/src/test/resources/farm/query/vgirpc/pyarrow_temporal_casts.json,
the oracle OutputReconcilerTemporalTest checks the Java casts against. Every
case is one Array.cast in the reference's pyarrow; the file records the value it
produced or the error it raised, verbatim.

    python scripts/gen_temporal_cast_cases.py   # with the reference's pyarrow importable

Kinds:
  ok          -- pyarrow produced `expected` (an int for a temporal target,
                 a str for a string target)
  error       -- pyarrow raised ArrowInvalid with message `expected`
  unsupported -- pyarrow has no such cast (ArrowNotImplementedError)
  diverge     -- a timezone-aware timestamp outside years -32767..32767:
                 pyarrow's zoned formatter prints wrapped garbage there, Java
                 refuses
"""

import json
import pathlib
import sys

import pyarrow as pa

OUT = pathlib.Path(__file__).resolve().parent.parent / "vgirpc/src/test/resources/farm/query/vgirpc/pyarrow_temporal_casts.json"

TYPES = {
    "string": pa.string(),
    "large_string": pa.large_string(),
    "binary": pa.binary(),
    "date32": pa.date32(),
    "date64": pa.date64(),
    "time32[s]": pa.time32("s"),
    "time32[ms]": pa.time32("ms"),
    "time64[us]": pa.time64("us"),
    "time64[ns]": pa.time64("ns"),
}
for unit in ["s", "ms", "us", "ns"]:
    TYPES[f"timestamp[{unit}]"] = pa.timestamp(unit)
    for tz in ["UTC", "America/New_York", "+05:30", "Asia/Kolkata"]:
        TYPES[f"timestamp[{unit},{tz}]"] = pa.timestamp(unit, tz=tz)

PER_DAY = {"s": 86400, "ms": 86400 * 10**3, "us": 86400 * 10**6, "ns": 86400 * 10**9}
MIN_DAY, MAX_DAY = -12687428, 11248738


def physical(arr):
    """The integer behind a temporal array, without a cast pyarrow may refuse."""
    t = arr.type
    width = pa.int32() if (pa.types.is_date32(t) or pa.types.is_time32(t)) else pa.int64()
    return pa.Array.from_buffers(width, len(arr), arr.buffers()).to_pylist()


def record(cases, source, target, value):
    src = TYPES[source]
    if isinstance(value, int) and not pa.types.is_integer(src):
        width = pa.int32() if (pa.types.is_date32(src) or pa.types.is_time32(src)) else pa.int64()
        arr = pa.Array.from_buffers(src, 1, pa.array([value], width).buffers())
    else:
        arr = pa.array([value], src)
    tgt = TYPES[target]
    case = {"source": source, "target": target, "input": value if not isinstance(value, bytes) else value.decode()}
    if (pa.types.is_timestamp(src) and src.tz is not None and pa.types.is_string(tgt)
            and src.unit != "ns" and not (MIN_DAY * PER_DAY[src.unit] <= value < MAX_DAY * PER_DAY[src.unit])):
        case.update(kind="diverge")
        cases.append(case)
        return
    try:
        out = arr.cast(tgt)
        expected = out.to_pylist()[0] if pa.types.is_string(tgt) or pa.types.is_large_string(tgt) else physical(out)[0]
        case.update(kind="ok", expected=expected)
    except pa.ArrowNotImplementedError:
        case.update(kind="unsupported")
    except pa.ArrowInvalid as e:
        case.update(kind="error", expected=str(e))
    cases.append(case)


def main():
    cases = []
    ts_strings = [
        "2020-01-02", "2020-01-02 03", "2020-01-02T03", "2020-01-02 03:04", "2020-01-02T03:04:05",
        "2020-01-02 03:04:05", "2020-01-02t03:04:05", "2020-01-02  03:04", "2020-01-02 03:04:05.",
        "2020-01-02 03:04:05.1", "2020-01-02 03:04:05.123", "2020-01-02 03:04:05.1234",
        "2020-01-02 03:04:05.123456", "2020-01-02 03:04:05.1234567", "2020-01-02 03:04:05.123456789",
        "2020-01-02 03:04:05.1234567890", "2020-01-02 03:04:05Z", "2020-01-02 03:04:05+01",
        "2020-01-02 03:04:05+0130", "2020-01-02 03:04:05+01:30", "2020-01-02 03:04:05-0530",
        "2020-01-02T03:04:05.123Z", "2020-01-02T03:04:05.5-01:00", "2020-01-02 03+01", "2020-01-02 03:04Z",
        "2020-01-02 24:00:00", "2020-01-02 23:60:00", "2020-01-02 23:59:60", "2020-02-29", "2021-02-29",
        "1600-02-29", "1700-02-29", "2020-13-01", "2020-00-01", "2020-01-00", "0000-01-01",
        "9999-12-31 23:59:59", "20200102", "2020-1-2", "2020/01/02", "", "2020-01-02 03:04:05 ",
        "+2020-01-02", " 2020-01-02", "2020-01-02 03:04:05,123", "1969-12-31 23:59:59.999", "1900-01-01",
        "2300-01-01", "1677-09-21 00:12:43.145224191", "1677-09-21 00:12:43.145224192",
        "2262-04-11 23:47:16.854775807", "2262-04-11 23:47:16.854775808", "2020-01-02 3:04", "2020-01-02 03:4",
        "2020-01-02 03:04:5", "2020-01-02 03:04:05.12a", "2020-01-02 03:04:05.-12", "2020-01-02 03:04:05+24:00",
        "2020-01-02 03:04:05+01:60", "2020-01-02 03:04:05+1", "2020-01-02 03:04:05+01:3",
        "2020-01-02 03:04:05 +01:00", "2020-01-02 03:04:05+0960", "2020-01-02 03:04:05-25", "abc",
        "2020-01-02é03:04", "éééé-01-02",
    ]
    for unit in ["s", "ms", "us", "ns"]:
        for target in [f"timestamp[{unit}]", f"timestamp[{unit},UTC]", f"timestamp[{unit},America/New_York]"]:
            for s in ts_strings:
                record(cases, "string", target, s)
    for s in ["2020-01-02", "2020-01-02 03:04:05Z"]:
        record(cases, "large_string", "timestamp[s]", s)
        record(cases, "large_string", "timestamp[s,UTC]", s)
    record(cases, "binary", "timestamp[s]", b"2020-01-02")
    for target in ["date32", "date64"]:
        for s in ["2020-01-02", "1969-12-31", "0000-01-01", "9999-12-31", "2020-02-29", "2020-02-30", "1600-02-29",
                  "1700-02-29", "2020-01-02 00:00", "2020-01-02T00:00:00", "20200102", "", "-001-01-01",
                  "2020-1-02", " 2020-01-02"]:
            record(cases, "string", target, s)
        record(cases, "large_string", target, "2020-01-02")
        record(cases, "binary", target, b"2020-01-02")
    for target in ["time32[s]", "time32[ms]", "time64[us]", "time64[ns]"]:
        for s in ["03:04", "03:04:05", "03:04:05.123"]:
            record(cases, "string", target, s)

    ts_values = [0, 1, -1, 123, -123, 1577934245, -86400, -86401, 1577934245123, 253402300799, 253402300800,
                 -62135596800, -62167219200, -62167219201, 10**12, -10**12, 10**15, 2**62, -2**62,
                 959904000, 1710054000, 1710053999, 1730613600, 1730613599]
    for unit in ["s", "ms", "us", "ns"]:
        for tz in [None, "UTC", "America/New_York", "+05:30", "Asia/Kolkata"]:
            source = f"timestamp[{unit}]" if tz is None else f"timestamp[{unit},{tz}]"
            for v in ts_values:
                record(cases, source, "string", v)
    record(cases, "timestamp[ms]", "large_string", 1577934245123)
    record(cases, "timestamp[ms,UTC]", "large_string", 1577934245123)
    for v in [0, 1, -1, 18263, -719528, -719529, 2932896, 2932897, -12687428, -12687429, 11248737, 11248738,
              2**31 - 1, -2**31]:
        record(cases, "date32", "string", v)
    for v in [0, 1, -1, 86399999, 86400000, -86400000, -86400001, 1577934245123, 10**16, -10**16]:
        record(cases, "date64", "string", v)
    for target, vals in [("time32[s]", [0, 1, 86399, 86400, -1, 3723]),
                         ("time32[ms]", [0, 1, 86399999, 86400000, -1, 3723045]),
                         ("time64[us]", [0, 1, 86399999999, 86400000000, -1, 3723045678]),
                         ("time64[ns]", [0, 1, 86399999999999, 86400000000000, -1, 3723045678901])]:
        for v in vals:
            record(cases, target, "string", v)
    record(cases, "date32", "large_string", 18263)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps({"pyarrow": pa.__version__, "cases": cases}, indent=1, ensure_ascii=False) + "\n")
    print(f"{len(cases)} cases from pyarrow {pa.__version__} -> {OUT}", file=sys.stderr)


if __name__ == "__main__":
    main()
