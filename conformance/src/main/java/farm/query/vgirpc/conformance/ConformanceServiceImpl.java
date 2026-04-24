// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.conformance;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public final class ConformanceServiceImpl implements ConformanceService {

    @Override public String echo_string(String value) { return value; }
    @Override public byte[] echo_bytes(byte[] data) { return data; }
    @Override public long echo_int(long value) { return value; }
    @Override public double echo_float(double value) { return value; }
    @Override public boolean echo_bool(boolean value) { return value; }

    @Override public void void_noop() {}
    @Override public void void_with_param(long value) {}

    @Override public Status echo_enum(Status status) { return status; }
    @Override public List<String> echo_list(List<String> values) { return values; }
    @Override public Map<String, Long> echo_dict(Map<String, Long> mapping) { return mapping; }
    @Override public List<List<Long>> echo_nested_list(List<List<Long>> matrix) { return matrix; }

    @Override public Optional<String> echo_optional_string(Optional<String> value) { return value; }
    @Override public Optional<Long> echo_optional_int(Optional<Long> value) { return value; }

    @Override public Point echo_point(Point point) { return point; }
    @Override public BoundingBox echo_bounding_box(BoundingBox box) { return box; }

    @Override public String inspect_point(Point point) {
        return "Point(x=" + point.x() + ", y=" + point.y() + ")";
    }

    @Override public int echo_int32(int value) { return value; }
    @Override public float echo_float32(float value) { return value; }

    @Override public double add_floats(double a, double b) { return a + b; }

    @Override public String concatenate(String prefix, String suffix, String separator) {
        return prefix + (separator != null ? separator : "-") + suffix;
    }

    @Override public String with_defaults(long required, String optional_str, long optional_int) {
        return "required=" + required
                + ", optional_str=" + (optional_str != null ? optional_str : "default")
                + ", optional_int=" + optional_int;
    }

    @Override public String raise_value_error(String message) {
        throw new IllegalArgumentException(message);
    }
    @Override public String raise_runtime_error(String message) {
        throw new RuntimeException(message);
    }
    @Override public String raise_type_error(String message) {
        throw new ClassCastException(message);
    }
}
