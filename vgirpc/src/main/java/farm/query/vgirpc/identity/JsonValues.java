// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0
package farm.query.vgirpc.identity;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** JSON-only deep snapshots used to keep evidence immutable and portable. */
final class JsonValues {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final int MAX_BYTES = 65_536;
    private static final int MAX_DEPTH = 16;
    private static final int MAX_VALUES = 4_096;

    private JsonValues() {}

    static Map<String, Object> snapshotMap(Map<String, ?> input) {
        if (input == null || input.isEmpty()) return Map.of();
        Map<String, Object> out = new LinkedHashMap<>();
        int[] count = {1};
        input.forEach((key, value) -> {
            if (key == null) throw new IllegalArgumentException("JSON object key must not be null");
            requireWellFormed(key, "JSON object key");
            out.put(key, snapshot(value, 1, count));
        });
        Map<String, Object> snapshot = Collections.unmodifiableMap(out);
        if (canonicalJson(snapshot).getBytes(java.nio.charset.StandardCharsets.UTF_8).length > MAX_BYTES) {
            throw new IllegalArgumentException("peer evidence exceeds maximum JSON byte size");
        }
        return snapshot;
    }

    private static Object snapshot(Object value, int depth, int[] count) {
        if (depth > MAX_DEPTH) throw new IllegalArgumentException("peer evidence exceeds maximum JSON depth");
        if (++count[0] > MAX_VALUES) throw new IllegalArgumentException("peer evidence exceeds maximum JSON value count");
        if (value == null || value instanceof Boolean) return value;
        if (value instanceof String text) {
            requireWellFormed(text, "JSON string");
            return text;
        }
        if (value instanceof Number number) {
            if ((number instanceof Double d && !Double.isFinite(d))
                    || (number instanceof Float f && !Float.isFinite(f))) {
                throw new IllegalArgumentException("JSON numbers must be finite");
            }
            if (!(number instanceof Byte || number instanceof Short || number instanceof Integer
                    || number instanceof Long || number instanceof Float || number instanceof Double
                    || number instanceof BigInteger || number instanceof BigDecimal)) {
                throw new IllegalArgumentException("JSON numbers must use an immutable numeric type");
            }
            return number;
        }
        if (value instanceof List<?> list) {
            List<Object> out = new ArrayList<>(list.size());
            list.forEach(item -> out.add(snapshot(item, depth + 1, count)));
            return Collections.unmodifiableList(out);
        }
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> out = new LinkedHashMap<>();
            map.forEach((key, item) -> {
                if (!(key instanceof String text)) {
                    throw new IllegalArgumentException("JSON object keys must be strings");
                }
                requireWellFormed(text, "JSON object key");
                out.put(text, snapshot(item, depth + 1, count));
            });
            return Collections.unmodifiableMap(out);
        }
        throw new IllegalArgumentException("peer evidence must contain only JSON-compatible values");
    }

    static void requireWellFormed(String value, String field) {
        for (int index = 0; index < value.length(); index++) {
            char unit = value.charAt(index);
            if (Character.isHighSurrogate(unit)) {
                if (index + 1 >= value.length() || !Character.isLowSurrogate(value.charAt(index + 1))) {
                    throw new IllegalArgumentException(field + " contains an unpaired surrogate");
                }
                index++;
            } else if (Character.isLowSurrogate(unit)) {
                throw new IllegalArgumentException(field + " contains an unpaired surrogate");
            }
        }
    }

    static String canonicalJson(Map<String, Object> value) {
        try {
            return JSON.writeValueAsString(sorted(value));
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("could not encode peer evidence", e);
        }
    }

    private static Object sorted(Object value) {
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> out = new TreeMap<>(JsonValues::compareUtf8);
            map.forEach((key, item) -> out.put((String) key, sorted(item)));
            return out;
        }
        if (value instanceof List<?> list) return list.stream().map(JsonValues::sorted).toList();
        return value;
    }

    private static int compareUtf8(String left, String right) {
        return java.util.Arrays.compareUnsigned(
                left.getBytes(java.nio.charset.StandardCharsets.UTF_8),
                right.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }
}
