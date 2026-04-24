// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.conformance;

import farm.query.vgirpc.schema.ArrowField;
import farm.query.vgirpc.schema.ArrowFieldType;
import farm.query.vgirpc.schema.ArrowSerializableRecord;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Exercises every supported Arrow type mapping. Mirrors the Python conformance
 * {@code AllTypes} dataclass so cross-language round-trips verify each coercion.
 */
public record AllTypes(
        String str_field,
        byte[] bytes_field,
        long int_field,
        double float_field,
        boolean bool_field,
        List<Long> list_of_int,
        List<String> list_of_str,
        Map<String, Long> dict_field,
        Status enum_field,
        Point nested_point,
        Optional<String> optional_str,
        Optional<Long> optional_int,
        Optional<Point> optional_nested,
        List<Point> list_of_nested,
        @ArrowField(ArrowFieldType.INT32) int annotated_int32,
        @ArrowField(ArrowFieldType.FLOAT32) float annotated_float32,
        List<List<Long>> nested_list,
        Map<String, String> dict_str_str
) implements ArrowSerializableRecord {}
