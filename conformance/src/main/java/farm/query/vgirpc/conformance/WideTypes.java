// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.conformance;

import farm.query.vgirpc.schema.ArrowField;
import farm.query.vgirpc.schema.ArrowFieldType;
import farm.query.vgirpc.schema.ArrowSerializableRecord;

/**
 * Mirrors the Python {@code WideTypes} dataclass: every Arrow primitive width
 * carried as a record component. Fields use explicit {@link ArrowField}
 * overrides because the Java default inference (e.g. {@code long → int64})
 * cannot pick the narrower physical types alone.
 */
public record WideTypes(
        @ArrowField(ArrowFieldType.INT8) long int8_field,
        @ArrowField(ArrowFieldType.INT16) long int16_field,
        @ArrowField(ArrowFieldType.INT32) int int32_field,
        @ArrowField(ArrowFieldType.UINT8) long uint8_field,
        @ArrowField(ArrowFieldType.UINT16) long uint16_field,
        @ArrowField(ArrowFieldType.UINT32) long uint32_field,
        @ArrowField(ArrowFieldType.UINT64) long uint64_field,
        @ArrowField(ArrowFieldType.FLOAT32) float float32_field,
        @ArrowField(ArrowFieldType.DATE32) int date_field,
        @ArrowField(ArrowFieldType.TIMESTAMP_US) long timestamp_field,
        @ArrowField(ArrowFieldType.TIMESTAMP_US_UTC) long timestamp_utc_field,
        @ArrowField(ArrowFieldType.TIME64_US) long time_field,
        @ArrowField(ArrowFieldType.DURATION_US) long duration_field,
        @ArrowField(ArrowFieldType.DECIMAL128_20_4) byte[] decimal_field,
        @ArrowField(ArrowFieldType.LARGE_UTF8) String large_string_field,
        @ArrowField(ArrowFieldType.LARGE_BINARY) byte[] large_binary_field,
        @ArrowField(ArrowFieldType.BINARY_8) byte[] fixed_binary_field
) implements ArrowSerializableRecord {}
