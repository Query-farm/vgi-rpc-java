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
 * Wide Arrow primitives nested inside list/map/optional containers. Lists
 * carry the wide type via the same {@link ArrowField} override used for
 * scalar parameters; the schema-derivation walk applies the override to the
 * list's element type.
 */
public record ContainerWideTypes(
        @ArrowField(ArrowFieldType.DECIMAL128_20_4) List<byte[]> list_decimal,
        @ArrowField(ArrowFieldType.DATE32) List<Integer> list_date,
        @ArrowField(ArrowFieldType.TIMESTAMP_US) List<Long> list_timestamp,
        @ArrowField(ArrowFieldType.DATE32) Optional<Integer> optional_date,
        @ArrowField(ArrowFieldType.DECIMAL128_20_4) Optional<byte[]> optional_decimal,
        @ArrowField(ArrowFieldType.TIMESTAMP_US) Optional<Long> optional_timestamp,
        @ArrowField(ArrowFieldType.DECIMAL128_20_4) Map<String, byte[]> dict_str_decimal,
        List<Long> frozenset_int,
        List<Long> list_optional_int
) implements ArrowSerializableRecord {}
