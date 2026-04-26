// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.conformance;

import farm.query.vgirpc.schema.ArrowField;
import farm.query.vgirpc.schema.ArrowFieldType;
import farm.query.vgirpc.schema.ArrowSerializableRecord;

import java.util.List;
import java.util.Optional;

/**
 * Multi-level nested containers and dictionary-encoded strings. Matches the
 * Python {@code DeepNested} dataclass.
 */
public record DeepNested(
        @ArrowField(ArrowFieldType.DECIMAL128_20_4) List<List<byte[]>> list_of_lists_decimal,
        @ArrowField(ArrowFieldType.DATE32) Optional<List<Integer>> optional_list_date,
        @ArrowField(ArrowFieldType.DICT_INT16_UTF8) String dict_encoded_string,
        @ArrowField(ArrowFieldType.DICT_INT16_UTF8) List<String> list_of_dict_encoded
) implements ArrowSerializableRecord {}
