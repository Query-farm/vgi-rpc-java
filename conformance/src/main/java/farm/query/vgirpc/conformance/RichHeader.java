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
 * Multi-type stream header used in the conformance suite. Every field must be
 * reproducible from the deterministic {@code build(seed)} factory so that
 * cross-language implementations emit byte-identical headers for the same seed.
 */
public record RichHeader(
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
        List<List<Long>> nested_list,
        @ArrowField(ArrowFieldType.INT32) int annotated_int32,
        @ArrowField(ArrowFieldType.FLOAT32) float annotated_float32,
        Map<String, String> dict_str_str
) implements ArrowSerializableRecord {

    private static final Status[] STATUS_CYCLE = { Status.PENDING, Status.ACTIVE, Status.CLOSED };

    /** Deterministic factory matching the Python reference implementation. */
    public static RichHeader build(long seed) {
        byte[] bytes = new byte[] {
                (byte) Math.floorMod(seed, 256),
                (byte) Math.floorMod(seed + 1, 256),
                (byte) Math.floorMod(seed + 2, 256)
        };
        java.util.LinkedHashMap<String, Long> dict = new java.util.LinkedHashMap<>(2);
        dict.put("a", seed);
        dict.put("b", seed + 1);
        java.util.LinkedHashMap<String, String> dictStr = new java.util.LinkedHashMap<>(1);
        dictStr.put("key", "val-" + seed);
        return new RichHeader(
                "seed-" + seed,
                bytes,
                seed * 7L,
                seed * 1.5,
                seed % 2 == 0,
                List.of(seed, seed + 1, seed + 2),
                List.of("item-" + seed, "item-" + (seed + 1)),
                dict,
                STATUS_CYCLE[(int) Math.floorMod(seed, 3)],
                new Point((double) seed, (double) (seed * 2)),
                seed % 2 == 0 ? Optional.of("opt-" + seed) : Optional.empty(),
                seed % 2 == 1 ? Optional.of(seed * 3) : Optional.empty(),
                seed % 3 == 0 ? Optional.of(new Point((double) seed, 0.0)) : Optional.empty(),
                List.of(new Point((double) seed, (double) (seed + 1))),
                List.of(List.of(seed, seed + 1), List.of(seed + 2)),
                (int) Math.floorMod(seed, 1000),
                (float) (seed / 3.0),
                dictStr
        );
    }
}
