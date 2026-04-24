// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.benchmark;

import farm.query.vgirpc.ExchangeState;
import farm.query.vgirpc.ProducerState;
import farm.query.vgirpc.Stream;

import java.util.List;
import java.util.Map;

/**
 * Benchmark fixture — mirrors the methods registered by the Python and Go
 * benchmark workers so cross-language perf comparisons run against identical
 * schemas.
 */
public interface BenchmarkService {

    /** No-op unary call; measures pure RPC overhead. */
    void noop();

    /** Add two doubles. */
    double add(double a, double b);

    /** Return {@code "Hello, {name}!"}. */
    String greet(String name);

    /** Exercises enum + map + list round-trip; output format matches the Python fixture. */
    String roundtrip_types(Color color, Map<String, Long> mapping, List<Long> tags);

    /** Producer stream yielding {@code count} rows of {@code (i, value=i*2)}. */
    Stream<? extends ProducerState> generate(long count);

    /** Exchange stream multiplying float64 values by {@code factor}. */
    Stream<? extends ExchangeState> transform(double factor);
}
