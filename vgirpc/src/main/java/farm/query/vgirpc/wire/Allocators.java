// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.wire;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

/**
 * Process-wide root Arrow allocator. Capped to 1 GiB by default so a runaway
 * allocation surfaces as a clean Arrow OOM rather than dragging the JVM heap
 * down. Override at startup with the {@code vgi_rpc.allocator_limit_bytes}
 * system property (e.g. {@code -Dvgi_rpc.allocator_limit_bytes=4294967296}
 * for 4 GiB). Production deployments should pick a value sized to expected
 * peak concurrent batches, not "infinity".
 */
public final class Allocators {

    /** Default cap. */
    public static final long DEFAULT_LIMIT_BYTES = 1L << 30;
    /** System property name for overriding {@link #DEFAULT_LIMIT_BYTES}. */
    public static final String LIMIT_PROPERTY = "vgi_rpc.allocator_limit_bytes";

    private static final BufferAllocator ROOT = new RootAllocator(parseLimit());

    private Allocators() {}

    public static BufferAllocator root() { return ROOT; }

    private static long parseLimit() {
        String prop = System.getProperty(LIMIT_PROPERTY);
        if (prop == null || prop.isBlank()) return DEFAULT_LIMIT_BYTES;
        try {
            long v = Long.parseLong(prop.trim());
            if (v <= 0) return DEFAULT_LIMIT_BYTES;
            return v;
        } catch (NumberFormatException e) {
            return DEFAULT_LIMIT_BYTES;
        }
    }
}
