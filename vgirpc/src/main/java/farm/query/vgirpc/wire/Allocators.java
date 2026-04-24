// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.wire;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

/** Process-wide root Arrow allocator. */
public final class Allocators {

    private static final BufferAllocator ROOT =
            new RootAllocator(Long.MAX_VALUE);

    private Allocators() {}

    public static BufferAllocator root() { return ROOT; }
}
