// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package org.apache.arrow.memory;

/**
 * Bridge into Arrow's package-private allocator config so a custom
 * {@link AllocationManager.Factory} can be installed on a {@link RootAllocator}.
 * Arrow exposes no public API for this ({@code BaseAllocator.Config} and its
 * builder are package-private), so this helper lives in
 * {@code org.apache.arrow.memory} on the classpath (split package, unnamed
 * module — permitted) purely to call {@link BaseAllocator#configBuilder()}.
 *
 * @see farm.query.vgirpc.wire.PooledDirectAllocator
 */
public final class VgiPooledAllocators {

    private VgiPooledAllocators() {}

    public static RootAllocator create(long maxAllocation, AllocationManager.Factory factory) {
        return new RootAllocator(BaseAllocator.configBuilder()
                .maxAllocation(maxAllocation)
                .allocationManagerFactory(factory)
                .build());
    }
}
