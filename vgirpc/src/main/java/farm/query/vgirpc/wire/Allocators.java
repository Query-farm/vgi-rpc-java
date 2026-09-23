// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.wire;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.memory.VgiPooledAllocators;

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

    private static final BufferAllocator ROOT = buildRoot();

    private Allocators() {}

    /** @return the shared process-wide root {@link BufferAllocator}. */
    public static BufferAllocator root() { return ROOT; }

    /** Netty's switch for {@code sun.misc.Unsafe}; see {@link #defaultNettyToUnsafe()}. */
    static final String NETTY_NO_UNSAFE_PROPERTY = "io.netty.noUnsafe";

    private static BufferAllocator buildRoot() {
        defaultNettyToUnsafe();
        long limit = parseLimit();
        // Pooled, non-zeroing allocator for large buffers is ON by default: it
        // removes the ~35% of worker CPU the JDK spends zeroing fresh direct
        // buffers that are then immediately overwritten (shm resolve body + large
        // output buffers), with no correctness change across the integration
        // suite. Disable with VGI_RPC_POOLED_ALLOC=0. See PooledDirectAllocator.
        if (!"0".equals(System.getenv("VGI_RPC_POOLED_ALLOC"))) {
            return VgiPooledAllocators.create(limit, PooledDirectAllocator.FACTORY);
        }
        return new RootAllocator(limit);
    }

    /**
     * Arrow 19 allocates through Netty 4.2, which turns {@code sun.misc.Unsafe}
     * off by default on Java 25+ unless {@code io.netty.noUnsafe} or
     * {@code --sun-misc-unsafe-memory-access} is set explicitly. Arrow's
     * {@code NettyAllocationManager} cannot run without it: its static
     * initializer calls {@code memoryAddress()} and every allocator in the
     * process then fails with an opaque {@code ExceptionInInitializerError}
     * (apache/arrow-java#728). Upstream's answer is a JVM flag; setting the
     * same property here, before this class first touches Arrow, means a
     * worker or client that forgot the flag still starts. An explicit choice
     * by the operator — either property — is left alone.
     *
     * <p>Netty reads the property once, in {@code PlatformDependent0}'s static
     * initializer, so this only helps when nothing in the process loaded Netty
     * or an Arrow allocator first. Launchers should still pass
     * {@code -Dio.netty.noUnsafe=false}.</p>
     */
    private static void defaultNettyToUnsafe() {
        if (System.getProperty(NETTY_NO_UNSAFE_PROPERTY) == null
                && System.getProperty("sun.misc.unsafe.memory.access") == null) {
            System.setProperty(NETTY_NO_UNSAFE_PROPERTY, "false");
        }
    }

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
