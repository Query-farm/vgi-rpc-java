// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.wire;

import org.apache.arrow.memory.AllocationManager;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.netty.NettyAllocationManager;
import org.apache.arrow.memory.util.MemoryUtil;

import java.util.ArrayDeque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Pooled, non-zeroing {@link AllocationManager.Factory} for large buffers.
 *
 * <p>Arrow's default (Netty) routes any buffer larger than its chunk size
 * through {@link java.nio.ByteBuffer#allocateDirect}, which the JDK zeroes
 * ({@code java.nio.Bits.setMemory}) before the caller overwrites every byte. A
 * JFR profile of the shm worker showed <b>~35% of CPU</b> in that zeroing —
 * split between the {@code resolve} body buffer (immediately overwritten by the
 * shm→buffer copy) and large output data buffers (immediately overwritten by
 * the function). Both are pure waste.</p>
 *
 * <p>This factory, for allocations {@code >= MIN_POOLED}:
 * <ul>
 *   <li>allocates via {@link MemoryUtil#allocateMemory} (Unsafe) — <b>not</b>
 *       zeroed;</li>
 *   <li>on free, returns the block to a size-keyed free list instead of the OS,
 *       so the next same-size batch reuses it — no syscall, no re-zero.</li>
 * </ul>
 * Smaller buffers delegate to {@link NettyAllocationManager} unchanged, so
 * validity/offset buffers (which may rely on zeroed memory) keep their default
 * behaviour. Bounded by a retained-bytes cap; blocks beyond it are freed.</p>
 *
 * <p>On by default; disable with {@code VGI_RPC_POOLED_ALLOC=0}. Memory handed
 * out is uninitialised, so it is only correct when callers fully populate their
 * large buffers (the VGI batch round trip does: deserialize copies the whole
 * body in; a function writes every output value). Validated against the
 * integration suite (results byte-identical to the zeroing path).</p>
 */
public final class PooledDirectAllocator {

    private PooledDirectAllocator() {}

    /** Below this, delegate to Netty (don't hand out uninitialised memory). */
    private static final long MIN_POOLED = Long.getLong("vgi_rpc.pool_min_bytes", 1L << 20);
    /** Cap on freed-but-retained native bytes; beyond it, release to the OS. */
    private static final long RETAIN_CAP = Long.getLong("vgi_rpc.pool_retain_bytes", 2L << 30);

    private static final Map<Long, ArrayDeque<Long>> POOL = new ConcurrentHashMap<>();
    private static final AtomicLong RETAINED = new AtomicLong();

    private static long acquire(long size) {
        ArrayDeque<Long> stack = POOL.get(size);
        if (stack != null) {
            Long addr;
            synchronized (stack) {
                addr = stack.pollFirst();
            }
            if (addr != null) {
                RETAINED.addAndGet(-size);
                return addr;
            }
        }
        return MemoryUtil.allocateMemory(size); // uninitialised — caller overwrites
    }

    private static void recycle(long size, long address) {
        if (RETAINED.get() + size <= RETAIN_CAP) {
            ArrayDeque<Long> stack = POOL.computeIfAbsent(size, k -> new ArrayDeque<>());
            synchronized (stack) {
                stack.addFirst(address);
            }
            RETAINED.addAndGet(size);
            return;
        }
        MemoryUtil.freeMemory(address);
    }

    public static final AllocationManager.Factory FACTORY = new AllocationManager.Factory() {
        @Override
        public AllocationManager create(BufferAllocator accountingAllocator, long size) {
            if (size < MIN_POOLED) {
                return NettyAllocationManager.FACTORY.create(accountingAllocator, size);
            }
            return new PooledManager(accountingAllocator, size);
        }

        @Override
        public ArrowBuf empty() {
            return NettyAllocationManager.FACTORY.empty();
        }
    };

    private static final class PooledManager extends AllocationManager {
        private final long size;
        private final long address;

        PooledManager(BufferAllocator allocator, long size) {
            super(allocator);
            this.size = size;
            this.address = acquire(size);
        }

        @Override
        public long getSize() {
            return size;
        }

        @Override
        protected long memoryAddress() {
            return address;
        }

        @Override
        protected void release0() {
            recycle(size, address);
        }
    }
}
