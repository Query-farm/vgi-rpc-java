// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.shm;

import java.nio.channels.ReadableByteChannel;

/**
 * Abstract handle to a POSIX named shared-memory region — the transport-neutral
 * API the rest of the worker depends on, with no {@code java.lang.foreign}
 * surface so it compiles and loads on Java 21.
 *
 * <p>The concrete FFM-backed implementation ({@code FfmShm}) lives in the
 * Java&nbsp;22 multi-release overlay and is only loaded on JDK&nbsp;&ge;&nbsp;22,
 * where {@code java.lang.foreign} is GA. On Java&nbsp;21 {@link ShmFactory}
 * returns {@code null} and callers fall back to the inline (pipe) transport.
 * shm is an optimisation, never a requirement.</p>
 *
 * <p>The usage counters are kept here as plain mutable fields (the segment is
 * touched by a single thread under the lockstep protocol) so the hot path in
 * {@code RpcServer} stays a bare field increment regardless of which
 * implementation is active.</p>
 */
public abstract class Shm implements AutoCloseable {

    /** Fixed header size; the allocator hands out offsets at or beyond this. */
    public static final int HEADER_SIZE = 65_536;

    /**
     * Operator kill-switch: {@code VGI_RPC_SHM_DISABLE=1} forces the shared-memory
     * side-channel off regardless of runtime support. The worker then advertises
     * {@code vgi_rpc.transport.shm=false} and falls back to inline (pipe)
     * transfer, exactly as on a Java&nbsp;21 runtime. Useful where POSIX shm is
     * unavailable or restricted (small/absent {@code /dev/shm}, sandboxes) and
     * for exercising the no-shm path on a JDK&nbsp;&ge;&nbsp;22 runtime.
     */
    public static boolean disabledByEnv() {
        return "1".equals(System.getenv("VGI_RPC_SHM_DISABLE"));
    }

    // Worker-side usage counters for this connection. "inline-eligible" = a data
    // batch that *should* have used shm (non-empty, non-dict) but didn't — the
    // signal that something leaked to the pipe.
    public long inShmBatches, inShmBytes, inInlineDataBatches;
    public long outShmBatches, outShmBytes, outInlineEligibleBatches;
    // Lockstep timeline (nanos): worker busy (resolve+process+emit) vs idle
    // (blocked waiting for the client = client work + handoff).
    public long busyNs, idleNs, resolveNs, processNs, emitNs;

    /** POSIX name without leading slash. */
    public abstract String name();

    /** Mapped length in bytes. */
    public abstract long size();

    /** Native address of byte {@code offset} within the mapping — used to wrap a
     *  segment region as a foreign Arrow buffer for zero-copy inbound decode. */
    public abstract long addressAt(long offset);

    /** Allocate {@code length} bytes; returns the absolute offset in the segment. */
    public abstract long allocate(long length);

    /** Free an allocation by its offset; no-op if no allocation matches. */
    public abstract void free(long offset);

    /** Write {@code data} into the segment at {@code offset}. */
    public abstract void writeAt(long offset, byte[] data);

    /** Read {@code length} bytes starting at {@code offset}. */
    public abstract byte[] readAt(long offset, long length);

    /** A write channel into {@code [offset, offset+capacity)} that reports its
     *  exact written length — handed straight to Arrow's {@code WriteChannel}. */
    public abstract ShmWriteChannel writeChannelAt(long offset, long capacity);

    /** A read channel over {@code [offset, offset+length)} for Arrow's
     *  {@code ReadChannel}. */
    public abstract ReadableByteChannel readChannelAt(long offset, long length);

    @Override
    public abstract void close();
}
