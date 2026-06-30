// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.shm;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Platform-neutral base for the FFM-backed {@link Shm} implementations. Holds
 * everything that touches only the mapped {@link MemorySegment}: the bump-pointer
 * allocator, the fixed-size header, and the read/write channels. The OS-specific
 * subclasses — {@link FfmShm} (POSIX {@code shm_open}/{@code mmap}) and
 * {@link WinShm} (Windows {@code CreateFileMapping}/{@code MapViewOfFile}) —
 * supply only the segment create/attach/map/unmap primitives and
 * {@link #close()}.
 *
 * <p>The byte layout is wire-compatible with the C++ ({@code vgi_shm_segment}),
 * Go ({@code vgirpc/shm.go}), Rust ({@code vgi-rpc::shm}) and Python
 * ({@code vgi_rpc.shm}) peers:</p>
 *
 * <pre>
 *   Offset  Size  Field
 *   0       4     magic = b"VGIS"
 *   4       4     version: uint32 = 1
 *   8       8     data_size: uint64 (segment size - HEADER_SIZE)
 *   16      4     num_allocs: uint32
 *   20      4     padding
 *   24      N*16  allocations: (offset: uint64, length: uint64), sorted by offset
 * </pre>
 *
 * <p>All integers little-endian.</p>
 */
abstract class MappedShm extends Shm {

    protected static final byte[] MAGIC = {'V', 'G', 'I', 'S'};
    protected static final int VERSION = 1;
    protected static final int HEADER_FIXED_LEN = 24;   // magic(4)+ver(4)+data_size(8)+n(4)+pad(4)
    protected static final int ALLOC_ENTRY_LEN = 16;    // offset(8) + length(8)
    public static final int MAX_ALLOCS = (HEADER_SIZE - HEADER_FIXED_LEN) / ALLOC_ENTRY_LEN;

    protected static final boolean DEBUG = System.getenv("VGI_RPC_SHM_DEBUG") != null;

    protected static final ValueLayout.OfInt I32 =
            ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    protected static final ValueLayout.OfLong I64 =
            ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

    // --- instance state --------------------------------------------------

    protected final String name;          // POSIX name without leading slash
    protected final Arena arena;
    protected final MemorySegment mapped;  // sized view of the mapping
    protected final long mappedAddr;
    protected final long size;            // mapped length in bytes
    protected final long dataEnd;         // HEADER_SIZE + data_size (allocator bound)
    protected final boolean owner;
    protected volatile boolean closed;

    protected MappedShm(String name, Arena arena, MemorySegment mapped,
                        long size, long dataEnd, boolean owner) {
        this.name = name;
        this.arena = arena;
        this.mapped = mapped;
        this.mappedAddr = mapped.address();
        this.size = size;
        this.dataEnd = dataEnd;
        this.owner = owner;
    }

    @Override public String name() { return name; }
    @Override public long size() { return size; }

    @Override public long addressAt(long offset) { return mappedAddr + offset; }

    // --- allocation ------------------------------------------------------

    /** Allocate {@code length} bytes; returns the absolute offset in the segment. */
    @Override public synchronized long allocate(long length) {
        if (length <= 0) throw new IllegalArgumentException("length must be positive");
        List<long[]> allocs = readAllocs();
        long cursor = HEADER_SIZE;
        for (long[] a : allocs) {
            long aoff = a[0], alen = a[1];
            if (aoff - cursor >= length) break;             // fits in the gap
            cursor = Math.max(cursor, aoff + alen);
        }
        if (cursor + length > dataEnd) {
            throw new IllegalStateException("ShmSegment full: requested=" + length
                    + " free=" + (dataEnd - cursor));
        }
        if (allocs.size() >= MAX_ALLOCS) {
            throw new IllegalStateException("ShmSegment allocation table full (max=" + MAX_ALLOCS + ")");
        }
        allocs.add(new long[]{cursor, length});
        allocs.sort(Comparator.comparingLong(a -> a[0]));
        writeAllocs(allocs);
        return cursor;
    }

    /** Free an allocation by its offset; no-op if no allocation matches. */
    @Override public synchronized void free(long offset) {
        List<long[]> allocs = readAllocs();
        allocs.removeIf(a -> a[0] == offset);
        writeAllocs(allocs);
    }

    /** Write {@code data} into the segment at {@code offset} (must be within an allocation). */
    @Override public void writeAt(long offset, byte[] data) {
        MemorySegment.copy(data, 0, mapped, ValueLayout.JAVA_BYTE, offset, data.length);
    }

    /** Read {@code length} bytes starting at {@code offset}. */
    @Override public byte[] readAt(long offset, long length) {
        byte[] out = new byte[(int) length];
        MemorySegment.copy(mapped, ValueLayout.JAVA_BYTE, offset, out, 0, (int) length);
        return out;
    }

    @Override public ShmWriteChannel writeChannelAt(long offset, long capacity) {
        return new SegmentWriteChannel(offset, capacity);
    }

    @Override public ReadableByteChannel readChannelAt(long offset, long length) {
        return new SegmentReadChannel(offset, length);
    }

    /**
     * A {@link ShmWriteChannel} that writes {@link ByteBuffer}s straight into
     * the mapped segment at {@code offset}, capped at {@code capacity}. Feeding
     * this directly to Arrow's {@code WriteChannel} (instead of wrapping an
     * {@code OutputStream} via {@code Channels.newChannel}) avoids the JDK
     * adapter's heap-{@code byte[]} bounce — each {@code write} is one bulk
     * {@code MemorySegment.copy} into shm. {@link #written()} reports the exact
     * serialized length after the writer closes.
     */
    public final class SegmentWriteChannel implements ShmWriteChannel {
        private final long base;
        private final long cap;
        private long pos;
        private boolean open = true;

        SegmentWriteChannel(long base, long cap) { this.base = base; this.cap = cap; }

        @Override public long written() { return pos; }

        @Override public int write(ByteBuffer src) {
            int n = src.remaining();
            if (pos + n > cap) throw new IndexOutOfBoundsException("shm write overflow");
            MemorySegment.copy(MemorySegment.ofBuffer(src), 0L, mapped, base + pos, n);
            src.position(src.position() + n);
            pos += n;
            return n;
        }

        @Override public boolean isOpen() { return open; }
        @Override public void close() { open = false; }
    }

    private final class SegmentReadChannel implements ReadableByteChannel {
        private final long base;
        private final long len;
        private long pos;
        private boolean open = true;

        SegmentReadChannel(long base, long len) { this.base = base; this.len = len; }

        @Override public int read(ByteBuffer dst) {
            if (pos >= len) return -1;
            int n = (int) Math.min(dst.remaining(), len - pos);
            if (n == 0) return 0;
            MemorySegment.copy(mapped, base + pos, MemorySegment.ofBuffer(dst), 0L, n);
            dst.position(dst.position() + n);
            pos += n;
            return n;
        }

        @Override public boolean isOpen() { return open; }
        @Override public void close() { open = false; }
    }

    // --- header helpers --------------------------------------------------

    protected void writeHeader(int numAllocs, long dataSize) {
        MemorySegment.copy(MAGIC, 0, mapped, ValueLayout.JAVA_BYTE, 0, 4);
        mapped.set(I32, 4, VERSION);
        mapped.set(I64, 8, dataSize);
        mapped.set(I32, 16, numAllocs);
        mapped.set(I32, 20, 0);
    }

    protected static void validateMagicVersion(MemorySegment m, String name) throws IOException {
        byte[] magic = new byte[4];
        MemorySegment.copy(m, ValueLayout.JAVA_BYTE, 0, magic, 0, 4);
        for (int i = 0; i < 4; i++) {
            if (magic[i] != MAGIC[i]) throw new IOException("bad SHM magic for " + name);
        }
        int version = m.get(I32, 4);
        if (version != VERSION) throw new IOException("unsupported SHM version " + version + " for " + name);
    }

    private List<long[]> readAllocs() {
        int n = mapped.get(I32, 16);
        // Corrupt num_allocs: trusting it would walk the entry table past the
        // header. Mirrors the C++ guard (vgi_shm_segment.cpp). n is read as a
        // signed int, so a high-bit value is also caught by the < 0 check.
        if (n < 0 || n > MAX_ALLOCS) {
            throw new IllegalStateException("ShmSegment corrupt header: num_allocs=" + n
                    + " exceeds max " + MAX_ALLOCS);
        }
        List<long[]> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            long base = HEADER_FIXED_LEN + (long) i * ALLOC_ENTRY_LEN;
            out.add(new long[]{mapped.get(I64, base), mapped.get(I64, base + 8)});
        }
        return out;
    }

    private void writeAllocs(List<long[]> allocs) {
        mapped.set(I32, 16, allocs.size());
        for (int i = 0; i < allocs.size(); i++) {
            long base = HEADER_FIXED_LEN + (long) i * ALLOC_ENTRY_LEN;
            mapped.set(I64, base, allocs.get(i)[0]);
            mapped.set(I64, base + 8, allocs.get(i)[1]);
        }
    }
}
