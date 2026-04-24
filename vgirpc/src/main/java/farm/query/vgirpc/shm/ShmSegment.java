// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.shm;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * POSIX-style shared memory region exposing a bump-pointer allocator stored
 * in a fixed-size header. Wire-compatible with the Python reference
 * ({@code vgi_rpc.shm}):
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
 * <p>All integers little-endian. Adjacent free regions coalesce implicitly —
 * the allocator tracks only occupied regions, so freeing a neighbour widens
 * the usable gap with no explicit coalescing step.</p>
 *
 * <p>This implementation backs the region with a normal {@link MappedByteBuffer}
 * over a file (on Linux {@code /dev/shm} gives POSIX SHM semantics; on macOS
 * use {@code /tmp}). Inter-process use requires both peers to know the same
 * file path; Python's {@code multiprocessing.shared_memory.SharedMemory}
 * places its segments under {@code /dev/shm} on Linux.</p>
 */
public final class ShmSegment implements AutoCloseable {

    public static final int HEADER_SIZE = 65_536;
    private static final byte[] MAGIC = {'V', 'G', 'I', 'S'};
    private static final int VERSION = 1;
    private static final int HEADER_FIXED_LEN = 24;     // magic(4)+ver(4)+data_size(8)+n(4)+pad(4)
    private static final int ALLOC_ENTRY_LEN = 16;      // offset(8) + length(8)
    public static final int MAX_ALLOCS = (HEADER_SIZE - HEADER_FIXED_LEN) / ALLOC_ENTRY_LEN;

    private final String name;
    private final Path file;
    private final RandomAccessFile raf;
    private final FileChannel channel;
    private final MappedByteBuffer buffer;
    private final long size;
    private final boolean owner;
    private volatile boolean closed;

    private ShmSegment(String name, Path file, RandomAccessFile raf, FileChannel channel,
                       MappedByteBuffer buffer, long size, boolean owner) {
        this.name = name;
        this.file = file;
        this.raf = raf;
        this.channel = channel;
        this.buffer = buffer;
        this.buffer.order(ByteOrder.LITTLE_ENDIAN);
        this.size = size;
        this.owner = owner;
    }

    public String name() { return name; }
    public long size() { return size; }
    public Path path() { return file; }

    /** Create (truncate + initialise) a new segment at {@code path} of {@code size} bytes. */
    public static ShmSegment create(Path path, long size) throws IOException {
        Objects.requireNonNull(path, "path");
        if (size <= HEADER_SIZE) {
            throw new IllegalArgumentException("size must exceed HEADER_SIZE=" + HEADER_SIZE);
        }
        Files.deleteIfExists(path);
        RandomAccessFile raf = new RandomAccessFile(path.toFile(), "rw");
        raf.setLength(size);
        FileChannel ch = raf.getChannel();
        MappedByteBuffer mb = ch.map(FileChannel.MapMode.READ_WRITE, 0, size);
        ShmSegment seg = new ShmSegment(path.getFileName().toString(), path, raf, ch, mb, size, true);
        seg.writeHeader(0);
        return seg;
    }

    /** Attach to an existing segment at {@code path}. */
    public static ShmSegment attach(Path path) throws IOException {
        Objects.requireNonNull(path, "path");
        RandomAccessFile raf = new RandomAccessFile(path.toFile(), "rw");
        long size = raf.length();
        FileChannel ch = raf.getChannel();
        MappedByteBuffer mb = ch.map(FileChannel.MapMode.READ_WRITE, 0, size);
        ShmSegment seg = new ShmSegment(path.getFileName().toString(), path, raf, ch, mb, size, false);
        seg.validateHeader();
        return seg;
    }

    // --- Allocation ------------------------------------------------------

    /** Allocate {@code length} bytes; returns the absolute offset in the segment. */
    public synchronized long allocate(long length) {
        if (length <= 0) throw new IllegalArgumentException("length must be positive");
        List<long[]> allocs = readAllocs();
        long dataStart = HEADER_SIZE;
        long dataEnd = size;
        // Treat allocations as occupied intervals; find the first gap large enough.
        long cursor = dataStart;
        for (long[] a : allocs) {
            long aoff = a[0], alen = a[1];
            if (aoff - cursor >= length) {
                // fits in the gap
                break;
            }
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
    public synchronized void free(long offset) {
        List<long[]> allocs = readAllocs();
        allocs.removeIf(a -> a[0] == offset);
        writeAllocs(allocs);
    }

    /** Write {@code data} into the segment at {@code offset} (must be within an allocation). */
    public void writeAt(long offset, byte[] data) {
        ByteBuffer view = buffer.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        view.position((int) offset);
        view.put(data);
    }

    /** Read {@code length} bytes starting at {@code offset}. */
    public byte[] readAt(long offset, long length) {
        byte[] out = new byte[(int) length];
        ByteBuffer view = buffer.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        view.position((int) offset);
        view.get(out);
        return out;
    }

    // --- Header helpers --------------------------------------------------

    private void writeHeader(int numAllocs) {
        ByteBuffer b = buffer.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        b.position(0);
        b.put(MAGIC);
        b.putInt(VERSION);
        b.putLong(size - HEADER_SIZE);
        b.putInt(numAllocs);
        b.putInt(0); // padding
    }

    private void validateHeader() throws IOException {
        ByteBuffer b = buffer.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        byte[] magic = new byte[4];
        b.position(0);
        b.get(magic);
        for (int i = 0; i < 4; i++) {
            if (magic[i] != MAGIC[i]) throw new IOException("bad SHM magic");
        }
        int version = b.getInt();
        if (version != VERSION) throw new IOException("unsupported SHM version: " + version);
    }

    private List<long[]> readAllocs() {
        ByteBuffer b = buffer.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        b.position(16);
        int n = b.getInt();
        b.position(HEADER_FIXED_LEN);
        List<long[]> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            long off = b.getLong();
            long len = b.getLong();
            out.add(new long[]{off, len});
        }
        return out;
    }

    private void writeAllocs(List<long[]> allocs) {
        ByteBuffer b = buffer.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        b.position(16);
        b.putInt(allocs.size());
        b.position(HEADER_FIXED_LEN);
        for (long[] a : allocs) {
            b.putLong(a[0]);
            b.putLong(a[1]);
        }
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        try { channel.close(); } catch (Exception ignore) {}
        try { raf.close(); } catch (Exception ignore) {}
        if (owner) {
            try { Files.deleteIfExists(file); } catch (Exception ignore) {}
        }
    }
}
