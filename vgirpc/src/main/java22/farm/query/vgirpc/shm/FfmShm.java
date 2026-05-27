// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.shm;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * FFM-backed {@link Shm}: a POSIX named shared-memory region exposing a
 * bump-pointer allocator stored in a fixed-size header. Wire-compatible with the
 * C++ ({@code vgi_shm_segment}), Go ({@code vgirpc/shm.go}) and Python
 * ({@code vgi_rpc.shm}) implementations:
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
 * <p>All integers little-endian. Backed by POSIX {@code shm_open} + {@code mmap}
 * via the {@code java.lang.foreign} FFM API (GA in JDK&nbsp;22), so this class
 * is compiled into the Java&nbsp;22 multi-release overlay and only loaded on
 * JDK&nbsp;&ge;&nbsp;22. The worker is always an <em>attacher</em>: the C++
 * client creates and owns the segment (and {@code shm_unlink}s it);
 * {@link #attach} maps it read-write and never unlinks. {@link #create} exists
 * for unit tests.</p>
 */
public final class FfmShm extends Shm {

    private static final byte[] MAGIC = {'V', 'G', 'I', 'S'};
    private static final int VERSION = 1;
    private static final int HEADER_FIXED_LEN = 24;     // magic(4)+ver(4)+data_size(8)+n(4)+pad(4)
    private static final int ALLOC_ENTRY_LEN = 16;      // offset(8) + length(8)
    public static final int MAX_ALLOCS = (HEADER_SIZE - HEADER_FIXED_LEN) / ALLOC_ENTRY_LEN;

    // --- libc bindings (FFM) ---------------------------------------------

    private static final boolean DEBUG = System.getenv("VGI_RPC_SHM_DEBUG") != null;
    private static final boolean IS_MAC =
            System.getProperty("os.name", "").toLowerCase().contains("mac");

    // open(2) flags diverge between Linux and Darwin; O_RDWR is identical.
    private static final int O_RDWR = 0x0002;
    private static final int O_CREAT = IS_MAC ? 0x0200 : 0x40;
    private static final int O_EXCL = IS_MAC ? 0x0800 : 0x80;
    // mmap(2) prot/flags are identical on both platforms.
    private static final int PROT_READ = 0x1;
    private static final int PROT_WRITE = 0x2;
    private static final int MAP_SHARED = 0x1;

    private static final ValueLayout.OfInt I32 =
            ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    private static final ValueLayout.OfLong I64 =
            ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

    private static final Linker LINKER = Linker.nativeLinker();
    private static final SymbolLookup LIBC = LINKER.defaultLookup();
    private static final MemoryLayout CAPTURE = Linker.Option.captureStateLayout();
    private static final VarHandle ERRNO =
            CAPTURE.varHandle(MemoryLayout.PathElement.groupElement("errno"));

    // int shm_open(const char *name, int oflag, mode_t mode)  [mode is variadic]
    private static final MethodHandle SHM_OPEN = LINKER.downcallHandle(
            LIBC.find("shm_open").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_INT,
                    ValueLayout.ADDRESS, ValueLayout.JAVA_INT, ValueLayout.JAVA_INT),
            Linker.Option.captureCallState("errno"),
            Linker.Option.firstVariadicArg(2));   // mode_t passed variadically (aarch64 ABI)
    // int shm_unlink(const char *name)
    private static final MethodHandle SHM_UNLINK = LINKER.downcallHandle(
            LIBC.find("shm_unlink").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));
    // int ftruncate(int fd, off_t length)
    private static final MethodHandle FTRUNCATE = LINKER.downcallHandle(
            LIBC.find("ftruncate").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG),
            Linker.Option.captureCallState("errno"));
    // void *mmap(void *addr, size_t len, int prot, int flags, int fd, off_t off)
    private static final MethodHandle MMAP = LINKER.downcallHandle(
            LIBC.find("mmap").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.ADDRESS,
                    ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.JAVA_INT,
                    ValueLayout.JAVA_INT, ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG),
            Linker.Option.captureCallState("errno"));
    // int munmap(void *addr, size_t len)
    private static final MethodHandle MUNMAP = LINKER.downcallHandle(
            LIBC.find("munmap").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));
    // int close(int fd)
    private static final MethodHandle CLOSE = LINKER.downcallHandle(
            LIBC.find("close").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_INT));

    // --- instance state --------------------------------------------------

    private final String name;          // POSIX name without leading slash
    private final Arena arena;
    private final MemorySegment mapped;  // sized view of the mapping
    private final long mappedAddr;
    private final int fd;
    private final long size;            // mapped length in bytes
    private final long dataEnd;         // HEADER_SIZE + data_size (allocator bound)
    private final boolean owner;
    private volatile boolean closed;

    private FfmShm(String name, Arena arena, MemorySegment mapped, int fd,
                   long size, long dataEnd, boolean owner) {
        this.name = name;
        this.arena = arena;
        this.mapped = mapped;
        this.mappedAddr = mapped.address();
        this.fd = fd;
        this.size = size;
        this.dataEnd = dataEnd;
        this.owner = owner;
    }

    @Override public String name() { return name; }
    @Override public long size() { return size; }

    @Override public long addressAt(long offset) { return mappedAddr + offset; }

    // --- create / attach -------------------------------------------------

    /** Create a fresh segment of {@code size} bytes (unit-test/owner path). */
    public static FfmShm create(String name, long size) throws IOException {
        Objects.requireNonNull(name, "name");
        if (size <= HEADER_SIZE) {
            throw new IllegalArgumentException("size must exceed HEADER_SIZE=" + HEADER_SIZE);
        }
        Arena arena = Arena.ofShared();
        try {
            MemorySegment cName = arena.allocateFrom(withSlash(name));
            MemorySegment cap = arena.allocate(CAPTURE);
            int fd = (int) SHM_OPEN.invoke(cap, cName, O_RDWR | O_CREAT | O_EXCL, 0600);
            if (fd < 0) throw new IOException("shm_open(create " + name + ") errno=" + errno(cap));
            try {
                if ((int) FTRUNCATE.invoke(cap, fd, size) < 0) {
                    throw new IOException("ftruncate(" + name + ", " + size + ") errno=" + errno(cap));
                }
                MemorySegment mapped = doMmap(arena, cap, size, fd, name);
                FfmShm seg = new FfmShm(name, arena, mapped, fd, size, size, true);
                seg.writeHeader(0, size - HEADER_SIZE);
                return seg;
            } catch (Throwable t) {
                CLOSE.invoke(fd);
                throw t;
            }
        } catch (IOException e) {
            arena.close();
            throw e;
        } catch (Throwable t) {
            arena.close();
            throw new IOException("shm create failed: " + t, t);
        }
    }

    /**
     * Attach (read-write) to an existing segment advertised by a peer. {@code name}
     * is the POSIX name <em>without</em> leading slash (as the C++ client sends it);
     * {@code advertisedSize} is the peer's mapped (page-rounded) byte size.
     */
    public static FfmShm attach(String name, long advertisedSize) throws IOException {
        Objects.requireNonNull(name, "name");
        Arena arena = Arena.ofShared();
        try {
            MemorySegment cName = arena.allocateFrom(withSlash(name));
            MemorySegment cap = arena.allocate(CAPTURE);
            int fd = (int) SHM_OPEN.invoke(cap, cName, O_RDWR, 0);
            if (fd < 0) throw new IOException("shm_open(attach " + name + ") errno=" + errno(cap));
            try {
                long mapLen = Math.max(advertisedSize, HEADER_SIZE);
                MemorySegment mapped = doMmap(arena, cap, mapLen, fd, name);
                validateMagicVersion(mapped, name);
                long dataSize = mapped.get(I64, 8);
                long needed = HEADER_SIZE + dataSize;
                if (needed > mapLen) {
                    // Peer's page-rounded size exceeded our guess; re-map larger.
                    munmap(mapped.address(), mapLen);
                    mapped = doMmap(arena, cap, needed, fd, name);
                    mapLen = needed;
                }
                FfmShm seg = new FfmShm(name, arena, mapped, fd, mapLen, needed, false);
                if (DEBUG) {
                    System.err.println("[vgi-shm] attached " + name + " size=" + mapLen
                            + " data_size=" + dataSize);
                }
                return seg;
            } catch (Throwable t) {
                CLOSE.invoke(fd);
                throw t;
            }
        } catch (IOException e) {
            arena.close();
            throw e;
        } catch (Throwable t) {
            arena.close();
            throw new IOException("shm attach(" + name + ") failed: " + t, t);
        }
    }

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

    private void writeHeader(int numAllocs, long dataSize) {
        MemorySegment.copy(MAGIC, 0, mapped, ValueLayout.JAVA_BYTE, 0, 4);
        mapped.set(I32, 4, VERSION);
        mapped.set(I64, 8, dataSize);
        mapped.set(I32, 16, numAllocs);
        mapped.set(I32, 20, 0);
    }

    private static void validateMagicVersion(MemorySegment m, String name) throws IOException {
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

    // --- FFM plumbing ----------------------------------------------------

    private static String withSlash(String name) {
        return name.startsWith("/") ? name : "/" + name;
    }

    private static int errno(MemorySegment cap) {
        return (int) ERRNO.get(cap, 0L);
    }

    private static MemorySegment doMmap(Arena arena, MemorySegment cap, long len, int fd, String name)
            throws Throwable {
        MemorySegment addr = (MemorySegment) MMAP.invoke(
                cap, MemorySegment.NULL, len, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0L);
        if (addr.address() == -1L) {           // MAP_FAILED
            throw new IOException("mmap(" + name + ", " + len + ") errno=" + errno(cap));
        }
        return addr.reinterpret(len, arena, null);
    }

    private static void munmap(long addr, long len) throws Throwable {
        MUNMAP.invoke(MemorySegment.ofAddress(addr), len);
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        try { munmap(mappedAddr, size); } catch (Throwable ignore) {}
        try { CLOSE.invoke(fd); } catch (Throwable ignore) {}
        if (owner) {
            try (Arena a = Arena.ofConfined()) {
                SHM_UNLINK.invoke(a.allocateFrom(withSlash(name)));
            } catch (Throwable ignore) {}
        }
        try { arena.close(); } catch (Throwable ignore) {}
    }
}
