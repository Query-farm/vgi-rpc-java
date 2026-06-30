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
import java.util.Objects;

/**
 * POSIX-backed {@link Shm}: a named shared-memory region created with
 * {@code shm_open} and mapped with {@code mmap} via the {@code java.lang.foreign}
 * FFM API (GA in JDK&nbsp;22), so this class is compiled into the Java&nbsp;22
 * multi-release overlay and only loaded on JDK&nbsp;&ge;&nbsp;22. The portable
 * allocator/header/channel logic lives in {@link MappedShm}; this subclass
 * supplies only the libc bindings and segment create/attach/map/unmap.
 *
 * <p>The worker is always an <em>attacher</em>: the C++ client creates and owns
 * the segment (and {@code shm_unlink}s it); {@link #attach} maps it read-write
 * and never unlinks. {@link #create} exists for unit tests. The Windows peer is
 * {@link WinShm}.</p>
 */
public final class FfmShm extends MappedShm {

    // --- libc bindings (FFM) ---------------------------------------------

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

    private final int fd;

    private FfmShm(String name, Arena arena, MemorySegment mapped, int fd,
                   long size, long dataEnd, boolean owner) {
        super(name, arena, mapped, size, dataEnd, owner);
        this.fd = fd;
    }

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
