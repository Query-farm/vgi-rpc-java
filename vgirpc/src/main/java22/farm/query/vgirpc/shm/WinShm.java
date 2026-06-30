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
 * Windows-backed {@link Shm}: a page-file-backed named section created with
 * {@code CreateFileMappingW} and mapped with {@code MapViewOfFile}, bound from
 * {@code kernel32} via the {@code java.lang.foreign} FFM API. The portable
 * allocator/header/channel logic lives in {@link MappedShm}; this subclass
 * supplies only the kernel32 bindings and segment create/attach/map/unmap. The
 * POSIX peer is {@link FfmShm}.
 *
 * <p>Wire-compatible with the C++/Go/Rust/Python peers, which create the same
 * page-file-backed section under the slash-free advertised name. The section
 * object is reference-counted by the kernel, so closing the worker's handle does
 * not destroy it while the owner (the engine) still holds one — there is no
 * {@code unlink} step, unlike POSIX {@code shm_unlink}.</p>
 *
 * <p>Loaded only on a Windows JDK&nbsp;&ge;&nbsp;22 runtime, dispatched by
 * {@link ShmFactory}; its kernel32-binding static initializer never runs on
 * other platforms.</p>
 */
public final class WinShm extends MappedShm {

    // --- kernel32 bindings (FFM) -----------------------------------------

    private static final int PAGE_READWRITE = 0x04;
    private static final int FILE_MAP_ACCESS = 0x0002 | 0x0004;  // FILE_MAP_WRITE | FILE_MAP_READ
    private static final int ERROR_ALREADY_EXISTS = 183;
    private static final MemorySegment INVALID_HANDLE = MemorySegment.ofAddress(-1L);

    private static final Linker LINKER = Linker.nativeLinker();
    private static final SymbolLookup K32 = SymbolLookup.libraryLookup("kernel32", Arena.global());
    private static final MemoryLayout CAPTURE = Linker.Option.captureStateLayout();
    private static final VarHandle LAST_ERROR =
            CAPTURE.varHandle(MemoryLayout.PathElement.groupElement("GetLastError"));

    // HANDLE CreateFileMappingW(HANDLE hFile, LPSECURITY_ATTRIBUTES, DWORD flProtect,
    //                           DWORD dwMaximumSizeHigh, DWORD dwMaximumSizeLow, LPCWSTR lpName)
    private static final MethodHandle CREATE_FILE_MAPPING = LINKER.downcallHandle(
            K32.find("CreateFileMappingW").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.ADDRESS,
                    ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_INT,
                    ValueLayout.JAVA_INT, ValueLayout.JAVA_INT, ValueLayout.ADDRESS),
            Linker.Option.captureCallState("GetLastError"));
    // HANDLE OpenFileMappingW(DWORD dwDesiredAccess, BOOL bInheritHandle, LPCWSTR lpName)
    private static final MethodHandle OPEN_FILE_MAPPING = LINKER.downcallHandle(
            K32.find("OpenFileMappingW").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.ADDRESS,
                    ValueLayout.JAVA_INT, ValueLayout.JAVA_INT, ValueLayout.ADDRESS),
            Linker.Option.captureCallState("GetLastError"));
    // LPVOID MapViewOfFile(HANDLE, DWORD access, DWORD offHigh, DWORD offLow, SIZE_T bytes)
    private static final MethodHandle MAP_VIEW = LINKER.downcallHandle(
            K32.find("MapViewOfFile").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.ADDRESS,
                    ValueLayout.ADDRESS, ValueLayout.JAVA_INT, ValueLayout.JAVA_INT,
                    ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG),
            Linker.Option.captureCallState("GetLastError"));
    // BOOL UnmapViewOfFile(LPCVOID lpBaseAddress)
    private static final MethodHandle UNMAP_VIEW = LINKER.downcallHandle(
            K32.find("UnmapViewOfFile").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));
    // BOOL CloseHandle(HANDLE hObject)
    private static final MethodHandle CLOSE_HANDLE = LINKER.downcallHandle(
            K32.find("CloseHandle").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));

    // --- instance state --------------------------------------------------

    private final MemorySegment mapHandle;  // the file-mapping object HANDLE

    private WinShm(String name, Arena arena, MemorySegment mapped, MemorySegment mapHandle,
                   long size, long dataEnd, boolean owner) {
        super(name, arena, mapped, size, dataEnd, owner);
        this.mapHandle = mapHandle;
    }

    // --- create / attach -------------------------------------------------

    /** Create a fresh page-file-backed section of {@code size} bytes (unit-test/owner path). */
    public static WinShm create(String name, long size) throws IOException {
        Objects.requireNonNull(name, "name");
        if (size <= HEADER_SIZE) {
            throw new IllegalArgumentException("size must exceed HEADER_SIZE=" + HEADER_SIZE);
        }
        Arena arena = Arena.ofShared();
        try {
            MemorySegment wName = wide(arena, name);
            MemorySegment cap = arena.allocate(CAPTURE);
            MemorySegment h = (MemorySegment) CREATE_FILE_MAPPING.invoke(
                    cap, INVALID_HANDLE, MemorySegment.NULL, PAGE_READWRITE,
                    (int) (size >>> 32), (int) size, wName);
            if (h.address() == 0) {
                throw new IOException("CreateFileMapping(create " + name + ") error=" + lastError(cap));
            }
            // Exclusive-create semantics (mirrors POSIX O_CREAT|O_EXCL): a valid
            // handle plus ERROR_ALREADY_EXISTS means the named object pre-existed.
            if (lastError(cap) == ERROR_ALREADY_EXISTS) {
                CLOSE_HANDLE.invoke(h);
                throw new IOException("shm name already exists: " + name);
            }
            try {
                MemorySegment mapped = mapView(arena, cap, h, size, name);
                WinShm seg = new WinShm(name, arena, mapped, h, size, size, true);
                seg.writeHeader(0, size - HEADER_SIZE);
                return seg;
            } catch (Throwable t) {
                CLOSE_HANDLE.invoke(h);
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
     * Attach (read-write) to an existing section advertised by a peer. {@code name}
     * is the slash-free advertised name; {@code advertisedSize} is the peer's
     * mapped (page-rounded) byte size.
     */
    public static WinShm attach(String name, long advertisedSize) throws IOException {
        Objects.requireNonNull(name, "name");
        Arena arena = Arena.ofShared();
        try {
            MemorySegment wName = wide(arena, name);
            MemorySegment cap = arena.allocate(CAPTURE);
            MemorySegment h = (MemorySegment) OPEN_FILE_MAPPING.invoke(cap, FILE_MAP_ACCESS, 0, wName);
            if (h.address() == 0) {
                throw new IOException("OpenFileMapping(attach " + name + ") error=" + lastError(cap));
            }
            try {
                long mapLen = Math.max(advertisedSize, HEADER_SIZE);
                MemorySegment mapped = mapView(arena, cap, h, mapLen, name);
                validateMagicVersion(mapped, name);
                long dataSize = mapped.get(I64, 8);
                long needed = HEADER_SIZE + dataSize;
                if (needed > mapLen) {
                    // Peer's page-rounded size exceeded our guess; re-map larger.
                    UNMAP_VIEW.invoke(MemorySegment.ofAddress(mapped.address()));
                    mapped = mapView(arena, cap, h, needed, name);
                    mapLen = needed;
                }
                WinShm seg = new WinShm(name, arena, mapped, h, mapLen, needed, false);
                if (DEBUG) {
                    System.err.println("[vgi-shm] attached(win) " + name + " size=" + mapLen
                            + " data_size=" + dataSize);
                }
                return seg;
            } catch (Throwable t) {
                CLOSE_HANDLE.invoke(h);
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

    /** Encode {@code s} as a null-terminated UTF-16LE wide string (LPCWSTR). */
    private static MemorySegment wide(Arena arena, String s) {
        int n = s.length();
        MemorySegment seg = arena.allocate((n + 1L) * Character.BYTES);
        for (int i = 0; i < n; i++) {
            seg.setAtIndex(ValueLayout.JAVA_CHAR, i, s.charAt(i));  // Windows is little-endian
        }
        seg.setAtIndex(ValueLayout.JAVA_CHAR, n, '\0');
        return seg;
    }

    private static int lastError(MemorySegment cap) {
        return (int) LAST_ERROR.get(cap, 0L);
    }

    private static MemorySegment mapView(Arena arena, MemorySegment cap, MemorySegment h, long len, String name)
            throws Throwable {
        MemorySegment addr = (MemorySegment) MAP_VIEW.invoke(cap, h, FILE_MAP_ACCESS, 0, 0, len);
        if (addr.address() == 0) {           // NULL = failure
            throw new IOException("MapViewOfFile(" + name + ", " + len + ") error=" + lastError(cap));
        }
        return addr.reinterpret(len, arena, null);
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        try { UNMAP_VIEW.invoke(MemorySegment.ofAddress(mappedAddr)); } catch (Throwable ignore) {}
        try { CLOSE_HANDLE.invoke(mapHandle); } catch (Throwable ignore) {}
        // No unlink: the kernel reference-counts the section and frees it when
        // the last handle closes (the owning peer still holds one).
        try { arena.close(); } catch (Throwable ignore) {}
    }
}
