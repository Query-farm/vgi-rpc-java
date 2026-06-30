// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.shm;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.Linker;
import java.lang.foreign.SymbolLookup;

/**
 * Java&nbsp;22+ overlay of {@link ShmFactory} (packaged under
 * {@code META-INF/versions/22}). On a JDK&nbsp;&ge;&nbsp;22 runtime this class
 * shadows the baseline no-op and returns a real FFM-backed segment; the public
 * API is identical to the baseline so multi-release loading is transparent to
 * callers.
 *
 * <p>FFM is GA on every JDK&nbsp;&ge;&nbsp;22, and the segment primitive is
 * implemented per platform: POSIX {@code shm_open}/{@code mmap} ({@link FfmShm})
 * on Unix and page-file-backed {@code CreateFileMapping}/{@code MapViewOfFile}
 * ({@link WinShm}) on Windows. Availability is probed via symbol lookup
 * <em>without</em> referencing either implementation class, so neither one's
 * symbol-binding static initializer (which would fail with an
 * {@link ExceptionInInitializerError} on a runtime missing its symbols) is
 * triggered on a platform that can't support it. On any other runtime (e.g. a
 * sandbox with native access denied) the probe reports shm <em>unavailable</em>
 * and {@link #attach} returns {@code null} — the same contract as the
 * Java&nbsp;21 baseline.</p>
 */
public final class ShmFactory {

    private ShmFactory() {}

    private static final boolean IS_WINDOWS =
            System.getProperty("os.name", "").toLowerCase().contains("win");

    // POSIX shared memory present? Probed via the default lookup so FfmShm — and
    // its orElseThrow() symbol bindings — are never loaded on a non-POSIX JVM.
    private static final boolean POSIX_SHM = !IS_WINDOWS
            && Linker.nativeLinker().defaultLookup().find("shm_open").isPresent();

    // Windows named file mappings present (and native access permitted)? Probed
    // by loading kernel32 directly so WinShm — and its kernel32 bindings — are
    // never loaded on a non-Windows JVM. A native-access denial makes the probe
    // report unavailable rather than crash.
    private static final boolean WINDOWS_SHM = IS_WINDOWS && probeWindows();

    private static boolean probeWindows() {
        try {
            return SymbolLookup.libraryLookup("kernel32", Arena.global())
                    .find("CreateFileMappingW").isPresent();
        } catch (Throwable t) {
            return false;
        }
    }

    /** True iff a real (FFM-backed) shared-memory implementation is active. */
    public static boolean available() {
        return POSIX_SHM || WINDOWS_SHM;
    }

    /**
     * Attach to the segment named {@code name} (the slash-free advertised name)
     * of {@code advertisedSize} bytes, or {@code null} when shared memory is
     * unavailable on this runtime. Throws on a real attach failure (caller falls
     * back to inline transfer).
     */
    public static Shm attach(String name, long advertisedSize) throws IOException {
        if (WINDOWS_SHM) return WinShm.attach(name, advertisedSize);
        if (POSIX_SHM) return FfmShm.attach(name, advertisedSize);
        return null;
    }
}
