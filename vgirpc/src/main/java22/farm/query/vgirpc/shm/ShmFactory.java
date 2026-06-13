// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.shm;

import java.io.IOException;
import java.lang.foreign.Linker;

/**
 * Java&nbsp;22+ overlay of {@link ShmFactory} (packaged under
 * {@code META-INF/versions/22}). On a JDK&nbsp;&ge;&nbsp;22 runtime this class
 * shadows the baseline no-op and returns a real FFM-backed {@link FfmShm}; the
 * public API is identical to the baseline so multi-release loading is
 * transparent to callers.
 *
 * <p>FFM is GA on every JDK&nbsp;&ge;&nbsp;22, but the segment primitive is
 * POSIX-only ({@code shm_open}/{@code mmap}). On a non-POSIX runtime (notably
 * Windows) those symbols are absent from the default linker lookup, so this
 * overlay reports shm <em>unavailable</em> and {@link #attach} returns
 * {@code null} — the same contract as the Java&nbsp;21 baseline. The probe uses
 * the linker's default lookup directly and never references {@link FfmShm}, so
 * its symbol-binding static initializer (which would fail on Windows with an
 * {@link ExceptionInInitializerError}, an {@code Error} the callers' {@code
 * catch (Exception)} would miss) is never triggered on a runtime that can't
 * support it.</p>
 */
public final class ShmFactory {

    private ShmFactory() {}

    // POSIX shared memory present? Probed via the default lookup so FfmShm — and
    // its orElseThrow() symbol bindings — are never loaded on a non-POSIX JVM.
    private static final boolean POSIX_SHM =
            Linker.nativeLinker().defaultLookup().find("shm_open").isPresent();

    /** True iff a real (FFM-backed) shared-memory implementation is active. */
    public static boolean available() {
        return POSIX_SHM;
    }

    /**
     * Attach to the segment named {@code name} (POSIX name without leading
     * slash) of {@code advertisedSize} bytes via {@code shm_open}+{@code mmap},
     * or {@code null} when POSIX shm is unavailable on this runtime (e.g.
     * Windows). Throws on a real attach failure (caller falls back to inline
     * transfer).
     */
    public static Shm attach(String name, long advertisedSize) throws IOException {
        if (!POSIX_SHM) return null;
        return FfmShm.attach(name, advertisedSize);
    }
}
