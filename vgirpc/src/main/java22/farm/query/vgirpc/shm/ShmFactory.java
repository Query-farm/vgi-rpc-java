// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.shm;

import java.io.IOException;

/**
 * Java&nbsp;22+ overlay of {@link ShmFactory} (packaged under
 * {@code META-INF/versions/22}). On a JDK&nbsp;&ge;&nbsp;22 runtime this class
 * shadows the baseline no-op and returns a real FFM-backed {@link FfmShm}; the
 * public API is identical to the baseline so multi-release loading is
 * transparent to callers.
 */
public final class ShmFactory {

    private ShmFactory() {}

    /** True iff a real (FFM-backed) shared-memory implementation is active. */
    public static boolean available() {
        return true;
    }

    /**
     * Attach to the segment named {@code name} (POSIX name without leading
     * slash) of {@code advertisedSize} bytes via {@code shm_open}+{@code mmap}.
     * Throws on a real attach failure (caller falls back to inline transfer).
     */
    public static Shm attach(String name, long advertisedSize) throws IOException {
        return FfmShm.attach(name, advertisedSize);
    }
}
