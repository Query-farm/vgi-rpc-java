// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.shm;

import java.io.IOException;

/**
 * Entry point for attaching to a peer-advertised shared-memory segment.
 *
 * <p><b>This is the Java&nbsp;21 baseline version: shm is unavailable, so
 * {@link #attach} always returns {@code null}</b> and the caller uses the
 * inline (pipe) transport. The Java&nbsp;22 multi-release overlay
 * ({@code META-INF/versions/22}) replaces this class with one that returns an
 * FFM-backed {@code FfmShm}; a JDK&nbsp;&ge;&nbsp;22 runtime loads the overlay
 * automatically, a JDK&nbsp;21 runtime loads this no-op. Because the baseline
 * never references the FFM implementation, a 21 JVM never loads FFM bytecode.</p>
 */
public final class ShmFactory {

    private ShmFactory() {}

    /** True iff a real (FFM-backed) shared-memory implementation is active. */
    public static boolean available() {
        return false;
    }

    /**
     * Attach to the segment named {@code name} (POSIX name without leading
     * slash) of {@code advertisedSize} bytes, or return {@code null} when shm is
     * unavailable on this JVM. The baseline always returns {@code null}.
     */
    public static Shm attach(String name, long advertisedSize) throws IOException {
        return null;
    }
}
