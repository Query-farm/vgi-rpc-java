// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.launcher;

import java.io.IOException;
import java.nio.file.Path;
import java.util.OptionalLong;

/**
 * Platform primitives {@code launch:} needs that plain {@code java.nio} can't
 * provide: the process's effective UID (POSIX state-dir naming parity with the
 * Python/C++ launchers — see {@link LauncherPaths}) and a {@code flock(2)}-backed
 * advisory lock. {@code java.nio.channels.FileChannel.lock()} uses {@code
 * fcntl(F_SETLK)} on POSIX, which does <b>not</b> interlock with {@code flock(2)}
 * (see {@code docs/launcher-protocol.md}'s <i>Lock semantics</i> section) — so this
 * is not a convenience wrapper choice, it's the one correct mechanism.
 *
 * <p><b>This is the Java&nbsp;21 baseline: neither primitive is available</b>, so
 * {@code launch:} is unsupported on this runtime. The Java&nbsp;22 multi-release
 * overlay ({@code META-INF/versions/22}) replaces this class with one backed by the
 * Foreign Function &amp; Memory API (GA in JDK&nbsp;22); a JDK&nbsp;&ge;&nbsp;22
 * runtime loads the overlay automatically, matching {@code
 * farm.query.vgirpc.shm.ShmFactory}'s own baseline/overlay split.
 */
public final class PosixLauncherSupport {

    private PosixLauncherSupport() {}

    /** True iff a real {@code flock(2)}/{@code geteuid()} implementation is active on this runtime. */
    public static boolean available() {
        return false;
    }

    /** The current process's effective UID, or empty when unavailable (JDK &lt; 22, or a non-POSIX platform). */
    public static OptionalLong euid() {
        return OptionalLong.empty();
    }

    /**
     * Acquire an exclusive {@code flock(2)} lock on {@code lockFile} (created if absent), blocking up to
     * {@code timeoutSeconds}. Always throws on the Java 21 baseline.
     *
     * @throws UnsupportedOperationException always, on this baseline — {@code launch:} requires JDK 22+
     */
    public static FlockHandle tryLock(Path lockFile, double timeoutSeconds) throws IOException {
        throw new UnsupportedOperationException(
                "launch: transport requires JDK 22+ (flock(2) via the Foreign Function & Memory API); "
                        + "running on " + Runtime.version());
    }
}
