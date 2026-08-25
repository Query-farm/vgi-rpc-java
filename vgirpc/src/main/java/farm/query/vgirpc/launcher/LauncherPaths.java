// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.launcher;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.EnumSet;
import java.util.OptionalLong;
import java.util.Set;

/**
 * Resolves the per-user launcher state directory and the three per-hash file paths
 * within it, per {@code docs/launcher-protocol.md}'s <i>State directory</i> and
 * <i>Per-tuple files</i> sections.
 */
public final class LauncherPaths {

    private LauncherPaths() {}

    /**
     * Resolve the per-user state directory, creating it (mode {@code 0700}) if
     * missing. Linux with {@code $XDG_RUNTIME_DIR} set uses {@code
     * $XDG_RUNTIME_DIR/vgi-rpc/}; everywhere else (Linux without it, macOS) uses
     * {@code $TMPDIR/vgi-rpc-<euid>/} — {@code euid} from {@link
     * PosixLauncherSupport#euid()} when available, or {@code "unknown"} as a
     * baseline-JDK21 fallback (irrelevant in practice: {@code launch:} is entirely
     * unsupported on JDK 21, so this name is never actually shared with another
     * process).
     *
     * @throws IOException if the directory can't be created, or exists but is owned
     *         by a different user (best-effort check: Java has no portable numeric
     *         UID comparison without FFM, so this compares {@link
     *         Files#getOwner}'s principal name against {@code user.name} instead of
     *         the true {@code st_uid} the Python/C++ launchers check)
     */
    public static Path defaultStateDir() throws IOException {
        String xdgRuntimeDir = System.getenv("XDG_RUNTIME_DIR");
        Path base;
        if (xdgRuntimeDir != null && !xdgRuntimeDir.isBlank()) {
            base = Path.of(xdgRuntimeDir, "vgi-rpc");
        } else {
            String tmpDir = System.getProperty("java.io.tmpdir", "/tmp");
            OptionalLong euid = PosixLauncherSupport.euid();
            String suffix = euid.isPresent() ? Long.toString(euid.getAsLong()) : "unknown";
            base = Path.of(tmpDir, "vgi-rpc-" + suffix);
        }
        Files.createDirectories(base);
        trySetPosixPermissions(base);
        try {
            String owner = Files.getOwner(base).getName();
            String currentUser = System.getProperty("user.name");
            if (currentUser != null && !currentUser.equals(owner)) {
                throw new IOException("state directory " + base + " is not owned by the current user"
                        + " (owner=" + owner + ", current=" + currentUser + ")");
            }
        } catch (java.io.IOException e) {
            // getOwner() unsupported on this filesystem (e.g. a non-POSIX view) — not a security
            // boundary against another process running as the same user anyway, so degrade quietly.
        }
        return base;
    }

    private static void trySetPosixPermissions(Path dir) {
        try {
            Set<PosixFilePermission> mode0700 = EnumSet.of(
                    PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE,
                    PosixFilePermission.OWNER_EXECUTE);
            Files.setPosixFilePermissions(dir, mode0700);
        } catch (UnsupportedOperationException | IOException e) {
            // Non-POSIX filesystem (e.g. exotic overlay) — best-effort only, matching the Python
            // reference's own `contextlib.suppress(OSError)` around this same chmod.
        }
    }

    /** The {@code <hash>.lock} file path within {@code stateDir}. */
    public static Path lockPath(Path stateDir, String hashId) {
        return stateDir.resolve(hashId + ".lock");
    }

    /** The {@code <hash>.sock} file path within {@code stateDir}. */
    public static Path sockPath(Path stateDir, String hashId) {
        return stateDir.resolve(hashId + ".sock");
    }

    /** The {@code <hash>.meta} file path within {@code stateDir}. */
    public static Path metaPath(Path stateDir, String hashId) {
        return stateDir.resolve(hashId + ".meta");
    }

    /** Human-readable POSIX permission string, exposed only for tests to assert the {@code 0700} mode. */
    static String permissionsString(Path path) throws IOException {
        return PosixFilePermissions.toString(Files.getPosixFilePermissions(path));
    }
}
