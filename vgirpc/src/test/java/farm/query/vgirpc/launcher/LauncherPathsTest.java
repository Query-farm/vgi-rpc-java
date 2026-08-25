// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.launcher;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisabledOnOs(OS.WINDOWS) // the launcher protocol itself marks Windows unsupported (see package-info)
final class LauncherPathsTest {

    @Test
    void defaultStateDirIsCreatedMode0700() throws Exception {
        Path dir = LauncherPaths.defaultStateDir();
        assertTrue(Files.isDirectory(dir));
        assertEquals("rwx------", LauncherPaths.permissionsString(dir));
    }

    @Test
    void defaultStateDirHonorsXdgRuntimeDirWhenSet() throws Exception {
        // Can't set XDG_RUNTIME_DIR for THIS process's env from a test without ProcessBuilder
        // gymnastics (System.getenv is immutable) — this test instead pins down the resolution
        // behavior actually exercised in this run's environment, whichever branch that is, so a
        // future edit that silently breaks the "XDG present -> no euid suffix" branch is at least
        // exercised on any CI box that happens to set XDG_RUNTIME_DIR (most Linux CI runners do).
        Path dir = LauncherPaths.defaultStateDir();
        String xdg = System.getenv("XDG_RUNTIME_DIR");
        if (xdg != null && !xdg.isBlank()) {
            assertEquals(Path.of(xdg, "vgi-rpc"), dir);
        } else {
            assertTrue(dir.getFileName().toString().startsWith("vgi-rpc-"));
        }
    }

    @Test
    void perHashPathsAreDeterministicFromTheHash() {
        Path stateDir = Path.of("/tmp/vgi-rpc-test-state-dir");
        String hash = "0123456789abcdef";
        assertEquals(stateDir.resolve("0123456789abcdef.lock"), LauncherPaths.lockPath(stateDir, hash));
        assertEquals(stateDir.resolve("0123456789abcdef.sock"), LauncherPaths.sockPath(stateDir, hash));
        assertEquals(stateDir.resolve("0123456789abcdef.meta"), LauncherPaths.metaPath(stateDir, hash));
    }
}
