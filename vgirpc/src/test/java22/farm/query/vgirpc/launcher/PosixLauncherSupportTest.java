// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.launcher;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Exercises the real {@code flock(2)}/{@code geteuid()} bindings — not a fake/mocked lock. */
@DisabledOnOs(OS.WINDOWS)
final class PosixLauncherSupportTest {

    @Test
    void availableIsTrueOnThisOverlay() {
        assertTrue(PosixLauncherSupport.available());
    }

    @Test
    void euidMatchesTheRealProcessIdentity() throws Exception {
        long euid = PosixLauncherSupport.euid().orElseThrow(
                () -> new AssertionError("expected a real euid on the FFM overlay"));
        // Cross-check against the OS's own answer via `id -u`, not just "is present".
        Process p = new ProcessBuilder("id", "-u").start();
        String output;
        try (var in = p.getInputStream()) {
            output = new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8).strip();
        }
        assertEquals(0, p.waitFor());
        assertEquals(Long.parseLong(output), euid);
    }

    @Test
    @Timeout(30)
    void flockGenuinelyInterlocksAcrossTwoIndependentFileDescriptors() throws Exception {
        // Two independent opens of the SAME lock file, each taking its own flock(2) — this proves
        // real kernel-level mutual exclusion (flock is scoped to the open file description, not the
        // process), the exact property docs/launcher-protocol.md requires and that
        // java.nio.channels.FileChannel.lock() (fcntl-based) would NOT provide.
        Path lockFile = Files.createTempFile("vgi-launcher-flock-test-", ".lock");
        try {
            AtomicBoolean secondHolderEnteredCriticalSection = new AtomicBoolean(false);
            CountDownLatch firstHasLock = new CountDownLatch(1);
            CountDownLatch releaseFirst = new CountDownLatch(1);
            ExecutorService pool = Executors.newFixedThreadPool(2);
            try {
                Future<?> first = pool.submit(() -> {
                    try (FlockHandle lock = PosixLauncherSupport.tryLock(lockFile, 10)) {
                        firstHasLock.countDown();
                        releaseFirst.await();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
                assertTrue(firstHasLock.await(5, TimeUnit.SECONDS));

                Future<?> second = pool.submit(() -> {
                    try (FlockHandle lock = PosixLauncherSupport.tryLock(lockFile, 10)) {
                        // Only reachable after the first holder releases.
                        secondHolderEnteredCriticalSection.set(true);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

                // Give the second acquirer a real chance to (wrongly) succeed while the first
                // still holds the lock — if flock() weren't truly exclusive this would flip true.
                Thread.sleep(300);
                assertTrue(!secondHolderEnteredCriticalSection.get(),
                        "second acquirer entered its critical section while the first still held the lock");

                releaseFirst.countDown();
                first.get(5, TimeUnit.SECONDS);
                second.get(5, TimeUnit.SECONDS);
                assertTrue(secondHolderEnteredCriticalSection.get());
            } finally {
                pool.shutdownNow();
            }
        } finally {
            Files.deleteIfExists(lockFile);
        }
    }

    @Test
    @Timeout(15)
    void tryLockTimesOutWhenAnotherHolderNeverReleases() throws Exception {
        Path lockFile = Files.createTempFile("vgi-launcher-flock-timeout-test-", ".lock");
        try {
            CountDownLatch holderReady = new CountDownLatch(1);
            ExecutorService pool = Executors.newSingleThreadExecutor();
            try {
                pool.submit(() -> {
                    try (FlockHandle lock = PosixLauncherSupport.tryLock(lockFile, 10)) {
                        holderReady.countDown();
                        Thread.sleep(10_000);
                    } catch (Exception ignored) {
                        // Test teardown interrupts this — expected.
                    }
                });
                assertTrue(holderReady.await(5, TimeUnit.SECONDS));

                long start = System.nanoTime();
                IOException e = assertThrows(IOException.class,
                        () -> PosixLauncherSupport.tryLock(lockFile, 0.5));
                double elapsedSeconds = (System.nanoTime() - start) / 1_000_000_000.0;
                assertTrue(e.getMessage().contains("timed out"), e.getMessage());
                assertTrue(elapsedSeconds < 5, "took " + elapsedSeconds + "s to time out on a 0.5s budget");
            } finally {
                pool.shutdownNow();
            }
        } finally {
            Files.deleteIfExists(lockFile);
        }
    }
}
