// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.launcher;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.io.IOException;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A busy worker is not a dead one.
 *
 * <p>Under a burst of connections a launched worker's accept queue fills, and Linux then fails
 * the launcher's non-blocking probe connect with {@code EAGAIN}. Reading that as "dead" unlinked
 * the live worker's socket and spawned a duplicate on every busy probe, orphaning the original and
 * racing clients onto a vanished path -- a 32-process Python test run produced 64 launcher workers
 * for 2 commands. Mirrors {@code tests/test_launcher.py} in the Python reference and
 * {@code test_launcher_e2e.cpp} in the vgi extension.
 *
 * <p>Each case runs against a real listener whose queue is genuinely full, filled by connects it
 * never accepts. The worker argv is one that fails loudly if spawned ("worker exited before
 * readiness"), so an erroneous respawn cannot pass quietly.
 */
@DisabledOnOs(OS.WINDOWS)
@Timeout(60)
final class LauncherBusyWorkerTest {

    /**
     * A listener at a fresh path that never accepts on its own, its accept queue filled by
     * non-blocking connects it holds open. Full means the next connect failed: {@code EAGAIN} on
     * Linux, {@code ECONNREFUSED} on macOS.
     */
    private static final class FullListener implements AutoCloseable {
        final Path path;
        private final Path dir;
        private final ServerSocketChannel listener;
        private final List<SocketChannel> queued = new ArrayList<>();

        FullListener() throws IOException {
            dir = Files.createTempDirectory("vgi-launcher-busy-");
            path = dir.resolve("busy.sock");
            UnixDomainSocketAddress address = UnixDomainSocketAddress.of(path);
            listener = ServerSocketChannel.open(StandardProtocolFamily.UNIX);
            // Not 0: to the JDK a backlog of 0 means "the implementation default", not "none".
            listener.bind(address, 1);
            for (int i = 0; i < 256; i++) {
                SocketChannel client = SocketChannel.open(StandardProtocolFamily.UNIX);
                client.configureBlocking(false);
                try {
                    client.connect(address);
                } catch (IOException full) {
                    client.close();
                    return;
                }
                queued.add(client);
            }
            close();
            throw new AssertionError("the accept queue never filled");
        }

        /** Accept (and drop) one queued connection after {@code delayMs}, freeing a slot. */
        CompletableFuture<Void> acceptOneAfter(long delayMs) {
            return CompletableFuture.runAsync(() -> {
                try {
                    Thread.sleep(delayMs);
                    listener.accept().close();
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            });
        }

        @Override
        public void close() throws IOException {
            for (SocketChannel client : queued) client.close();
            listener.close();
            Files.deleteIfExists(path);
            Files.deleteIfExists(Path.of(path + ".lock"));
            Files.deleteIfExists(dir);
        }
    }

    /** A config for {@code socket} whose worker would fail loudly if the launcher spawned it. */
    private static LaunchConfig mustNotSpawn(Path socket) {
        return new LaunchConfig(List.of("/bin/sh", "-c", "exit 3"), socket.toString(),
                LaunchConfig.DEFAULT_IDLE_TIMEOUT_SECONDS, 5.0, 5.0, socket.getParent(), null);
    }

    private static Object inode(Path path) throws IOException {
        return Files.getAttribute(path, "unix:ino", LinkOption.NOFOLLOW_LINKS);
    }

    /**
     * The regression itself: a probe meeting {@code EAGAIN} leaves the socket alone.
     *
     * <p>Linux-only because only Linux tells a full queue from no listener: macOS would report
     * this queue as a refusal forever, which is indistinguishable from a dead worker.
     */
    @Test
    @EnabledOnOs(OS.LINUX)
    void launchLeavesAWorkerWithAFullAcceptQueueAlone() throws Exception {
        try (FullListener busy = new FullListener()) {
            Object before = inode(busy.path);

            // A respawn would throw here ("worker exited before readiness"), not return.
            assertEquals(busy.path.toString(), LauncherClient.launch(mustNotSpawn(busy.path)));

            assertEquals(before, inode(busy.path), "the live worker's socket was replaced");
        }
    }

    /**
     * The same from a virtual thread -- the shape in which Java turned a busy worker into a dead
     * one outright.
     *
     * <p>A blocking {@code SocketChannel.connect} on a platform thread waits for a queue slot (the
     * old probe hung there, holding the launcher lock, on a worker that never drained); on a
     * virtual thread the JDK makes the socket non-blocking and the connect fails at once with the
     * kernel's {@code EAGAIN}, which the old probe read as "nothing listening": it unlinked the
     * live worker's socket and spawned a replacement.
     */
    @Test
    @EnabledOnOs(OS.LINUX)
    void launchFromAVirtualThreadLeavesAWorkerWithAFullAcceptQueueAlone() throws Exception {
        try (FullListener busy = new FullListener();
             java.util.concurrent.ExecutorService virtual =
                     java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            Object before = inode(busy.path);

            java.util.concurrent.Future<String> launched =
                    virtual.submit(() -> LauncherClient.launch(mustNotSpawn(busy.path)));
            assertEquals(busy.path.toString(), launched.get(30, TimeUnit.SECONDS));

            assertEquals(before, inode(busy.path), "the live worker's socket was replaced");
        }
    }

    /**
     * A full queue that drains is alive however the platform reports it.
     *
     * <p>{@code EAGAIN} on Linux answers at once; macOS's {@code ECONNREFUSED} is re-probed
     * after 50 ms, by which time the worker has taken a connection.
     */
    @Test
    void probeCountsAMomentarilyFullAcceptQueueAsAlive() throws Exception {
        try (FullListener busy = new FullListener()) {
            CompletableFuture<Void> drain = busy.acceptOneAfter(30);
            assertTrue(LauncherClient.probe(busy.path));
            drain.get(5, TimeUnit.SECONDS);
        }
    }

    /** The same, end to end through {@link LauncherClient#launch}. */
    @Test
    void launchCountsAMomentarilyFullAcceptQueueAsAlive() throws Exception {
        try (FullListener busy = new FullListener()) {
            Object before = inode(busy.path);
            CompletableFuture<Void> drain = busy.acceptOneAfter(30);

            assertEquals(busy.path.toString(), LauncherClient.launch(mustNotSpawn(busy.path)));
            drain.get(5, TimeUnit.SECONDS);
            assertEquals(before, inode(busy.path), "the live worker's socket was replaced");
        }
    }

    /**
     * A socket nobody listens on is still dead -- after the re-probes, not instead of them.
     *
     * <p>The other half of the refusal rule: counting every refusal as busy would pin a crashed
     * worker's socket forever and never respawn it.
     */
    @Test
    void probeOfASocketWithNoListenerIsFalseAfterItsRetries() throws Exception {
        Path dir = Files.createTempDirectory("vgi-launcher-stale-");
        Path path = dir.resolve("stale.sock");
        try {
            ServerSocketChannel dead = ServerSocketChannel.open(StandardProtocolFamily.UNIX);
            dead.bind(UnixDomainSocketAddress.of(path));
            dead.close(); // the JDK leaves the socket file behind, as a crashed worker does

            long start = System.nanoTime();
            assertFalse(LauncherClient.probe(path));
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            assertTrue(elapsedMs >= 350, "a refusal was believed after only " + elapsedMs + "ms");
        } finally {
            Files.deleteIfExists(path);
            Files.deleteIfExists(dir);
        }
    }

    /** An absent path is dead at once: there is nothing to wait out. */
    @Test
    void probeOfAnAbsentPathIsFalse() throws Exception {
        Path dir = Files.createTempDirectory("vgi-launcher-absent-");
        try {
            assertFalse(LauncherClient.probe(dir.resolve("absent.sock")));
        } finally {
            Files.deleteIfExists(dir);
        }
    }
}
