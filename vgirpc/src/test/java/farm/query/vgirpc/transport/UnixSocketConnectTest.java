// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.transport;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link UnixSocketTransport#connectChannel}: a full accept queue is a busy peer, not a refusal.
 *
 * <p>Every case runs against a real listener whose accept queue is genuinely full -- filled by
 * connects it never accepts -- because the property under test is how the kernel's answer
 * surfaces through the JDK, which nothing short of the real thing can reproduce. Linux is the
 * only platform that tells a full queue ({@code EAGAIN}) from no listener ({@code ECONNREFUSED});
 * macOS reports both as the latter, so the waiting cases are Linux-only, as in the vgi extension's
 * own tests.
 */
@Timeout(60)
final class UnixSocketConnectTest {

    /**
     * A listener at a fresh path that never accepts on its own, with its accept queue filled by
     * non-blocking connects it holds open -- a worker too busy to take another connection. Full
     * means the next connect failed: {@code EAGAIN} on Linux, {@code ECONNREFUSED} on macOS.
     */
    static final class FullListener implements AutoCloseable {
        final Path path;
        private final Path dir;
        private final ServerSocketChannel listener;
        private final List<SocketChannel> queued = new ArrayList<>();

        FullListener() throws IOException {
            dir = Files.createTempDirectory("vgi-full-queue-");
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
            Files.deleteIfExists(dir);
        }
    }

    /** A connect that meets a full queue waits for the slot a draining worker frees. */
    @Test
    @EnabledOnOs(OS.LINUX)
    void connectWaitsOutAFullAcceptQueue() throws Exception {
        try (FullListener busy = new FullListener()) {
            CompletableFuture<Void> drain = busy.acceptOneAfter(100);
            try (SocketChannel channel = UnixSocketTransport.connectChannel(busy.path, Duration.ofSeconds(5))) {
                assertTrue(channel.isConnected());
                assertTrue(channel.isBlocking(), "callers get classic blocking semantics back");
            }
            drain.get(5, TimeUnit.SECONDS);
        }
    }

    /**
     * On a virtual thread, where a plain connect fails at once.
     *
     * <p>The JDK puts a virtual thread's socket in non-blocking mode, so a plain
     * {@link SocketChannel#connect} there turns the kernel's {@code EAGAIN} straight into a
     * {@link SocketException} (measured: "Resource temporarily unavailable", JDK 21 and 25). The
     * first half is the control -- it proves the queue is full and that the plain path fails;
     * without it the second half could pass against a queue with room in it.
     */
    @Test
    @EnabledOnOs(OS.LINUX)
    void connectOnAVirtualThreadWaitsOutAFullAcceptQueue() throws Exception {
        try (FullListener busy = new FullListener();
             ExecutorService virtual = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<Throwable> plain = virtual.submit(() -> {
                try (SocketChannel channel = SocketChannel.open(StandardProtocolFamily.UNIX)) {
                    channel.connect(UnixDomainSocketAddress.of(busy.path));
                    return null;
                } catch (IOException e) {
                    return e;
                }
            });
            Throwable refused = plain.get(5, TimeUnit.SECONDS);
            assertInstanceOf(SocketException.class, refused, "control: a plain connect should fail on a full queue");
            assertTrue(UnixSocketTransport.isAcceptQueueFull((IOException) refused, busy.path),
                    "a full queue must be recognised as one: " + refused);

            CompletableFuture<Void> drain = busy.acceptOneAfter(100);
            Future<Boolean> helped = virtual.submit(() -> {
                try (SocketChannel channel =
                             UnixSocketTransport.connectChannel(busy.path, Duration.ofSeconds(5))) {
                    return channel.isConnected();
                }
            });
            assertTrue(helped.get(10, TimeUnit.SECONDS));
            drain.get(5, TimeUnit.SECONDS);
        }
    }

    /** A queue that never drains is a timeout that says why, not a hang and not a refusal. */
    @Test
    @EnabledOnOs(OS.LINUX)
    void connectGivesUpOnAnAcceptQueueThatStaysFull() throws Exception {
        try (FullListener busy = new FullListener()) {
            long start = System.nanoTime();
            SocketTimeoutException e = assertThrows(SocketTimeoutException.class,
                    () -> UnixSocketTransport.connectChannel(busy.path, Duration.ofMillis(200)));
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            assertTrue(e.getMessage().contains("accept queue stayed full"), e.getMessage());
            assertTrue(elapsedMs >= 200, "gave up after " + elapsedMs + "ms, before the timeout");
            assertInstanceOf(SocketException.class, e.getCause(), "the last refusal rides along as the cause");
        }
    }

    /**
     * Nothing listening is a refusal, and it is not waited out.
     *
     * <p>The file a dead listener leaves behind is still a socket inode, so this also pins that
     * the busy test keys on the exception type and not on the path alone.
     */
    @Test
    @DisabledOnOs(OS.WINDOWS)
    void aSocketWithNoListenerIsRefusedAtOnce() throws Exception {
        Path dir = Files.createTempDirectory("vgi-stale-");
        Path path = dir.resolve("stale.sock");
        try {
            ServerSocketChannel dead = ServerSocketChannel.open(StandardProtocolFamily.UNIX);
            dead.bind(UnixDomainSocketAddress.of(path));
            dead.close(); // the JDK leaves the socket file behind, as a crashed worker does
            assertTrue(Files.exists(path));

            long start = System.nanoTime();
            ConnectException e = assertThrows(ConnectException.class,
                    () -> UnixSocketTransport.connectChannel(path, Duration.ofSeconds(10)));
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            assertTrue(elapsedMs < 5_000, "a refusal was retried for " + elapsedMs + "ms");
            assertFalse(UnixSocketTransport.isAcceptQueueFull(e, path));
        } finally {
            Files.deleteIfExists(path);
            Files.deleteIfExists(dir);
        }
    }

    /** An absent path fails at once too, and is not mistaken for a busy peer. */
    @Test
    @DisabledOnOs(OS.WINDOWS)
    void anAbsentPathIsNotBusy() throws Exception {
        Path dir = Files.createTempDirectory("vgi-absent-");
        Path path = dir.resolve("absent.sock");
        try {
            IOException e = assertThrows(IOException.class,
                    () -> UnixSocketTransport.connectChannel(path, Duration.ofSeconds(10)));
            assertFalse(e instanceof SocketTimeoutException, "an absent path was waited on: " + e);
            assertFalse(UnixSocketTransport.isAcceptQueueFull(e, path));
        } finally {
            Files.deleteIfExists(dir);
        }
    }
}
