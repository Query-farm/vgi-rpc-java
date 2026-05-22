// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.transport;

import farm.query.vgirpc.RpcServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Regression test for the idle-timeout watchdog in
 * {@link UnixSocketTransport#serveForever(Path, RpcServer, long)}. Before the
 * watchdog was wired up, {@code --idle-timeout} was parsed but ignored — the
 * accept loop ran forever, leaving stale worker JVMs alive across test runs
 * and silently shadowing rebuilt classes.
 */
final class UnixSocketTransportIdleTest {

    /** Marker service — the test never actually dispatches anything. */
    public interface Empty { void noop(); }
    public static final class EmptyImpl implements Empty { @Override public void noop() {} }

    @Test
    @Timeout(10)
    void selfExitsAfterIdleTimeoutWithNoConnections() throws Exception {
        Path socket = uniqueSocketPath();
        AtomicReference<Exception> failure = new AtomicReference<>();
        long startNanos = System.nanoTime();
        Thread serverThread = startServer(socket, 200L, failure);
        serverThread.join(5_000L);
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

        assertFalse(serverThread.isAlive(),
                "server should self-exit after idle timeout, but ran > 5s");
        assertNull(failure.get(), "server thread propagated: " + failure.get());
        assertTrue(elapsedMs >= 200L,
                "should not exit before timeout (elapsed=" + elapsedMs + "ms)");
        // Generous upper bound — the 100ms poll floor + JVM scheduling jitter.
        assertTrue(elapsedMs < 4_000L,
                "should exit well before test timeout (elapsed=" + elapsedMs + "ms)");
    }

    @Test
    @Timeout(10)
    void activeConnectionDeferesIdleExit() throws Exception {
        Path socket = uniqueSocketPath();
        AtomicReference<Exception> failure = new AtomicReference<>();
        Thread serverThread = startServer(socket, 200L, failure);
        waitForSocketReady(socket);

        // Hold a connection open past the would-be idle boundary. With the
        // watchdog active but our connection open, active>0 must defer exit.
        long openHoldMs = 600L;
        try (SocketChannel client = SocketChannel.open(StandardProtocolFamily.UNIX)) {
            client.connect(UnixDomainSocketAddress.of(socket));
            Thread.sleep(openHoldMs);
            assertTrue(serverThread.isAlive(),
                    "server exited while a connection was open (" + openHoldMs + "ms held)");
        }
        // After close, watchdog should fire on the next poll tick.
        serverThread.join(5_000L);
        assertFalse(serverThread.isAlive(),
                "server should exit after connection closes + idle window elapses");
        assertNull(failure.get());
    }

    @Test
    @Timeout(5)
    void zeroTimeoutDoesNotEnableWatchdog() throws Exception {
        // Old callers passing 0 (or omitting the param) must still run forever.
        Path socket = uniqueSocketPath();
        AtomicReference<Exception> failure = new AtomicReference<>();
        Thread serverThread = startServer(socket, 0L, failure);
        waitForSocketReady(socket);
        // Give the watchdog poll-floor (100ms) twice over to prove it isn't firing.
        Thread.sleep(300L);
        assertTrue(serverThread.isAlive(),
                "server with idleTimeoutMs=0 must not self-exit");
        // Interrupt to clean up — the listener loop exits when ssc closes.
        serverThread.interrupt();
        // We can't cleanly stop a serveForever(0) from this test surface; the
        // test JVM tearing down on exit closes the channel. Leave the thread.
        assertNull(failure.get());
    }

    private static Thread startServer(Path socket, long idleTimeoutMs,
                                       AtomicReference<Exception> failureSink) {
        RpcServer server = new RpcServer(Empty.class, new EmptyImpl());
        Thread t = new Thread(() -> {
            try {
                UnixSocketTransport.serveForever(socket, server, idleTimeoutMs);
            } catch (Exception e) {
                failureSink.set(e);
            }
        }, "vgi-test-server-" + idleTimeoutMs);
        t.setDaemon(true);
        t.start();
        return t;
    }

    private static Path uniqueSocketPath() throws Exception {
        Path dir = Files.createTempDirectory("vgi-idle-test-");
        return dir.resolve("server.sock");
    }

    /** Spin briefly until {@code serveForever} has bound the socket. */
    private static void waitForSocketReady(Path socket) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (System.nanoTime() < deadline) {
            if (Files.exists(socket)) return;
            Thread.sleep(20L);
        }
        throw new IllegalStateException("server socket never appeared at " + socket);
    }
}
