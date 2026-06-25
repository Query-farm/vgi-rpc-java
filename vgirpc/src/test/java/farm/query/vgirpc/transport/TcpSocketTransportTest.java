// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.transport;

import farm.query.vgirpc.RpcConnection;
import farm.query.vgirpc.RpcServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link TcpSocketTransport} — the raw-TCP analog of
 * {@link UnixSocketTransport}. Covers a client/server round-trip over loopback
 * (port 0 ⇒ OS auto-select, surfaced via the on-bound callback) and the
 * idle-timeout watchdog.
 */
final class TcpSocketTransportTest {

    /** Minimal service for the round-trip. */
    public interface EchoService {
        String echo(String value);
        long add(long a, long b);
    }

    public static final class EchoImpl implements EchoService {
        @Override public String echo(String value) { return value; }
        @Override public long add(long a, long b) { return a + b; }
    }

    @Test
    @Timeout(20)
    void clientServerRoundTripOverLoopback() throws Exception {
        RpcServer server = new RpcServer(EchoService.class, new EchoImpl());
        AtomicReference<Integer> boundPort = new AtomicReference<>();
        AtomicReference<Exception> failure = new AtomicReference<>();

        // port 0 ⇒ OS picks a free loopback port, reported via on-bound.
        Thread serverThread = new Thread(() -> {
            try {
                TcpSocketTransport.serveForever("127.0.0.1", 0, server, 0L,
                        (host, port) -> {
                            synchronized (boundPort) {
                                boundPort.set(port);
                                boundPort.notifyAll();
                            }
                        });
            } catch (Exception e) {
                failure.set(e);
            }
        }, "vgi-tcp-test-server");
        serverThread.setDaemon(true);
        serverThread.start();

        int port = awaitBoundPort(boundPort);
        try (RpcConnection conn = new RpcConnection(TcpSocketTransport.connect("127.0.0.1", port))) {
            EchoService proxy = conn.proxy(EchoService.class);
            assertEquals("hello", proxy.echo("hello"));
            assertEquals(7L, proxy.add(3L, 4L));
            assertEquals("again", proxy.echo("again"));
        }
        assertNull(failure.get(), "server thread propagated: " + failure.get());
    }

    @Test
    @Timeout(10)
    void selfExitsAfterIdleTimeoutWithNoConnections() throws Exception {
        RpcServer server = new RpcServer(EchoService.class, new EchoImpl());
        AtomicReference<Exception> failure = new AtomicReference<>();
        long startNanos = System.nanoTime();
        Thread serverThread = new Thread(() -> {
            try {
                TcpSocketTransport.serveForever("127.0.0.1", 0, server, 200L, null);
            } catch (Exception e) {
                failure.set(e);
            }
        }, "vgi-tcp-test-idle");
        serverThread.setDaemon(true);
        serverThread.start();
        serverThread.join(5_000L);
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

        assertFalse(serverThread.isAlive(),
                "server should self-exit after idle timeout, but ran > 5s");
        assertNull(failure.get(), "server thread propagated: " + failure.get());
        assertTrue(elapsedMs >= 200L,
                "should not exit before timeout (elapsed=" + elapsedMs + "ms)");
        assertTrue(elapsedMs < 4_000L,
                "should exit well before test timeout (elapsed=" + elapsedMs + "ms)");
    }

    @Test
    @Timeout(10)
    void activeConnectionDefersIdleExit() throws Exception {
        RpcServer server = new RpcServer(EchoService.class, new EchoImpl());
        AtomicReference<Integer> boundPort = new AtomicReference<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        Thread serverThread = new Thread(() -> {
            try {
                TcpSocketTransport.serveForever("127.0.0.1", 0, server, 200L,
                        (host, port) -> {
                            synchronized (boundPort) {
                                boundPort.set(port);
                                boundPort.notifyAll();
                            }
                        });
            } catch (Exception e) {
                failure.set(e);
            }
        }, "vgi-tcp-test-active");
        serverThread.setDaemon(true);
        serverThread.start();
        int port = awaitBoundPort(boundPort);

        long openHoldMs = 600L;
        try (RpcConnection conn = new RpcConnection(TcpSocketTransport.connect("127.0.0.1", port))) {
            EchoService proxy = conn.proxy(EchoService.class);
            // Keep issuing calls past the idle boundary; active>0 must defer exit.
            AtomicInteger calls = new AtomicInteger();
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(openHoldMs);
            while (System.nanoTime() < deadline) {
                assertEquals("ping", proxy.echo("ping"));
                calls.incrementAndGet();
            }
            assertTrue(serverThread.isAlive(),
                    "server exited while a connection was active (" + openHoldMs + "ms held)");
            assertTrue(calls.get() > 0, "expected at least one call");
        }
        serverThread.join(5_000L);
        assertFalse(serverThread.isAlive(),
                "server should exit after connection closes + idle window elapses");
        assertNull(failure.get());
    }

    private static int awaitBoundPort(AtomicReference<Integer> boundPort) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        synchronized (boundPort) {
            while (boundPort.get() == null) {
                long remainingMs = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
                if (remainingMs <= 0) throw new IllegalStateException("server never bound");
                boundPort.wait(remainingMs);
            }
            return boundPort.get();
        }
    }
}
