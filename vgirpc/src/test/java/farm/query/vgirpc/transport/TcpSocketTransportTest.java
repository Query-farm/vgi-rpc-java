// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.transport;

import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.RpcConnection;
import farm.query.vgirpc.RpcServer;
import farm.query.vgirpc.identity.IdentityAssurance;
import farm.query.vgirpc.identity.PeerAuthenticationPolicies;
import farm.query.vgirpc.identity.PeerIdentity;
import farm.query.vgirpc.identity.PeerIdentityResult;
import farm.query.vgirpc.identity.PeerSubjectKind;
import farm.query.vgirpc.identity.SubjectStability;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

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
        String identity(CallContext context, String value);
        String irohIdentity(CallContext context);
    }

    public static final class EchoImpl implements EchoService {
        @Override public String echo(String value) { return value; }
        @Override public long add(long a, long b) { return a + b; }
        @Override public String identity(CallContext context, String value) {
            return value + ":" + context.auth().domain() + ":"
                    + context.peerEvidence().status("test-peer").wireValue();
        }
        @Override public String irohIdentity(CallContext context) {
            PeerIdentity identity = context.peerEvidence().uniqueVerifiedSubject("iroh");
            return context.auth().domain() + ":" + identity.issuer() + ":"
                    + identity.subjectKey() + ":" + identity.assurance().wireValue() + ":"
                    + identity.attributes().get("original_assurance") + ":"
                    + identity.proxyAddress();
        }
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
    @Timeout(20)
    void snapshotsPeerIdentityOncePerConnection() throws Exception {
        AtomicInteger resolutions = new AtomicInteger();
        var provider = new farm.query.vgirpc.identity.PeerIdentityProvider() {
            @Override public String provider() { return "test-peer"; }
            @Override public PeerIdentityResult resolve(
                    farm.query.vgirpc.identity.PeerResolutionContext context) {
                resolutions.incrementAndGet();
                assertEquals("tcp", context.transport());
                assertTrue(context.sourceEndpoint().contains(":"));
                return PeerIdentityResult.available(new PeerIdentity(
                        provider(), "test_socket", IdentityAssurance.CRYPTOGRAPHIC_PEER,
                        "test-issuer", "tcp", PeerSubjectKind.WORKLOAD, "worker-1",
                        SubjectStability.STABLE, true, Map.of(), Map.of(), false,
                        context.sourceEndpoint(), null));
            }
        };
        TcpServerOptions options = TcpServerOptions.builder()
                .peerIdentityProviders(List.of(provider))
                .peerAuthenticationPolicy(PeerAuthenticationPolicies.primary("test-peer"))
                .identityResolutionTimeout(Duration.ofSeconds(2))
                .build();
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
                        }, options);
            } catch (Exception e) {
                failure.set(e);
            }
        }, "vgi-tcp-test-identity");
        serverThread.setDaemon(true);
        serverThread.start();

        int port = awaitBoundPort(boundPort);
        try (RpcConnection conn = new RpcConnection(TcpSocketTransport.connect("127.0.0.1", port))) {
            EchoService proxy = conn.proxy(EchoService.class);
            assertEquals("first:test-peer:available", proxy.identity(null, "first"));
            assertEquals("second:test-peer:available", proxy.identity(null, "second"));
            assertEquals(1, resolutions.get());
        }
        serverThread.join(5_000L);
        assertNull(failure.get(), "server thread propagated: " + failure.get());
    }

    @Test
    @Timeout(20)
    void preservesCompletedInvalidEvidenceWhenSiblingTimesOut() throws Exception {
        AtomicReference<List<farm.query.vgirpc.identity.PeerIdentityStatus>> observed = new AtomicReference<>();
        farm.query.vgirpc.identity.PeerIdentityProvider hung = new farm.query.vgirpc.identity.PeerIdentityProvider() {
            @Override public String provider() { return "hung"; }
            @Override public PeerIdentityResult resolve(
                    farm.query.vgirpc.identity.PeerResolutionContext context) {
                try {
                    Thread.sleep(10_000L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new farm.query.vgirpc.identity.PeerIdentityUnavailableException("interrupted");
                }
                throw new AssertionError("hung provider unexpectedly completed");
            }
        };
        farm.query.vgirpc.identity.PeerIdentityProvider invalid = new farm.query.vgirpc.identity.PeerIdentityProvider() {
            @Override public String provider() { return "invalid"; }
            @Override public PeerIdentityResult resolve(
                    farm.query.vgirpc.identity.PeerResolutionContext context) {
                return new PeerIdentityResult(provider(), farm.query.vgirpc.identity.PeerIdentityStatus.INVALID);
            }
        };
        TcpServerOptions options = TcpServerOptions.builder()
                .peerIdentityProviders(List.of(hung, invalid))
                .peerAuthenticationPolicy((evidence, auth) -> {
                    observed.set(List.of(evidence.status("hung"), evidence.status("invalid")));
                    return auth;
                })
                .identityResolutionTimeout(Duration.ofMillis(40))
                .build();
        RpcServer server = new RpcServer(EchoService.class, new EchoImpl());
        AtomicReference<Integer> boundPort = new AtomicReference<>();
        Thread serverThread = new Thread(() -> {
            try {
                TcpSocketTransport.serveForever("127.0.0.1", 0, server, 200L,
                        (host, port) -> {
                            synchronized (boundPort) {
                                boundPort.set(port);
                                boundPort.notifyAll();
                            }
                        }, options);
            } catch (Exception ignored) {
                // The idle watchdog closes the listener.
            }
        });
        serverThread.setDaemon(true);
        serverThread.start();
        try (RpcConnection conn = new RpcConnection(
                TcpSocketTransport.connect("127.0.0.1", awaitBoundPort(boundPort)))) {
            assertEquals("ok", conn.proxy(EchoService.class).echo("ok"));
        }
        assertEquals(List.of(
                farm.query.vgirpc.identity.PeerIdentityStatus.UNAVAILABLE,
                farm.query.vgirpc.identity.PeerIdentityStatus.INVALID), observed.get());
    }

    @Test
    @Timeout(20)
    void timedOutProviderRetainsCapacityUntilItsCallableActuallyExits() throws Exception {
        AtomicInteger resolutions = new AtomicInteger();
        AtomicBoolean releaseProvider = new AtomicBoolean();
        List<farm.query.vgirpc.identity.PeerIdentityStatus> observed =
                new CopyOnWriteArrayList<>();
        farm.query.vgirpc.identity.PeerIdentityProvider ignoresInterrupts =
                new farm.query.vgirpc.identity.PeerIdentityProvider() {
                    @Override public String provider() { return "ignores-interrupts"; }

                    @Override public PeerIdentityResult resolve(
                            farm.query.vgirpc.identity.PeerResolutionContext context) {
                        resolutions.incrementAndGet();
                        while (!releaseProvider.get()) {
                            try {
                                Thread.sleep(10L);
                            } catch (InterruptedException ignored) {
                                // Deliberately model an uncooperative provider. Its concurrency
                                // permit must remain held until this callable really returns.
                            }
                        }
                        return new PeerIdentityResult(provider(),
                                farm.query.vgirpc.identity.PeerIdentityStatus.UNAVAILABLE);
                    }
                };
        TcpServerOptions options = TcpServerOptions.builder()
                .peerIdentityProviders(List.of(ignoresInterrupts))
                .peerAuthenticationPolicy((evidence, auth) -> {
                    observed.add(evidence.status("ignores-interrupts"));
                    return auth;
                })
                .peerProviderConcurrency(1)
                .identityResolutionTimeout(Duration.ofMillis(40))
                .build();
        AtomicReference<Integer> boundPort = new AtomicReference<>();
        Thread serverThread = startServer(options, boundPort);
        int port = awaitBoundPort(boundPort);

        try {
            try (RpcConnection connection = new RpcConnection(
                    TcpSocketTransport.connect("127.0.0.1", port))) {
                assertEquals("first", connection.proxy(EchoService.class).echo("first"));
            }
            try (RpcConnection connection = new RpcConnection(
                    TcpSocketTransport.connect("127.0.0.1", port))) {
                assertEquals("second", connection.proxy(EchoService.class).echo("second"));
            }

            assertEquals(1, resolutions.get(),
                    "the second connection must not start another live provider call");
            assertEquals(List.of(
                    farm.query.vgirpc.identity.PeerIdentityStatus.UNAVAILABLE,
                    farm.query.vgirpc.identity.PeerIdentityStatus.UNAVAILABLE), observed);
        } finally {
            releaseProvider.set(true);
        }
        serverThread.join(5_000L);
    }

    @Test
    @Timeout(20)
    void proxyV2UsesAssertedSourceAndPreservesFirstVgiBytes() throws Exception {
        AtomicInteger resolutions = new AtomicInteger();
        AtomicReference<farm.query.vgirpc.identity.PeerResolutionContext> observed =
                new AtomicReference<>();
        var provider = new farm.query.vgirpc.identity.PeerIdentityProvider() {
            @Override public String provider() { return "test-peer"; }
            @Override public PeerIdentityResult resolve(
                    farm.query.vgirpc.identity.PeerResolutionContext context) {
                resolutions.incrementAndGet();
                observed.set(context);
                return PeerIdentityResult.available(new PeerIdentity(
                        provider(), "proxy_v2", IdentityAssurance.CONFIGURED_PROXY,
                        "test-issuer", "tcp", PeerSubjectKind.WORKLOAD, "worker-1",
                        SubjectStability.STABLE, true, Map.of(), Map.of(), false,
                        "100.64.0.42", context.immediatePeer()));
            }
        };
        TcpServerOptions options = TcpServerOptions.builder()
                .proxyProtocolV2Required(true)
                .trustedProxyAddresses(Set.of("127.0.0.1"))
                .proxyPreambleTimeout(Duration.ofMillis(500))
                .peerIdentityProviders(List.of(provider))
                .peerAuthenticationPolicy(PeerAuthenticationPolicies.primary("test-peer"))
                .build();
        AtomicReference<Integer> boundPort = new AtomicReference<>();
        Thread serverThread = startServer(options, boundPort);

        Socket socket = new Socket("127.0.0.1", awaitBoundPort(boundPort));
        socket.getOutputStream().write(ProxyProtocolV2Test.ipv4(
                new byte[] {100, 64, 0, 42, 10, 0, 0, 9}, 51_234, 19_400,
                new byte[] {(byte) 0xee, 0, 1, 7}));
        socket.getOutputStream().flush();
        try (RpcConnection connection = new RpcConnection(new TcpSocketTransport(socket))) {
            EchoService service = connection.proxy(EchoService.class);
            assertEquals("first:test-peer:available", service.identity(null, "first"));
            assertEquals("second:test-peer:available", service.identity(null, "second"));
        }

        var context = observed.get();
        assertEquals("127.0.0.1", context.immediatePeer());
        assertTrue(context.sourceEndpoint().startsWith("127.0.0.1:"));
        assertEquals("100.64.0.42:51234", context.assertedPeer());
        assertEquals("10.0.0.9:19400", context.destinationAddress());
        assertEquals("100.64.0.42:51234", context.metadata().get("asserted_peer"));
        assertEquals(Boolean.TRUE, context.metadata().get("proxy_protocol_v2"));
        assertEquals(1, resolutions.get());
        serverThread.join(5_000L);
    }

    @Test
    @Timeout(20)
    void proxyV2PromotesForwardedIrohIdentityThroughExistingPolicy() throws Exception {
        TcpServerOptions options = TcpServerOptions.builder()
                .proxyProtocolV2Required(true)
                .trustedProxyAddresses(Set.of("127.0.0.1"))
                .proxyPreambleTimeout(Duration.ofMillis(500))
                .irohProxyIssuer("production-mesh")
                .peerAuthenticationPolicy(PeerAuthenticationPolicies.primary("iroh"))
                .build();
        AtomicReference<Integer> boundPort = new AtomicReference<>();
        Thread serverThread = startServer(options, boundPort);

        byte[] endpoint = new byte[32];
        for (int index = 0; index < endpoint.length; index++) endpoint[index] = (byte) index;
        Socket socket = new Socket("127.0.0.1", awaitBoundPort(boundPort));
        socket.getOutputStream().write(ProxyProtocolV2Test.iroh(endpoint,
                new byte[] {(byte) 0xee, 0, 1, 7}));
        socket.getOutputStream().flush();
        try (RpcConnection connection = new RpcConnection(new TcpSocketTransport(socket))) {
            String identity = connection.proxy(EchoService.class).irohIdentity(null);
            assertTrue(identity.startsWith("iroh:production-mesh:"
                    + "000102030405060708090a0b0c0d0e0f"
                    + "101112131415161718191a1b1c1d1e1f"
                    + ":configured_proxy:cryptographic_peer:127.0.0.1:"));
        }
        serverThread.join(5_000L);
    }

    @Test
    @Timeout(10)
    void forwardedIrohPrimaryRejectsOrdinaryProxyAddressWithoutIdentity() throws Exception {
        TcpServerOptions options = TcpServerOptions.builder()
                .proxyProtocolV2Required(true)
                .trustedProxyAddresses(Set.of("127.0.0.1"))
                .proxyPreambleTimeout(Duration.ofMillis(500))
                .irohProxyIssuer("production-mesh")
                .peerAuthenticationPolicy(PeerAuthenticationPolicies.primary("iroh"))
                .build();
        AtomicReference<Integer> boundPort = new AtomicReference<>();
        startServer(options, boundPort);

        try (Socket socket = new Socket("127.0.0.1", awaitBoundPort(boundPort))) {
            socket.getOutputStream().write(ProxyProtocolV2Test.ipv4(
                    new byte[] {100, 64, 0, 42, 10, 0, 0, 9}, 51_234, 19_400,
                    new byte[0]));
            socket.getOutputStream().flush();
            socket.setSoTimeout(1_000);
            assertConnectionClosed(socket);
        }
    }

    @Test
    @Timeout(10)
    void proxyV2RejectsUntrustedPeerBeforeReadingPreamble() throws Exception {
        TcpServerOptions options = TcpServerOptions.builder()
                .proxyProtocolV2Required(true)
                .trustedProxyAddresses(Set.of("192.0.2.1"))
                .proxyPreambleTimeout(Duration.ofSeconds(2))
                .build();
        AtomicReference<Integer> boundPort = new AtomicReference<>();
        startServer(options, boundPort);

        try (Socket socket = new Socket("127.0.0.1", awaitBoundPort(boundPort))) {
            socket.setSoTimeout(1_000);
            assertConnectionClosed(socket);
        }
    }

    @Test
    @Timeout(10)
    void proxyV2AbsolutePreambleDeadlineStopsSlowloris() throws Exception {
        TcpServerOptions options = TcpServerOptions.builder()
                .proxyProtocolV2Required(true)
                .trustedProxyAddresses(Set.of("127.0.0.1"))
                .proxyPreambleTimeout(Duration.ofMillis(75))
                .build();
        AtomicReference<Integer> boundPort = new AtomicReference<>();
        startServer(options, boundPort);

        try (Socket socket = new Socket("127.0.0.1", awaitBoundPort(boundPort))) {
            socket.getOutputStream().write(0x0d);
            socket.getOutputStream().flush();
            Thread.sleep(200L);
            socket.setSoTimeout(1_000);
            assertConnectionClosed(socket);
        }
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

    private static Thread startServer(
            TcpServerOptions options, AtomicReference<Integer> boundPort) {
        RpcServer server = new RpcServer(EchoService.class, new EchoImpl());
        Thread serverThread = new Thread(() -> {
            try {
                TcpSocketTransport.serveForever("127.0.0.1", 0, server, 200L,
                        (host, port) -> {
                            synchronized (boundPort) {
                                boundPort.set(port);
                                boundPort.notifyAll();
                            }
                        }, options);
            } catch (IOException ignored) {
                // The idle watchdog closes the listener.
            }
        });
        serverThread.setDaemon(true);
        serverThread.start();
        return serverThread;
    }

    private static void assertConnectionClosed(Socket socket) throws IOException {
        try {
            assertEquals(-1, socket.getInputStream().read());
        } catch (java.net.SocketException expectedReset) {
            // A reset is also a prompt fail-closed rejection.
        }
    }
}
