// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.conformance;

import farm.query.vgirpc.Introspect;
import farm.query.vgirpc.Introspect.HostedProtocol;
import farm.query.vgirpc.Introspect.ServiceDescription;
import farm.query.vgirpc.ReflectionNotSupportedError;
import farm.query.vgirpc.RpcConnection;
import farm.query.vgirpc.RpcError;
import farm.query.vgirpc.RpcServer;
import farm.query.vgirpc.ServiceIntrospector;
import farm.query.vgirpc.http.HttpRpcConnection;
import farm.query.vgirpc.http.HttpServer;
import farm.query.vgirpc.transport.RpcTransport;
import farm.query.vgirpc.transport.SubprocessTransport;
import farm.query.vgirpc.transport.TcpSocketTransport;
import farm.query.vgirpc.transport.UnixSocketTransport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.Authenticator;
import java.net.CookieHandler;
import java.net.InetSocketAddress;
import java.net.ProxySelector;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The public reflection client: {@link Introspect#listProtocols(Object)} and
 * {@link Introspect#describeProtocol(Object, String)} over a connection the
 * caller already holds, on every transport this port's client speaks, against
 * this port's own server and the Python reference conformance server.
 *
 * <p>The reference worker (and this port's, mirrored here in-process) hosts
 * the conformance primary, then {@code conformance.Secondary.v1}, then
 * {@code vgi_rpc.Reflection.v1} — that order is the contract.
 */
@Timeout(60)
final class ReflectionClientTest {

    private static final String PRIMARY = ServiceIntrospector.protocolName(ConformanceService.class);
    private static final List<String> EXPECTED =
            List.of(PRIMARY, Secondary.PROTOCOL_NAME, Introspect.REFLECTION_PROTOCOL);

    private final List<AutoCloseable> cleanup = new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (int i = cleanup.size() - 1; i >= 0; i--) {
            try { cleanup.get(i).close(); } catch (Exception ignore) { }
        }
    }

    /** How a test reaches a server; each yields a held connection and its typed proxy. */
    enum Transport { PIPE, SUBPROCESS, UNIX, TCP, HTTP }

    /** What the test holds: the connection object, a typed proxy on it, and close tracking. */
    private record Held(Object connection, ConformanceService proxy, AtomicBoolean closed,
                        AtomicInteger connectionsOpened) {}

    // =====================================================================
    // Against this port's own server
    // =====================================================================

    private static RpcServer javaServer() {
        RpcServer server = new RpcServer(ConformanceService.class, new ConformanceServiceImpl());
        server.setProtocolVersion("2.0.0");
        server.addProtocol(Secondary.class, new SecondaryImpl());
        return server;
    }

    private Held javaPeer(Transport transport) throws Exception {
        RpcServer server = javaServer();
        return switch (transport) {
            case PIPE -> {
                PipedOutputStream clientOut = new PipedOutputStream();
                PipedInputStream serverIn = new PipedInputStream(clientOut, 1 << 16);
                PipedOutputStream serverOut = new PipedOutputStream();
                PipedInputStream clientIn = new PipedInputStream(serverOut, 1 << 16);
                Thread t = new Thread(() -> server.serve(new Pipes(serverIn, serverOut)), "reflection-pipe");
                t.setDaemon(true);
                t.start();
                yield byteStream(new Pipes(clientIn, clientOut), new AtomicInteger(1));
            }
            case SUBPROCESS -> throw new IllegalArgumentException("covered against the reference");
            case UNIX -> {
                Path dir = Files.createTempDirectory("vgirpc-refl");
                Path socket = dir.resolve("w.sock");
                Thread t = new Thread(() -> {
                    try { UnixSocketTransport.serveForever(socket, server); } catch (IOException ignore) { }
                }, "reflection-unix");
                t.setDaemon(true);
                t.start();
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (!Files.exists(socket) && System.nanoTime() < deadline) Thread.sleep(10);
                yield byteStream(UnixSocketTransport.connect(socket, Duration.ofSeconds(5)), new AtomicInteger(1));
            }
            case TCP -> {
                // Our own accept loop, so a test can see every connection the client opens.
                ServerSocket listener = new ServerSocket();
                listener.bind(new InetSocketAddress("127.0.0.1", 0));
                cleanup.add(listener);
                AtomicInteger accepted = new AtomicInteger();
                Thread t = new Thread(() -> {
                    while (!listener.isClosed()) {
                        try {
                            Socket s = listener.accept();
                            accepted.incrementAndGet();
                            Thread.ofVirtual().start(() -> {
                                try { server.serve(new TcpSocketTransport(s)); } catch (IOException ignore) { }
                            });
                        } catch (IOException e) {
                            return;
                        }
                    }
                }, "reflection-tcp");
                t.setDaemon(true);
                t.start();
                yield byteStream(TcpSocketTransport.connect("127.0.0.1", listener.getLocalPort()), accepted);
            }
            case HTTP -> {
                HttpServer http = new HttpServer(server, HttpServer.Config.builder().port(0).build());
                http.start();
                cleanup.add(http::stop);
                yield httpHeld("http://127.0.0.1:" + http.port());
            }
        };
    }

    @ParameterizedTest
    @EnumSource(value = Transport.class, names = {"PIPE", "UNIX", "TCP", "HTTP"})
    void javaServerListsPrimarySecondaryThenReflection(Transport transport) throws Exception {
        assertListingContract(javaPeer(transport));
    }

    @ParameterizedTest
    @EnumSource(value = Transport.class, names = {"PIPE", "UNIX", "TCP", "HTTP"})
    void javaServerDescribesAndRefusesUnknown(Transport transport) throws Exception {
        assertDescribeContract(javaPeer(transport));
    }

    @Test
    void tcpListingOpensNoSecondConnection() throws Exception {
        Held held = javaPeer(Transport.TCP);
        Introspect.listProtocols(held.connection());
        Introspect.describeProtocol(held.proxy(), Secondary.PROTOCOL_NAME);
        assertEquals("x", held.proxy().echo_string("x"));
        assertEquals(1, held.connectionsOpened().get(), "reflection must ride the held socket");
    }

    @Test
    void httpListingGoesThroughTheHeldClient() throws Exception {
        HttpServer http = new HttpServer(javaServer(), HttpServer.Config.builder().port(0).build());
        http.start();
        cleanup.add(http::stop);
        CountingHttpClient client = new CountingHttpClient(HttpClient.newHttpClient());
        cleanup.add(client.delegate::close);
        try (HttpRpcConnection conn = HttpRpcConnection.builder("http://127.0.0.1:" + http.port())
                .httpClient(client).build()) {
            ConformanceService proxy = conn.proxy(ConformanceService.class);
            // Warm the connection first: its first request also discovers capabilities.
            assertEquals("warm", proxy.echo_string("warm"));
            int before = client.sent.get();
            assertTrue(before >= 1, "the connection must be using the supplied client");
            assertEquals(EXPECTED, names(Introspect.listProtocols(proxy)));
            assertEquals(before + 1, client.sent.get(), "list_protocols is one request on the held client");
            Introspect.describeProtocol(conn, PRIMARY);
            assertEquals(before + 3, client.sent.get(), "describe_protocol is list then describe");
        }
    }

    @Test
    void anUnsupportedTargetIsRefused() {
        assertThrows(IllegalArgumentException.class, () -> Introspect.listProtocols("not a connection"));
        assertThrows(IllegalArgumentException.class, () -> Introspect.listProtocols((Object) null));
    }

    /** Every "not hosted" answer a current or older server gives, and one that is not. */
    @Test
    void olderServerAnswersClassifyAsNotSupported() {
        List<RpcError> notHosted = List.of(
                new RpcError("ProtocolNotSupportedError", "no", "", "r1", "protocol_not_supported",
                        "UNIMPLEMENTED", List.of()),
                new RpcError("AttributeError", "Unknown method: list_protocols", "", "", "method_not_implemented"),
                new RpcError("SomethingElse", "x", "", "", null, "UNIMPLEMENTED", List.of()),
                new RpcError("MethodNotImplementedError", "x", ""),
                new RpcError("HttpError", "list_protocols: HTTP 404 with a non-Arrow body", ""),
                new RpcError("HttpError", "HTTP 404 Not Found", ""));
        for (RpcError answer : notHosted) {
            Introspect.RawUnaryCaller peer = (protocol, method, request) -> { throw answer; };
            ReflectionNotSupportedError e = assertThrows(ReflectionNotSupportedError.class,
                    () -> Introspect.listProtocols((Object) peer), answer.toString());
            assertEquals(answer.errorType(), e.errorType());
            assertEquals(answer.errorMessage(), e.errorMessage());
            assertEquals(answer.errorKind(), e.errorKind());
            assertEquals(answer.errorCode(), e.errorCode());
            assertEquals(answer.requestId(), e.requestId());
        }
        for (RpcError other : List.of(
                new RpcError("HttpError", "list_protocols: HTTP 500 with a non-Arrow body", ""),
                new RpcError("RuntimeError", "boom", "", "", null, "INTERNAL", List.of()))) {
            Introspect.RawUnaryCaller peer = (protocol, method, request) -> { throw other; };
            RpcError e = assertThrows(RpcError.class, () -> Introspect.listProtocols((Object) peer));
            assertFalse(e instanceof ReflectionNotSupportedError, e.toString());
            assertTrue(e == other);
        }
    }

    @Test
    void hostedProtocolDefaults() {
        HostedProtocol p = new HostedProtocol("a.B.v1", "1.0.0", "ab");
        assertFalse(p.deprecated());
        assertEquals("", p.deprecationMessage());
        assertEquals(List.of(), p.features());
        assertThrows(UnsupportedOperationException.class,
                () -> new HostedProtocol("a", "", "", false, "", new ArrayList<>(List.of("f"))).features().add("g"));
    }

    // =====================================================================
    // Against the Python reference conformance server
    // =====================================================================

    @ParameterizedTest
    @EnumSource(value = Transport.class, names = {"SUBPROCESS", "UNIX", "TCP", "HTTP"})
    void referenceServerListsPrimarySecondaryThenReflection(Transport transport) throws Exception {
        assertListingContract(referencePeer(transport, true));
    }

    @ParameterizedTest
    @EnumSource(value = Transport.class, names = {"SUBPROCESS", "UNIX", "TCP", "HTTP"})
    void referenceServerDescribesAndRefusesUnknown(Transport transport) throws Exception {
        assertDescribeContract(referencePeer(transport, true));
    }

    @ParameterizedTest
    @EnumSource(value = Transport.class, names = {"SUBPROCESS", "UNIX", "TCP", "HTTP"})
    void referenceServerWithoutReflectionRaisesAndStaysUsable(Transport transport) throws Exception {
        Held held = referencePeer(transport, false);
        for (Object target : List.of(held.connection(), held.proxy())) {
            ReflectionNotSupportedError e = assertThrows(ReflectionNotSupportedError.class,
                    () -> Introspect.listProtocols(target));
            assertEquals("protocol_not_supported", e.errorKind(), e.toString());
            assertEquals("UNIMPLEMENTED", e.errorCode(), e.toString());
            assertFalse(e.errorMessage().isEmpty());
            // A subclass, so existing RpcError handlers still catch it.
            assertTrue(e instanceof RpcError);
            assertThrows(ReflectionNotSupportedError.class,
                    () -> Introspect.describeProtocol(target, PRIMARY));
            assertEquals("still-usable", held.proxy().echo_string("still-usable"));
        }
        assertFalse(held.closed().get());
    }

    private Held referencePeer(Transport transport, boolean describe) throws Exception {
        String python = findPython();
        Assumptions.assumeTrue(python != null,
                "the Python reference (vgi_rpc.conformance) is unavailable; set VGI_RPC_PYTHON");
        List<String> cmd = new ArrayList<>(List.of(python, "-c",
                "from vgi_rpc.conformance._cli import main; main()"));
        if (describe) cmd.add("--describe");
        return switch (transport) {
            case SUBPROCESS -> {
                SubprocessTransport sub = new SubprocessTransport(cmd);
                yield byteStream(sub, new AtomicInteger(1));
            }
            case UNIX -> {
                Path socket = Files.createTempDirectory("vgirpc-ref").resolve("w.sock");
                cmd.addAll(List.of("--unix", socket.toString()));
                startAndAwait(cmd, "UNIX:");
                // The reference announces the path before it binds.
                awaitUntil(() -> Files.exists(socket));
                yield byteStream(UnixSocketTransport.connect(socket, Duration.ofSeconds(5)), new AtomicInteger(1));
            }
            case TCP -> {
                cmd.addAll(List.of("--tcp", "127.0.0.1:0"));
                String line = startAndAwait(cmd, "TCP:");
                int port = Integer.parseInt(line.substring(line.lastIndexOf(':') + 1));
                yield byteStream(TcpSocketTransport.connect("127.0.0.1", port), new AtomicInteger(1));
            }
            case HTTP -> {
                cmd.addAll(List.of("--http", "0"));
                String line = startAndAwait(cmd, "PORT:");
                int port = Integer.parseInt(line.substring(5).trim());
                // The reference announces the port before waitress binds it.
                awaitUntil(() -> {
                    try (Socket probe = new Socket("127.0.0.1", port)) {
                        return true;
                    } catch (IOException e) {
                        return false;
                    }
                });
                yield httpHeld("http://127.0.0.1:" + port);
            }
            case PIPE -> throw new IllegalArgumentException("pipe is in-process only");
        };
    }

    // =====================================================================
    // The contract, shared by every server and transport
    // =====================================================================

    private void assertListingContract(Held held) {
        // Through the connection and through a proxy bound to the primary; the order is the server's.
        List<HostedProtocol> viaConnection = Introspect.listProtocols(held.connection());
        assertEquals(EXPECTED, names(viaConnection));
        assertEquals(viaConnection, Introspect.listProtocols(held.proxy()));
        for (HostedProtocol p : viaConnection) {
            assertTrue(p.hash().matches("[0-9a-f]{64}"), p.toString());
            assertFalse(p.deprecated(), p.toString());
        }
        assertEquals(Secondary.PROTOCOL_HASH, viaConnection.get(1).hash());
        assertEquals("2.0.0", viaConnection.get(0).version());

        // A proxy bound to a protocol other than the primary reaches the same listing.
        Secondary secondary = proxyOf(held, Secondary.class);
        assertEquals(viaConnection, Introspect.listProtocols(secondary));
        assertEquals(Secondary.ECHO_PREFIX + "s", secondary.echo_string("s"));

        // The held connection was reused, not closed: it still serves the bound protocol.
        assertEquals("after", held.proxy().echo_string("after"));
        assertFalse(held.closed().get(), "the held connection must never be closed");
    }

    private void assertDescribeContract(Held held) {
        ServiceDescription primary = Introspect.describeProtocol(held.proxy(), PRIMARY);
        assertEquals(PRIMARY, primary.protocolName());
        assertTrue(primary.methods().containsKey("echo_string"), primary.methods().keySet().toString());
        assertFalse(primary.serverId().isEmpty(), "server identity comes from the listing hop");

        ServiceDescription secondary = Introspect.describeProtocol(held.connection(), Secondary.PROTOCOL_NAME);
        assertEquals(Secondary.PROTOCOL_HASH, secondary.protocolHash());
        assertTrue(secondary.methods().containsKey("echo_string"));

        ServiceDescription reflection = Introspect.describeProtocol(held.proxy(), Introspect.REFLECTION_PROTOCOL);
        assertTrue(reflection.methods().keySet().containsAll(List.of("list_protocols", "describe")));

        // An unknown name is an ordinary RPC error, not "no reflection".
        RpcError unknown = assertThrows(RpcError.class,
                () -> Introspect.describeProtocol(held.proxy(), "no.Such.v1"));
        assertFalse(unknown instanceof ReflectionNotSupportedError, unknown.toString());
        assertEquals("protocol_not_supported", unknown.errorKind(), unknown.toString());

        assertEquals("after", held.proxy().echo_string("after"));
        assertFalse(held.closed().get(), "the held connection must never be closed");
    }

    // =====================================================================
    // Plumbing
    // =====================================================================

    private Held byteStream(RpcTransport raw, AtomicInteger opened) {
        AtomicBoolean closed = new AtomicBoolean();
        RpcTransport tracked = new Tracked(raw, closed);
        RpcConnection conn = new RpcConnection(tracked);
        cleanup.add(raw::close);
        return new Held(conn, conn.proxy(ConformanceService.class), closed, opened);
    }

    private Held httpHeld(String url) {
        // The connection owns its HttpClient, so closing it would break every later call.
        HttpRpcConnection conn = HttpRpcConnection.builder(url).build();
        cleanup.add(conn);
        AtomicBoolean closed = new AtomicBoolean();
        return new Held(conn, conn.proxy(ConformanceService.class), closed, new AtomicInteger(1));
    }

    private static <T> T proxyOf(Held held, Class<T> iface) {
        return held.connection() instanceof RpcConnection c
                ? c.proxy(iface)
                : ((HttpRpcConnection) held.connection()).proxy(iface);
    }

    private static List<String> names(List<HostedProtocol> protocols) {
        return protocols.stream().map(HostedProtocol::name).toList();
    }

    private String startAndAwait(List<String> cmd, String prefix) throws Exception {
        Process p = new ProcessBuilder(cmd).redirectError(ProcessBuilder.Redirect.DISCARD).start();
        cleanup.add(() -> {
            p.destroy();
            if (!p.waitFor(5, TimeUnit.SECONDS)) p.destroyForcibly();
        });
        BufferedReader out = new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8));
        CompletableFuture<String> line = CompletableFuture.supplyAsync(() -> {
            try {
                for (String l; (l = out.readLine()) != null; ) if (l.startsWith(prefix)) return l;
                return null;
            } catch (IOException e) {
                return null;
            }
        });
        String found = line.get(30, TimeUnit.SECONDS);
        assertNotNull(found, "reference server never announced " + prefix);
        return found;
    }

    private static void awaitUntil(java.util.function.BooleanSupplier ready) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (!ready.getAsBoolean()) {
            assertTrue(System.nanoTime() < deadline, "reference server never became ready");
            Thread.sleep(10);
        }
    }

    private static final List<String> REFERENCE_VENVS =
            List.of("vgi-rpc-python/.venv/bin/python", "vgi-rpc/.venv/bin/python");

    private static String findPython() throws IOException, InterruptedException {
        String configured = System.getenv("VGI_RPC_PYTHON");
        if (configured != null && !configured.isBlank()) return configured;
        for (String relative : REFERENCE_VENVS) {
            Path cursor = Path.of("").toAbsolutePath();
            while (cursor != null) {
                Path candidate = cursor.resolveSibling(relative);
                if (Files.isExecutable(candidate)) return candidate.toString();
                cursor = cursor.getParent();
            }
        }
        try {
            Process probe = new ProcessBuilder("python3", "-c", "import vgi_rpc.conformance._cli").start();
            return probe.waitFor() == 0 ? "python3" : null;
        } catch (IOException e) {
            return null;
        }
    }

    private record Pipes(InputStream in, OutputStream out) implements RpcTransport {
        @Override public InputStream reader() { return in; }
        @Override public OutputStream writer() { return out; }
        @Override public void close() {
            try { in.close(); } catch (Exception ignore) { }
            try { out.close(); } catch (Exception ignore) { }
        }
    }

    /** Delegates to a real transport and records whether anyone closed it. */
    private record Tracked(RpcTransport inner, AtomicBoolean closed) implements RpcTransport {
        @Override public InputStream reader() { return inner.reader(); }
        @Override public OutputStream writer() { return inner.writer(); }
        @Override public void close() {
            closed.set(true);
            inner.close();
        }
    }

    /** An {@link HttpClient} that counts the requests sent through it. */
    private static final class CountingHttpClient extends HttpClient {
        final HttpClient delegate;
        final AtomicInteger sent = new AtomicInteger();

        CountingHttpClient(HttpClient delegate) { this.delegate = delegate; }

        @Override public Optional<CookieHandler> cookieHandler() { return delegate.cookieHandler(); }
        @Override public Optional<Duration> connectTimeout() { return delegate.connectTimeout(); }
        @Override public Redirect followRedirects() { return delegate.followRedirects(); }
        @Override public Optional<ProxySelector> proxy() { return delegate.proxy(); }
        @Override public SSLContext sslContext() { return delegate.sslContext(); }
        @Override public SSLParameters sslParameters() { return delegate.sslParameters(); }
        @Override public Optional<Authenticator> authenticator() { return delegate.authenticator(); }
        @Override public Version version() { return delegate.version(); }
        @Override public Optional<Executor> executor() { return delegate.executor(); }

        @Override
        public <T> HttpResponse<T> send(HttpRequest request, HttpResponse.BodyHandler<T> handler)
                throws IOException, InterruptedException {
            sent.incrementAndGet();
            return delegate.send(request, handler);
        }

        @Override
        public <T> CompletableFuture<HttpResponse<T>> sendAsync(HttpRequest request,
                                                                HttpResponse.BodyHandler<T> handler) {
            sent.incrementAndGet();
            return delegate.sendAsync(request, handler);
        }

        @Override
        public <T> CompletableFuture<HttpResponse<T>> sendAsync(HttpRequest request,
                                                                HttpResponse.BodyHandler<T> handler,
                                                                HttpResponse.PushPromiseHandler<T> push) {
            sent.incrementAndGet();
            return delegate.sendAsync(request, handler, push);
        }
    }
}
