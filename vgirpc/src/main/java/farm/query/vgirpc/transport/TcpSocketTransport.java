// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.transport;

import farm.query.vgirpc.AuthContext;
import farm.query.vgirpc.AuthScope;
import farm.query.vgirpc.RpcServer;
import farm.query.vgirpc.identity.PeerEvidenceSet;
import farm.query.vgirpc.identity.PeerIdentityProvider;
import farm.query.vgirpc.identity.PeerIdentityRejectedException;
import farm.query.vgirpc.identity.PeerIdentityResult;
import farm.query.vgirpc.identity.PeerIdentityStatus;
import farm.query.vgirpc.identity.PeerIdentityUnavailableException;
import farm.query.vgirpc.identity.PeerResolutionContext;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeoutException;

/**
 * Raw-TCP ({@code AF_INET}) server/client transport — the network analog of
 * {@link UnixSocketTransport}. One instance wraps a single connected socket and
 * speaks the same raw Arrow-IPC framing protocol; only the listening socket
 * differs (host:port instead of a filesystem path).
 *
 * <p><strong>Security:</strong> this transport carries <em>no</em>
 * authentication and <em>no</em> TLS. It is intended for trusted networks only
 * (co-located workers behind a private boundary). The serve loop defaults to
 * loopback ({@code 127.0.0.1}); binding a routable address exposes the
 * unauthenticated, unencrypted framing on the network. For untrusted networks
 * use the HTTP transport, which carries auth middleware and TLS via the
 * fronting server.
 *
 * <p>Nagle's algorithm is disabled ({@code TCP_NODELAY}) so the lockstep
 * request/response framing is not delayed waiting to coalesce small writes.
 */
public final class TcpSocketTransport implements RpcTransport {

    private final Socket socket;
    private final InputStream in;
    private final OutputStream out;

    /**
     * Wrap a connected TCP socket in buffered IO streams, disabling Nagle.
     *
     * @param socket the connected {@code AF_INET} socket (server- or client-side)
     * @throws IOException if the socket streams cannot be opened
     */
    public TcpSocketTransport(Socket socket) throws IOException {
        this.socket = socket;
        try {
            socket.setTcpNoDelay(true);
        } catch (SocketException ignore) {
            // Best-effort: a missing TCP_NODELAY only costs latency, never correctness.
        }
        this.in = new BufferedInputStream(socket.getInputStream(), 1 << 16);
        this.out = new BufferedOutputStream(socket.getOutputStream(), 1 << 16);
    }

    @Override public InputStream reader() { return in; }
    @Override public OutputStream writer() { return out; }

    /** Flush and close both stream directions (closing the underlying socket). */
    @Override public void close() {
        try { out.flush(); } catch (Exception ignore) {}
        try { out.close(); } catch (Exception ignore) {}
        try { in.close(); } catch (Exception ignore) {}
        try { socket.close(); } catch (Exception ignore) {}
    }

    /**
     * Open a client-side TCP connection and wrap it as a transport.
     *
     * @param host the server host to connect to
     * @param port the server TCP port
     * @return a connected client transport
     * @throws IOException if the connection cannot be established
     */
    public static TcpSocketTransport connect(String host, int port) throws IOException {
        return new TcpSocketTransport(new Socket(host, port));
    }

    /** Open a TCP connection through an explicit credential-free SOCKS5h proxy. */
    public static TcpSocketTransport connect(
            String host, int port, String proxy, Duration connectTimeout) throws IOException {
        if (proxy == null) return connect(host, port);
        return new TcpSocketTransport(Socks5h.connect(host, port, proxy, connectTimeout));
    }

    /**
     * Bind to {@code (host, port)} and serve each accepted connection on a
     * dedicated virtual thread, so multiple clients can be active concurrently.
     * Runs until the process is killed.
     *
     * <p>Equivalent to {@link #serveForever(String, int, RpcServer, long, BiConsumer)}
     * with {@code idleTimeoutMs = 0} and no on-bound callback.
     *
     * @param host the interface to bind; {@code 127.0.0.1} restricts to loopback
     * @param port the TCP port to bind; {@code 0} lets the OS pick a free port
     * @param server the dispatcher invoked for every accepted connection
     * @throws IOException if the socket cannot be bound or the accept loop fails
     */
    public static void serveForever(String host, int port, RpcServer server) throws IOException {
        serveForever(host, port, server, 0L, null, TcpServerOptions.defaults());
    }

    /**
     * Bind to {@code (host, port)} and serve each accepted connection on a
     * dedicated virtual thread; optionally self-exit after
     * {@code idleTimeoutMs} milliseconds with zero active connections.
     *
     * <p>After the socket is bound and listening (before the accept loop runs)
     * the {@code onBound} callback — if non-null — is invoked with the bound
     * host and the <em>actual</em> port from {@link ServerSocket#getLocalPort()}
     * (resolved when {@code port == 0}). This is how a worker emits its
     * {@code TCP:<host>:<port>} discovery line only after bind has succeeded.
     *
     * <p>When the idle watchdog fires it closes the server socket; the accept
     * loop catches the resulting {@link SocketException} and returns cleanly so
     * the JVM can exit. {@code idleTimeoutMs <= 0} disables the watchdog.
     *
     * @param host the interface to bind; {@code 127.0.0.1} restricts to loopback
     * @param port the TCP port to bind; {@code 0} lets the OS pick a free port
     * @param server the dispatcher invoked for every accepted connection
     * @param idleTimeoutMs idle period (milliseconds, with no active
     *        connections) after which the server shuts itself down;
     *        {@code <= 0} runs forever
     * @param onBound optional callback invoked with {@code (host, actualPort)}
     *        once bound and listening; may be {@code null}
     * @throws IOException if the socket cannot be bound or the accept loop fails
     */
    public static void serveForever(String host, int port, RpcServer server,
                                     long idleTimeoutMs, BiConsumer<String, Integer> onBound)
            throws IOException {
        serveForever(host, port, server, idleTimeoutMs, onBound, TcpServerOptions.defaults());
    }

    /** Serve raw TCP with optional connection-snapshot peer identity. */
    public static void serveForever(String host, int port, RpcServer server,
                                     TcpServerOptions options) throws IOException {
        serveForever(host, port, server, 0L, null, options);
    }

    /** Full raw-TCP listener overload retaining the existing lifecycle controls. */
    public static void serveForever(String host, int port, RpcServer server,
                                     long idleTimeoutMs, BiConsumer<String, Integer> onBound,
                                     TcpServerOptions options) throws IOException {
        if (options == null) options = TcpServerOptions.defaults();
        final TcpServerOptions configuredOptions = options;
        ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
        ExecutorService identityWorkers = Executors.newVirtualThreadPerTaskExecutor();
        Semaphore identitySlots = new Semaphore(configuredOptions.peerProviderConcurrency());
        AtomicInteger active = new AtomicInteger();
        AtomicLong idleSinceNanos = new AtomicLong(System.nanoTime());
        try (ServerSocket ss = new ServerSocket()) {
            ss.setReuseAddress(true);
            ss.bind(new InetSocketAddress(host, port), 128);
            if (onBound != null) {
                onBound.accept(host, ss.getLocalPort());
            }

            Thread watchdog = idleTimeoutMs > 0
                    ? startIdleWatchdog(ss, active, idleSinceNanos, idleTimeoutMs)
                    : null;
            try {
                while (true) {
                    Socket conn;
                    try {
                        conn = ss.accept();
                    } catch (SocketException e) {
                        // Watchdog (or external close) terminated the listener.
                        return;
                    }
                    active.incrementAndGet();
                    workers.submit(() -> {
                        try (TcpSocketTransport t = new TcpSocketTransport(conn)) {
                            ResolvedIdentity identity = resolveIdentity(
                                    conn, configuredOptions, identityWorkers, identitySlots);
                            String source = endpoint(conn.getInetAddress().getHostAddress(), conn.getPort());
                            try (AutoCloseable ignored = AuthScope.push(identity.auth(),
                                    Map.of("remote_addr", source), identity.evidence())) {
                                server.serve(t);
                            }
                        } catch (Exception ignore) {
                            // Per-connection failure must not take the accept loop down.
                        } finally {
                            if (active.decrementAndGet() == 0) {
                                idleSinceNanos.set(System.nanoTime());
                            }
                        }
                    });
                }
            } finally {
                if (watchdog != null) watchdog.interrupt();
            }
        } finally {
            workers.shutdown();
            identityWorkers.shutdownNow();
        }
    }

    private record ResolvedIdentity(AuthContext auth, PeerEvidenceSet evidence) {}

    private static ResolvedIdentity resolveIdentity(
            Socket socket, TcpServerOptions options, ExecutorService identityWorkers,
            Semaphore identitySlots) throws Exception {
        List<PeerIdentityProvider> providers = options.peerIdentityProviders();
        if (providers.isEmpty()) {
            return new ResolvedIdentity(AuthContext.ANONYMOUS, PeerEvidenceSet.EMPTY);
        }
        long timeoutNanos = options.identityResolutionTimeout().toNanos();
        long startedNanos = System.nanoTime();
        String remoteAddress = socket.getInetAddress().getHostAddress();
        String source = endpoint(remoteAddress, socket.getPort());
        String destination = endpoint(socket.getLocalAddress().getHostAddress(), socket.getLocalPort());
        PeerResolutionContext context = new PeerResolutionContext(
                "tcp", remoteAddress, source, null, destination, null,
                options.peerServiceName(), Map.of(), Map.of("remote_addr", source),
                Instant.now().plus(options.identityResolutionTimeout()));
        List<Callable<PeerIdentityResult>> tasks = providers.stream()
                .<Callable<PeerIdentityResult>>map(provider -> () -> {
                    try {
                        PeerIdentityResult result = provider.resolve(context);
                        if (result == null || !provider.provider().equals(result.provider())) {
                            return new PeerIdentityResult(provider.provider(), PeerIdentityStatus.INVALID);
                        }
                        return result;
                    } catch (PeerIdentityUnavailableException e) {
                        return new PeerIdentityResult(provider.provider(), PeerIdentityStatus.UNAVAILABLE);
                    } catch (PeerIdentityRejectedException e) {
                        return new PeerIdentityResult(provider.provider(), PeerIdentityStatus.INVALID);
                    } catch (RuntimeException e) {
                        return new PeerIdentityResult(provider.provider(), PeerIdentityStatus.INVALID);
                    } finally {
                        identitySlots.release();
                    }
                }).toList();
        List<Future<PeerIdentityResult>> futures = new ArrayList<>(tasks.size());
        List<PeerIdentityResult> results = new ArrayList<>(tasks.size());
        for (int index = 0; index < tasks.size(); index++) {
            if (!identitySlots.tryAcquire()) {
                futures.add(null);
                results.add(new PeerIdentityResult(
                        providers.get(index).provider(), PeerIdentityStatus.UNAVAILABLE));
            } else {
                futures.add(identityWorkers.submit(tasks.get(index)));
                results.add(null);
            }
        }
        for (int index = 0; index < futures.size(); index++) {
            Future<PeerIdentityResult> future = futures.get(index);
            if (future == null) continue;
            long remainingNanos = Math.max(0L, timeoutNanos - (System.nanoTime() - startedNanos));
            try {
                if (future.isDone()) {
                    results.set(index, future.get());
                } else if (remainingNanos == 0L) {
                    results.set(index, unavailable(providers.get(index), future));
                } else {
                    results.set(index, future.get(remainingNanos, TimeUnit.NANOSECONDS));
                }
            } catch (TimeoutException e) {
                results.set(index, unavailable(providers.get(index), future));
            } catch (ExecutionException e) {
                results.set(index, new PeerIdentityResult(
                        providers.get(index).provider(), PeerIdentityStatus.INVALID));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new PeerIdentityUnavailableException("peer identity resolution interrupted");
            }
        }
        PeerEvidenceSet evidence = new PeerEvidenceSet(results);
        AuthContext auth = options.peerAuthenticationPolicy() != null
                ? options.peerAuthenticationPolicy().evaluate(evidence, AuthContext.ANONYMOUS)
                : AuthContext.ANONYMOUS;
        return new ResolvedIdentity(auth, evidence);
    }

    private static PeerIdentityResult unavailable(
            PeerIdentityProvider provider, Future<PeerIdentityResult> future) {
        future.cancel(true);
        return new PeerIdentityResult(provider.provider(), PeerIdentityStatus.UNAVAILABLE);
    }

    private static String endpoint(String address, int port) {
        return address.contains(":") ? "[" + address + "]:" + port : address + ":" + port;
    }

    private static Thread startIdleWatchdog(ServerSocket ss,
                                              AtomicInteger active,
                                              AtomicLong idleSinceNanos,
                                              long idleTimeoutMs) {
        long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(idleTimeoutMs);
        // Poll at 1/10 the timeout (clamped to [100ms, idleTimeoutMs]). Short
        // enough that exit happens close to the configured boundary, long
        // enough that overhead is negligible for production-sized timeouts.
        long pollMs = Math.min(idleTimeoutMs, Math.max(100L, idleTimeoutMs / 10L));
        return Thread.ofVirtual().name("vgi-tcp-idle-watchdog").start(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(pollMs);
                } catch (InterruptedException e) {
                    return;
                }
                if (active.get() == 0
                        && System.nanoTime() - idleSinceNanos.get() >= timeoutNanos) {
                    try { ss.close(); } catch (IOException ignore) {}
                    return;
                }
            }
        });
    }
}
