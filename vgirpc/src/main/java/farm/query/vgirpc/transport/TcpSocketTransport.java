// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.transport;

import farm.query.vgirpc.RpcServer;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.function.BiConsumer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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
        serveForever(host, port, server, 0L, null);
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
        ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
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
                            server.serve(t);
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
        }
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
