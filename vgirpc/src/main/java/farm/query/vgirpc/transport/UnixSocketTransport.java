// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.transport;

import farm.query.vgirpc.RpcServer;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** Unix-domain-socket server transport. One instance wraps a single accepted connection. */
public final class UnixSocketTransport implements RpcTransport {

    private final InputStream in;
    private final OutputStream out;

    /**
     * Wrap an accepted socket channel in buffered IO streams.
     *
     * @param channel the connected Unix-domain socket channel
     * @throws IOException if the channel streams cannot be opened
     */
    public UnixSocketTransport(SocketChannel channel) throws IOException {
        this.in = new BufferedInputStream(Channels.newInputStream(channel), 1 << 16);
        this.out = new BufferedOutputStream(Channels.newOutputStream(channel), 1 << 16);
    }

    @Override public InputStream reader() { return in; }
    @Override public OutputStream writer() { return out; }
    /** Flush and close both stream directions (closing the underlying channel). */
    @Override public void close() {
        try { out.flush(); } catch (Exception ignore) {}
        try { out.close(); } catch (Exception ignore) {}
        try { in.close(); } catch (Exception ignore) {}
    }

    /**
     * Bind to the given socket path and serve each accepted connection on a
     * dedicated virtual thread, so multiple clients can be active concurrently.
     * The caller's {@link RpcServer} must be safe for concurrent dispatch
     * (the default {@code RpcServer} is — the user's service impl must be
     * too).
     *
     * <p>Equivalent to {@link #serveForever(Path, RpcServer, long)} with
     * {@code idleTimeoutMs = 0} — server runs until the process is killed.
     *
     * @param socketPath filesystem path for the Unix-domain socket; any stale
     *        file at this path is deleted before binding
     * @param server the dispatcher invoked for every accepted connection
     * @throws IOException if the socket cannot be bound or the accept loop fails
     */
    public static void serveForever(Path socketPath, RpcServer server) throws IOException {
        serveForever(socketPath, server, 0L);
    }

    /**
     * Variant that self-exits after {@code idleTimeoutMs} milliseconds with
     * zero active connections. {@code idleTimeoutMs <= 0} disables the
     * watchdog and runs forever, matching the no-timeout overload.
     *
     * <p>When the watchdog fires it closes the server channel; the accept
     * loop catches the resulting {@link ClosedChannelException} (the
     * superclass of {@code AsynchronousCloseException}) and returns cleanly
     * so the JVM can exit.
     *
     * @param socketPath filesystem path for the Unix-domain socket; any stale
     *        file at this path is deleted before binding
     * @param server the dispatcher invoked for every accepted connection
     * @param idleTimeoutMs idle period (milliseconds, with no active
     *        connections) after which the server shuts itself down;
     *        {@code <= 0} runs forever
     * @throws IOException if the socket cannot be bound or the accept loop fails
     */
    public static void serveForever(Path socketPath, RpcServer server, long idleTimeoutMs)
            throws IOException {
        Files.deleteIfExists(socketPath);
        UnixDomainSocketAddress addr = UnixDomainSocketAddress.of(socketPath);
        ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
        AtomicInteger active = new AtomicInteger();
        AtomicLong idleSinceNanos = new AtomicLong(System.nanoTime());
        try (ServerSocketChannel ssc = ServerSocketChannel.open(StandardProtocolFamily.UNIX)) {
            ssc.bind(addr);
            System.out.println("UNIX:" + socketPath);
            System.out.flush();

            Thread watchdog = idleTimeoutMs > 0
                    ? startIdleWatchdog(ssc, active, idleSinceNanos, idleTimeoutMs)
                    : null;
            try {
                while (true) {
                    SocketChannel channel;
                    try {
                        channel = ssc.accept();
                    } catch (ClosedChannelException e) {
                        // Watchdog (or external close) terminated the listener.
                        return;
                    }
                    active.incrementAndGet();
                    workers.submit(() -> {
                        try (UnixSocketTransport t = new UnixSocketTransport(channel)) {
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

    private static Thread startIdleWatchdog(ServerSocketChannel ssc,
                                              AtomicInteger active,
                                              AtomicLong idleSinceNanos,
                                              long idleTimeoutMs) {
        long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(idleTimeoutMs);
        // Poll at 1/10 the timeout (clamped to [100ms, idleTimeoutMs]). Short
        // enough that exit happens close to the configured boundary, long
        // enough that overhead is negligible for production-sized timeouts.
        long pollMs = Math.min(idleTimeoutMs, Math.max(100L, idleTimeoutMs / 10L));
        return Thread.ofVirtual().name("vgi-idle-watchdog").start(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(pollMs);
                } catch (InterruptedException e) {
                    return;
                }
                if (active.get() == 0
                        && System.nanoTime() - idleSinceNanos.get() >= timeoutNanos) {
                    try { ssc.close(); } catch (IOException ignore) {}
                    return;
                }
            }
        });
    }
}
