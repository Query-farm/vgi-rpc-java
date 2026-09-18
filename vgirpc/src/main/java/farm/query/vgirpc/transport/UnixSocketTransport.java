// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.transport;

import farm.query.vgirpc.RpcServer;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Unix-domain-socket transport. One instance wraps a single connection: an accepted one on the
 * server side ({@link #serveForever}), or a client one from {@link #connect(Path, Duration)}.
 */
public final class UnixSocketTransport implements RpcTransport {

    /** Ceiling on the pause between connect attempts while a peer's accept queue is full. */
    private static final long MAX_BUSY_BACKOFF_MS = 50;

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

    /**
     * Connect to a Unix-domain socket as a client, waiting out a full accept queue.
     *
     * <p>{@link #connectChannel(Path, Duration)}, wrapped in a transport.
     *
     * @param socketPath the socket to connect to
     * @param timeout how long to wait out a busy peer's full accept queue; must be positive
     * @return a transport over the connected socket
     * @throws SocketTimeoutException when the accept queue stayed full for {@code timeout}
     * @throws IOException on any other connect failure
     */
    public static UnixSocketTransport connect(Path socketPath, Duration timeout) throws IOException {
        SocketChannel channel = connectChannel(socketPath, timeout);
        try {
            return new UnixSocketTransport(channel);
        } catch (IOException | RuntimeException e) {
            closeQuietly(channel);
            throw e;
        }
    }

    /**
     * Open a blocking client channel to the Unix-domain socket at {@code socketPath}.
     *
     * <p><strong>A full accept queue is a busy peer, not a refusal.</strong> Under a burst of
     * connections a listener's accept queue fills, and Linux then fails a non-blocking AF_UNIX
     * {@code connect} with {@code EAGAIN}. A plain {@link SocketChannel#connect} surfaces that two
     * different ways depending on the calling thread (measured on Linux, JDK 21 and 25): on a
     * platform thread the blocking connect waits for a slot with no bound at all, and on a virtual
     * thread -- where the JDK puts the socket in non-blocking mode -- it fails at once with a bare
     * {@link SocketException} ("Resource temporarily unavailable"). This connects non-blocking on
     * every thread and retries {@code EAGAIN} with a capped backoff (1 ms, doubling to
     * {@value #MAX_BUSY_BACKOFF_MS} ms) until {@code timeout}, so a busy worker costs the caller a
     * short wait instead of a failed call -- which a launcher client answers by relaunching a
     * worker that was fine. Mirrors {@code UnixSocket::Connect} in the vgi DuckDB extension and
     * {@code docs/launcher-protocol.md}: "A client connect that meets a full queue SHOULD wait for
     * a slot until its connect timeout rather than failing."
     *
     * <p>Anything else fails at once: {@link java.net.ConnectException} when nothing is listening
     * ({@code ECONNREFUSED} -- which is also how macOS reports a full queue, so there it cannot be
     * waited out), or the underlying error.
     *
     * @param socketPath the socket to connect to
     * @param timeout how long to wait out a busy peer's full accept queue; must be positive
     * @return a connected channel, in blocking mode
     * @throws SocketTimeoutException when the accept queue stayed full for {@code timeout}; the
     *     last refusal is its cause
     * @throws IOException on any other connect failure
     */
    public static SocketChannel connectChannel(Path socketPath, Duration timeout) throws IOException {
        if (timeout == null || timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("timeout must be positive");
        }
        UnixDomainSocketAddress addr = UnixDomainSocketAddress.of(socketPath);
        long deadline = System.nanoTime() + timeout.toNanos();
        long backoffMs = 1;
        while (true) {
            SocketChannel channel = SocketChannel.open(StandardProtocolFamily.UNIX);
            boolean connected = false;
            try {
                channel.configureBlocking(false);
                if (!channel.connect(addr)) {
                    // AF_UNIX completes (or fails) inside connect() on Linux and macOS; a pending
                    // connect is waited for, but only for what is left of the budget.
                    finishConnect(channel, deadline, socketPath, timeout);
                }
                channel.configureBlocking(true);
                connected = true;
                return channel;
            } catch (IOException e) {
                if (!isAcceptQueueFull(e, socketPath)) throw e;
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    SocketTimeoutException t = new SocketTimeoutException("connect to " + socketPath
                            + " timed out after " + timeout.toMillis()
                            + "ms: the worker's accept queue stayed full");
                    t.initCause(e);
                    throw t;
                }
                pause(Math.min(TimeUnit.MILLISECONDS.toNanos(backoffMs), remaining));
                backoffMs = Math.min(backoffMs * 2, MAX_BUSY_BACKOFF_MS);
            } finally {
                if (!connected) closeQuietly(channel);
            }
        }
    }

    /**
     * Whether a failed AF_UNIX connect to {@code socketPath} met a live listener whose accept
     * queue is full -- a busy peer -- rather than no listener at all.
     *
     * <p>Linux fails such a connect with {@code EAGAIN}, and the JDK hands that back as a bare
     * {@link SocketException} whose only distinguishing mark is {@code strerror}'s text -- which
     * the process locale translates, so this does not read the message. It reads what the JDK
     * does encode in the exception type, plus the filesystem. {@code ECONNREFUSED} (nothing
     * listening) is a {@link java.net.ConnectException}, {@code EACCES} a
     * {@link java.net.BindException}, and {@code ENOENT} leaves no socket at the path; a bare
     * {@code SocketException} on a path that is still a socket inode means something is bound
     * there and the kernel turned the connect away for another reason. On Linux that reason is
     * the full queue.
     *
     * <p>The rarer errors of the same shape ({@code EPROTOTYPE}, a datagram socket bound at the
     * path; {@code EPERM}, a security module refusing the connect) are read as busy too. That is
     * the safe direction for both callers: the launcher's probe leaves the socket alone rather
     * than unlinking it, and {@link #connectChannel} waits out its timeout and then fails with
     * the real error as the cause. The unsafe direction -- a busy worker read as a dead one -- is
     * what this exists to close: the launcher unlinked the live worker's socket and spawned a
     * duplicate on every busy probe.
     *
     * <p>macOS reports a full queue as {@code ECONNREFUSED}, indistinguishable from no listener,
     * so this is always {@code false} there; the launcher re-probes a refusal briefly instead.
     *
     * @param failure the exception a connect to {@code socketPath} failed with
     * @param socketPath the path that was connected to
     * @return {@code true} when the peer is alive but its accept queue is full
     */
    public static boolean isAcceptQueueFull(IOException failure, Path socketPath) {
        if (failure == null || failure.getClass() != SocketException.class) return false;
        try {
            return isUnixSocket(socketPath);
        } catch (IOException | UnsupportedOperationException e) {
            return false;
        }
    }

    private static void finishConnect(SocketChannel channel, long deadline, Path socketPath,
                                      Duration timeout) throws IOException {
        try (Selector selector = Selector.open()) {
            channel.register(selector, SelectionKey.OP_CONNECT);
            while (!channel.finishConnect()) {
                long remainingMs = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
                if (remainingMs <= 0) {
                    throw new SocketTimeoutException("connect to " + socketPath + " timed out after "
                            + timeout.toMillis() + "ms");
                }
                selector.select(remainingMs);
                selector.selectedKeys().clear();
            }
        }
    }

    private static void pause(long nanos) throws InterruptedIOException {
        try {
            TimeUnit.NANOSECONDS.sleep(nanos);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException("interrupted waiting for a full accept queue to drain");
        }
    }

    private static void closeQuietly(SocketChannel channel) {
        try {
            channel.close();
        } catch (IOException ignore) {
            // Nothing useful to do: the connect already failed or the transport never formed.
        }
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
     * @param socketPath filesystem path for the Unix-domain socket; a stale
     *        socket at this path is deleted before binding, but other file
     *        types are preserved and rejected
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
     * @param socketPath filesystem path for the Unix-domain socket; a stale
     *        socket at this path is deleted before binding, but other file
     *        types are preserved and rejected
     * @param server the dispatcher invoked for every accepted connection
     * @param idleTimeoutMs idle period (milliseconds, with no active
     *        connections) after which the server shuts itself down;
     *        {@code <= 0} runs forever
     * @throws IOException if the socket cannot be bound or the accept loop fails
     */
    public static void serveForever(Path socketPath, RpcServer server, long idleTimeoutMs)
            throws IOException {
        removeStaleSocket(socketPath);
        UnixDomainSocketAddress addr = UnixDomainSocketAddress.of(socketPath);
        ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
        AtomicInteger active = new AtomicInteger();
        AtomicLong idleSinceNanos = new AtomicLong(System.nanoTime());
        Object boundFileKey = null;
        try (ServerSocketChannel ssc = ServerSocketChannel.open(StandardProtocolFamily.UNIX)) {
            ssc.bind(addr);
            boundFileKey = Files.readAttributes(
                    socketPath, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS).fileKey();
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
            removeBoundSocket(socketPath, boundFileKey);
        }
    }

    private static void removeStaleSocket(Path path) throws IOException {
        if (!Files.exists(path, LinkOption.NOFOLLOW_LINKS)) return;
        if (!isUnixSocket(path)) {
            throw new FileAlreadyExistsException(
                    path.toString(), null, "refusing to replace a non-socket filesystem entry");
        }
        Files.delete(path);
    }

    private static void removeBoundSocket(Path path, Object expectedFileKey) {
        try {
            if (!Files.exists(path, LinkOption.NOFOLLOW_LINKS) || !isUnixSocket(path)) return;
            Object actualFileKey = Files.readAttributes(
                    path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS).fileKey();
            if (expectedFileKey == null || expectedFileKey.equals(actualFileKey)) Files.delete(path);
        } catch (IOException ignore) {
            // Cleanup is best-effort; never delete an entry whose identity or
            // type cannot be proved to be the socket this server bound.
        }
    }

    private static boolean isUnixSocket(Path path) throws IOException {
        Object rawMode = Files.getAttribute(path, "unix:mode", LinkOption.NOFOLLOW_LINKS);
        if (!(rawMode instanceof Integer mode)) return false;
        return (mode & 0170000) == 0140000;
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
