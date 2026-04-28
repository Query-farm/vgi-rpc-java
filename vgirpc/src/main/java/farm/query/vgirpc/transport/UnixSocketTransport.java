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
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** Unix-domain-socket server transport. One instance wraps a single accepted connection. */
public final class UnixSocketTransport implements RpcTransport {

    private final InputStream in;
    private final OutputStream out;

    public UnixSocketTransport(SocketChannel channel) throws IOException {
        this.in = new BufferedInputStream(Channels.newInputStream(channel), 1 << 16);
        this.out = new BufferedOutputStream(Channels.newOutputStream(channel), 1 << 16);
    }

    @Override public InputStream reader() { return in; }
    @Override public OutputStream writer() { return out; }
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
     */
    public static void serveForever(Path socketPath, RpcServer server) throws IOException {
        Files.deleteIfExists(socketPath);
        UnixDomainSocketAddress addr = UnixDomainSocketAddress.of(socketPath);
        ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
        try (ServerSocketChannel ssc = ServerSocketChannel.open(StandardProtocolFamily.UNIX)) {
            ssc.bind(addr);
            System.out.println("UNIX:" + socketPath);
            System.out.flush();
            while (true) {
                SocketChannel channel = ssc.accept();
                workers.submit(() -> {
                    try (UnixSocketTransport t = new UnixSocketTransport(channel)) {
                        server.serve(t);
                    } catch (Exception ignore) {
                        // Per-connection failure must not take the accept loop down.
                    }
                });
            }
        } finally {
            workers.shutdown();
        }
    }
}
