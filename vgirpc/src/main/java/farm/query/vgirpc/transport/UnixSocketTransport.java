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

/** Unix-domain-socket server transport: one client connection at a time. */
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
     * Bind to the given socket path, then serve connections sequentially using
     * {@code server.serve(transport)}. Each accepted connection gets its own
     * {@link UnixSocketTransport}.
     */
    public static void serveForever(Path socketPath, RpcServer server) throws IOException {
        Files.deleteIfExists(socketPath);
        UnixDomainSocketAddress addr = UnixDomainSocketAddress.of(socketPath);
        try (ServerSocketChannel ssc = ServerSocketChannel.open(StandardProtocolFamily.UNIX)) {
            ssc.bind(addr);
            System.out.println("UNIX:" + socketPath);
            System.out.flush();
            while (true) {
                SocketChannel channel = ssc.accept();
                try (UnixSocketTransport t = new UnixSocketTransport(channel)) {
                    server.serve(t);
                } catch (Exception e) {
                    // Continue accepting next connection
                }
            }
        }
    }
}
