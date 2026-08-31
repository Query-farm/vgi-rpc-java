// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0
package farm.query.vgirpc.transport;

import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.IDN;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.StandardSocketOptions;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/** Credential-free SOCKS5h dialing with proxy-side target resolution. */
public final class Socks5h {
    private Socks5h() {}

    /** Connect to {@code targetHost:targetPort} through an explicit SOCKS5h proxy. */
    public static Socket connect(String targetHost, int targetPort, String proxyUri, Duration timeout)
            throws IOException {
        long started = System.nanoTime();
        long budget = positiveNanos(timeout);
        ProxyEndpoint proxy = parseProxy(proxyUri);
        Target target = target(targetHost, targetPort);
        IOException last = null;

        // The JDK's blocking resolver cannot be interrupted. Its elapsed time is still charged
        // to the one monotonic setup budget; the target is never passed to it.
        InetAddress[] addresses = InetAddress.getAllByName(proxy.host());
        for (InetAddress address : addresses) {
            SocketChannel channel = SocketChannel.open();
            boolean connected = false;
            try {
                channel.configureBlocking(false);
                channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
                try (Selector selector = Selector.open()) {
                    SelectionKey key = channel.register(selector, 0);
                    connect(channel, new InetSocketAddress(address, proxy.port()), selector, key,
                            started, budget);
                    negotiate(channel, selector, key, target, started, budget);
                }
                channel.configureBlocking(true);
                connected = true;
                return channel.socket();
            } catch (IOException | RuntimeException e) {
                if (e instanceof IOException io) last = io;
                else throw e;
            } finally {
                if (!connected) try { channel.close(); } catch (IOException ignored) {}
            }
        }
        throw last != null ? last : new IOException("SOCKS5h proxy resolved without usable addresses");
    }

    private static void connect(SocketChannel channel, InetSocketAddress address, Selector selector,
                                SelectionKey key, long started, long budget) throws IOException {
        if (channel.connect(address)) return;
        while (!channel.finishConnect()) await(selector, key, SelectionKey.OP_CONNECT, started, budget);
    }

    private static void negotiate(SocketChannel channel, Selector selector, SelectionKey key,
                                  Target target, long started, long budget) throws IOException {
        write(channel, selector, key, new byte[]{5, 1, 0}, started, budget);
        byte[] greeting = read(channel, selector, key, 2, started, budget);
        if (greeting[0] != 5 || greeting[1] != 0) throw new IOException("SOCKS5h proxy did not accept NO AUTH");

        byte[] request = new byte[4 + target.address().length + 2];
        request[0] = 5;
        request[1] = 1;
        request[2] = 0;
        request[3] = target.atyp();
        System.arraycopy(target.address(), 0, request, 4, target.address().length);
        request[request.length - 2] = (byte) (target.port() >>> 8);
        request[request.length - 1] = (byte) target.port();
        write(channel, selector, key, request, started, budget);

        byte[] reply = read(channel, selector, key, 4, started, budget);
        if (reply[0] != 5 || reply[2] != 0) throw new IOException("malformed SOCKS5h reply");
        if (reply[1] != 0) throw new IOException("SOCKS5h proxy rejected target (reply " + (reply[1] & 0xff) + ")");
        int addressLength = switch (reply[3] & 0xff) {
            case 1 -> 4;
            case 4 -> 16;
            case 3 -> read(channel, selector, key, 1, started, budget)[0] & 0xff;
            default -> throw new IOException("SOCKS5h proxy returned an invalid address type");
        };
        read(channel, selector, key, addressLength + 2, started, budget);
    }

    private static byte[] read(SocketChannel channel, Selector selector, SelectionKey key,
                               int length, long started, long budget) throws IOException {
        ByteBuffer value = ByteBuffer.allocate(length);
        while (value.hasRemaining()) {
            int count = channel.read(value);
            if (count < 0) throw new EOFException("SOCKS5h proxy returned a truncated reply");
            if (count == 0) await(selector, key, SelectionKey.OP_READ, started, budget);
        }
        return value.array();
    }

    private static void write(SocketChannel channel, Selector selector, SelectionKey key,
                              byte[] value, long started, long budget) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(value);
        while (buffer.hasRemaining()) {
            if (channel.write(buffer) == 0) await(selector, key, SelectionKey.OP_WRITE, started, budget);
        }
    }

    private static void await(Selector selector, SelectionKey key, int operation,
                              long started, long budget) throws IOException {
        key.interestOps(operation);
        while (true) {
            if (Thread.interrupted()) throw interrupted();
            int selected = selector.select(remainingMillis(started, budget));
            if (Thread.interrupted()) throw interrupted();
            if (selected > 0) {
                selector.selectedKeys().clear();
                return;
            }
        }
    }

    private static InterruptedIOException interrupted() {
        Thread.currentThread().interrupt();
        return new InterruptedIOException("SOCKS5h setup interrupted");
    }

    private static Target target(String host, int port) throws IOException {
        if (host == null || host.isBlank() || port < 1 || port > 65535 || containsControl(host)) {
            throw new IllegalArgumentException("invalid SOCKS5h target");
        }
        byte[] ipv4 = ipv4(host);
        if (ipv4 != null) return new Target((byte) 1, ipv4, port);
        if (host.indexOf(':') >= 0) {
            if (host.indexOf('%') >= 0) throw new IllegalArgumentException("scoped IPv6 targets are unsupported");
            InetAddress parsed = InetAddress.getByName(host);
            if (!(parsed instanceof Inet6Address)) throw new IllegalArgumentException("invalid IPv6 target");
            return new Target((byte) 4, parsed.getAddress(), port);
        }
        String ascii = IDN.toASCII(host, IDN.USE_STD3_ASCII_RULES);
        if (ascii.isEmpty() || ascii.length() > 253 || containsControl(ascii)) {
            throw new IllegalArgumentException("invalid SOCKS5h target hostname");
        }
        byte[] encoded = ascii.getBytes(StandardCharsets.US_ASCII);
        if (encoded.length > 255) throw new IllegalArgumentException("SOCKS5h target hostname exceeds 255 bytes");
        byte[] domain = new byte[encoded.length + 1];
        domain[0] = (byte) encoded.length;
        System.arraycopy(encoded, 0, domain, 1, encoded.length);
        return new Target((byte) 3, domain, port);
    }

    private static byte[] ipv4(String host) {
        String[] parts = host.split("\\.", -1);
        if (parts.length != 4) return null;
        byte[] address = new byte[4];
        for (int i = 0; i < 4; i++) {
            if (parts[i].isEmpty() || (parts[i].length() > 1 && parts[i].charAt(0) == '0')) return null;
            int octet;
            try { octet = Integer.parseInt(parts[i]); } catch (NumberFormatException e) { return null; }
            if (octet < 0 || octet > 255) return null;
            address[i] = (byte) octet;
        }
        return address;
    }

    private static ProxyEndpoint parseProxy(String value) {
        URI uri = URI.create(value);
        if (!"socks5h".equals(uri.getScheme()) || uri.getRawUserInfo() != null || uri.getHost() == null
                || uri.getPort() < 1 || uri.getPort() > 65535 || uri.getRawQuery() != null
                || uri.getRawFragment() != null || !(uri.getRawPath() == null || uri.getRawPath().isEmpty())) {
            throw new IllegalArgumentException("proxy must be credential-free socks5h://host:port");
        }
        return new ProxyEndpoint(uri.getHost(), uri.getPort());
    }

    private static long positiveNanos(Duration timeout) {
        if (timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("SOCKS5h setup timeout must be positive");
        }
        try { return timeout.toNanos(); } catch (ArithmeticException e) { return Long.MAX_VALUE; }
    }

    private static int remainingMillis(long started, long budget) throws java.net.SocketTimeoutException {
        long remaining = budget - (System.nanoTime() - started);
        if (remaining <= 0) throw new java.net.SocketTimeoutException("SOCKS5h setup timed out");
        long millis = remaining / 1_000_000L + (remaining % 1_000_000L == 0 ? 0 : 1);
        return (int) Math.min(Integer.MAX_VALUE, Math.max(1L, millis));
    }

    private static boolean containsControl(String value) {
        return value.codePoints().anyMatch(code -> code <= 0x1f || code == 0x7f);
    }

    private record ProxyEndpoint(String host, int port) {}
    private record Target(byte atyp, byte[] address, int port) {}
}
