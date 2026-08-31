// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0
package farm.query.vgirpc.transport;

import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.io.InputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class Socks5hTest {
    @Test void sendsUnicodeTargetAsIdnaWithoutLocalResolutionAndHandlesFragments() throws Exception {
        try (FakeProxy proxy = new FakeProxy(new byte[]{5, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1})) {
            try (Socket ignored = Socks5h.connect("café.invalid", 9400, proxy.uri(), Duration.ofSeconds(2))) {}
            Request request = proxy.request().join();
            assertEquals(3, request.atyp());
            assertArrayEquals("xn--caf-dma.invalid".getBytes(java.nio.charset.StandardCharsets.US_ASCII), request.address());
            assertEquals(9400, request.port());
        }
    }

    @Test void sendsIpv4AndIpv6TargetsWithoutDns() throws Exception {
        try (FakeProxy ipv4 = new FakeProxy(new byte[]{5, 0, 0, 1, 127, 0, 0, 1, 0, 1})) {
            try (Socket ignored = Socks5h.connect("192.0.2.1", 80, ipv4.uri(), Duration.ofSeconds(2))) {}
            assertEquals(1, ipv4.request().join().atyp());
        }
        try (FakeProxy ipv6 = new FakeProxy(new byte[]{5, 0, 0, 3, 3, 'f', 'o', 'o', 0, 1})) {
            try (Socket ignored = Socks5h.connect("2001:db8::1", 443, ipv6.uri(), Duration.ofSeconds(2))) {}
            assertEquals(4, ipv6.request().join().atyp());
        }
    }

    @Test void rejectsCredentialsAndNeverFallsBackDirect() throws Exception {
        assertThrows(IllegalArgumentException.class, () ->
                Socks5h.connect("example.invalid", 9400, "socks5h://user:pass@127.0.0.1:9", Duration.ofSeconds(1)));
        assertThrows(Exception.class, () ->
                Socks5h.connect("127.0.0.1", 9, "socks5h://127.0.0.1:1", Duration.ofMillis(100)));
    }

    @Test void setupDeadlineCoversNegotiation() throws Exception {
        try (ServerSocket listener = new ServerSocket(0)) {
            Thread stalled = Thread.ofVirtual().start(() -> {
                try (Socket peer = listener.accept()) { Thread.sleep(500); } catch (Exception ignored) {}
            });
            assertThrows(java.net.SocketTimeoutException.class, () -> Socks5h.connect(
                    "example.invalid", 9400, "socks5h://127.0.0.1:" + listener.getLocalPort(), Duration.ofMillis(50)));
            stalled.join();
        }
    }

    @Test void interruptCancelsAStalledNegotiation() throws Exception {
        try (ServerSocket listener = new ServerSocket(0)) {
            CountDownLatch accepted = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            Thread server = Thread.ofVirtual().start(() -> {
                try (Socket peer = listener.accept()) {
                    accepted.countDown();
                    release.await();
                } catch (Exception ignored) {}
            });
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread client = Thread.ofVirtual().start(() -> {
                try {
                    Socks5h.connect("example.invalid", 9400,
                            "socks5h://127.0.0.1:" + listener.getLocalPort(), Duration.ofSeconds(30));
                } catch (Throwable error) {
                    failure.set(error);
                }
            });
            assertTrue(accepted.await(1, java.util.concurrent.TimeUnit.SECONDS));
            client.interrupt();
            client.join(1_000);
            release.countDown();
            server.join();
            assertFalse(client.isAlive());
            assertInstanceOf(java.io.InterruptedIOException.class, failure.get());
        }
    }

    @Test void rejectsProxyFailureAndTruncatedRepliesWithoutFallback() throws Exception {
        for (byte[] reply : new byte[][]{
                {5, 5, 0, 1, 127, 0, 0, 1, 0, 1},
                {5, 0, 0, 4, 0, 0, 0},
        }) {
            try (FakeProxy proxy = new FakeProxy(reply)) {
                assertThrows(IOException.class, () -> Socks5h.connect(
                        "127.0.0.1", 9, proxy.uri(), Duration.ofSeconds(2)));
                assertEquals(1, proxy.request().join().atyp());
            }
        }
    }

    private record Request(int atyp, byte[] address, int port) {}

    private static final class FakeProxy implements AutoCloseable {
        private final ServerSocket listener = new ServerSocket(0);
        private final CompletableFuture<Request> request = new CompletableFuture<>();
        private final Thread thread;

        FakeProxy(byte[] reply) throws Exception {
            thread = Thread.ofVirtual().start(() -> {
                try (Socket peer = listener.accept()) {
                    InputStream in = peer.getInputStream();
                    assertArrayEquals(new byte[]{5, 1, 0}, read(in, 3));
                    peer.getOutputStream().write(5);
                    peer.getOutputStream().flush();
                    peer.getOutputStream().write(0);
                    peer.getOutputStream().flush();
                    byte[] header = read(in, 4);
                    int atyp = header[3] & 0xff;
                    int size = switch (atyp) { case 1 -> 4; case 4 -> 16; case 3 -> in.read(); default -> -1; };
                    if (size < 0) throw new EOFException();
                    byte[] address = read(in, size);
                    byte[] port = read(in, 2);
                    request.complete(new Request(atyp, address, ((port[0] & 0xff) << 8) | (port[1] & 0xff)));
                    for (byte value : reply) {
                        peer.getOutputStream().write(value);
                        peer.getOutputStream().flush();
                    }
                } catch (Throwable error) {
                    request.completeExceptionally(error);
                }
            });
        }

        String uri() { return "socks5h://127.0.0.1:" + listener.getLocalPort(); }
        CompletableFuture<Request> request() { return request; }
        @Override public void close() throws Exception { listener.close(); thread.join(); }
        private static byte[] read(InputStream input, int size) throws Exception {
            byte[] value = new byte[size];
            int offset = 0;
            while (offset < size) {
                int count = input.read(value, offset, size - offset);
                if (count < 0) throw new EOFException();
                offset += count;
            }
            return value;
        }
    }
}
