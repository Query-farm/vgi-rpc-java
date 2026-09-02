// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import farm.query.vgirpc.RpcError;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class HttpRpcConnectionSocks5hTest {

    @Test
    void sendsUnresolvedOriginToSocksProxyWithoutDirectFallback() throws Exception {
        try (ServerSocket proxy = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            ArrayBlockingQueue<String> observed = new ArrayBlockingQueue<>(1);
            Thread peer = Thread.ofPlatform().start(() -> serveOne(proxy, observed));

            String proxyUri = "socks5h://127.0.0.1:" + proxy.getLocalPort();
            try (HttpRpcConnection connection = HttpRpcConnection.builder(
                    "http://magicdns-name.invalid:9400")
                    .connectTimeout(Duration.ofSeconds(2))
                    .requestTimeout(Duration.ofSeconds(2))
                    .socks5hProxy(proxyUri)
                    .build()) {
                RpcError error = assertThrows(RpcError.class,
                        () -> connection.post("http://magicdns-name.invalid:9400/echo", new byte[0], "echo"));
                assertTrue(error.getMessage().contains(
                        HttpServer.ACCEPT_MAX_RESPONSE_BYTES_SUPPORT_HEADER), error.getMessage());
            }

            assertEquals("magicdns-name.invalid:9400", observed.poll(2, TimeUnit.SECONDS));
            peer.join(Duration.ofSeconds(2));
        }
    }

    @Test
    void rejectsCredentialsAndCustomClientComposition() {
        assertThrows(IllegalArgumentException.class, () -> HttpRpcConnection.builder("http://example.invalid")
                .socks5hProxy("socks5h://user@127.0.0.1:1080"));
        assertThrows(IllegalArgumentException.class, () -> HttpRpcConnection.builder("http://example.invalid")
                .socks5hProxy("http://127.0.0.1:1080"));
        assertThrows(IllegalArgumentException.class, () -> HttpRpcConnection.builder("http://example.invalid")
                .socks5hProxy("socks5h://127.0.0.1:1080")
                .httpClient(java.net.http.HttpClient.newHttpClient())
                .build());
    }

    @Test
    void boundsChunkedResponsesThroughSocksBeforeBuffering() throws Exception {
        try (ServerSocket proxy = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            ArrayBlockingQueue<String> observed = new ArrayBlockingQueue<>(1);
            Thread peer = Thread.ofPlatform().start(() -> serveBudgetResponses(proxy, observed));

            try (HttpRpcConnection connection = HttpRpcConnection.builder(
                    "http://magicdns-name.invalid:9400")
                    .connectTimeout(Duration.ofSeconds(2))
                    .requestTimeout(Duration.ofSeconds(5))
                    .acceptedMaxResponseBytes(64L << 10)
                    .socks5hProxy("socks5h://127.0.0.1:" + proxy.getLocalPort())
                    .build()) {
                RpcError error = assertThrows(RpcError.class,
                        () -> connection.post("http://magicdns-name.invalid:9400/echo",
                                new byte[0], "echo"));
                assertEquals("ResponseTooLargeError", error.errorType());
                assertTrue(error.errorMessage().contains("max_response_bytes"));
            }

            assertEquals("bounded", observed.poll(2, TimeUnit.SECONDS));
            peer.join(Duration.ofSeconds(2));
        }
    }

    private static void serveOne(ServerSocket proxy, ArrayBlockingQueue<String> observed) {
        try (Socket socket = proxy.accept()) {
            socket.setSoTimeout(2_000);
            DataInputStream input = new DataInputStream(socket.getInputStream());
            byte[] greeting = input.readNBytes(3);
            if (greeting.length != 3 || greeting[0] != 5 || greeting[1] != 1 || greeting[2] != 0) {
                throw new AssertionError("unexpected SOCKS5 greeting");
            }
            socket.getOutputStream().write(new byte[]{5, 0});
            byte[] request = input.readNBytes(4);
            if (request.length != 4 || request[0] != 5 || request[1] != 1 || request[3] != 3) {
                throw new AssertionError("origin was not encoded as a SOCKS domain name");
            }
            int nameLength = input.readUnsignedByte();
            String host = new String(input.readNBytes(nameLength), StandardCharsets.US_ASCII);
            int port = input.readUnsignedShort();
            observed.add(host + ":" + port);
            socket.getOutputStream().write(new byte[]{5, 0, 0, 1, 127, 0, 0, 1, 0, 1});

            ByteArrayOutputStream headers = new ByteArrayOutputStream();
            int matched = 0;
            while (matched < 4) {
                int value = input.readUnsignedByte();
                headers.write(value);
                byte[] delimiter = {'\r', '\n', '\r', '\n'};
                matched = value == delimiter[matched] ? matched + 1 : (value == '\r' ? 1 : 0);
            }
            String response = "HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
            socket.getOutputStream().write(response.getBytes(StandardCharsets.US_ASCII));
        } catch (Exception e) {
            observed.offer("ERROR:" + e);
        }
    }

    private static void serveBudgetResponses(ServerSocket proxy, ArrayBlockingQueue<String> observed) {
        try (Socket socket = proxy.accept()) {
            socket.setSoTimeout(5_000);
            DataInputStream input = new DataInputStream(socket.getInputStream());
            completeSocksHandshake(socket, input);

            String options = readHeaders(input);
            if (!options.startsWith("OPTIONS ")) throw new AssertionError(options);
            if (!options.toLowerCase(java.util.Locale.ROOT).contains(
                    "vgi-accept-max-response-bytes: 65536")) {
                throw new AssertionError("OPTIONS omitted accepted response budget: " + options);
            }
            socket.getOutputStream().write((
                    "HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n"
                            + HttpServer.ACCEPT_MAX_RESPONSE_BYTES_SUPPORT_HEADER
                            + ": true\r\nConnection: keep-alive\r\n\r\n")
                    .getBytes(StandardCharsets.US_ASCII));
            socket.getOutputStream().flush();

            String post = readHeaders(input);
            if (!post.startsWith("POST ")) throw new AssertionError(post);
            socket.getOutputStream().write((
                    "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nContent-Type: "
                            + HttpServer.ARROW_CONTENT_TYPE + "\r\n"
                            + HttpServer.ACCEPT_MAX_RESPONSE_BYTES_SUPPORT_HEADER
                            + ": true\r\nConnection: close\r\n\r\n")
                    .getBytes(StandardCharsets.US_ASCII));
            byte[] chunk = new byte[8192];
            for (int i = 0; i < 9; i++) {
                socket.getOutputStream().write("2000\r\n".getBytes(StandardCharsets.US_ASCII));
                socket.getOutputStream().write(chunk);
                socket.getOutputStream().write("\r\n".getBytes(StandardCharsets.US_ASCII));
                socket.getOutputStream().flush();
            }
            observed.offer("bounded");
        } catch (Exception e) {
            observed.offer("ERROR:" + e);
        }
    }

    private static void completeSocksHandshake(Socket socket, DataInputStream input) throws Exception {
        byte[] greeting = input.readNBytes(3);
        if (greeting.length != 3 || greeting[0] != 5 || greeting[1] != 1 || greeting[2] != 0) {
            throw new AssertionError("unexpected SOCKS5 greeting");
        }
        socket.getOutputStream().write(new byte[]{5, 0});
        byte[] request = input.readNBytes(4);
        if (request.length != 4 || request[0] != 5 || request[1] != 1 || request[3] != 3) {
            throw new AssertionError("origin was not encoded as a SOCKS domain name");
        }
        int nameLength = input.readUnsignedByte();
        input.readNBytes(nameLength);
        input.readUnsignedShort();
        socket.getOutputStream().write(new byte[]{5, 0, 0, 1, 127, 0, 0, 1, 0, 1});
        socket.getOutputStream().flush();
    }

    private static String readHeaders(DataInputStream input) throws Exception {
        ByteArrayOutputStream headers = new ByteArrayOutputStream();
        int matched = 0;
        byte[] delimiter = {'\r', '\n', '\r', '\n'};
        while (matched < delimiter.length) {
            int value = input.readUnsignedByte();
            headers.write(value);
            matched = value == delimiter[matched] ? matched + 1 : (value == '\r' ? 1 : 0);
        }
        return headers.toString(StandardCharsets.US_ASCII);
    }
}
