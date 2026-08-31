// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0
package farm.query.vgirpc.identity;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.net.ServerSocket;
import java.net.URI;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.channels.Channels;
import java.nio.channels.ServerSocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class TailscaleLocalApiProviderTest {
    @Test void performsAnUncachedLookupAndUsesStableUserIdentity() {
        AtomicInteger calls = new AtomicInteger();
        var provider = new TailscaleLocalApiProvider("tailnet:example", context -> {
            calls.incrementAndGet();
            return json(200, """
                    {"Node":{"StableID":"node-1","Name":"workstation"},
                     "UserProfile":{"ID":42,"LoginName":"alice@example.com","DisplayName":"Alice"},
                     "CapMap":{"query.farm/cap":[{"role":"reader"}]}}
                    """);
        });
        var first = provider.resolve(context(null, "100.64.0.1:1234"));
        var second = provider.resolve(context(null, "100.64.0.1:1234"));
        assertEquals(2, calls.get());
        assertEquals(PeerIdentityStatus.AVAILABLE, first.status());
        assertEquals("user:42", first.identities().getFirst().subjectKey());
        assertEquals(PeerSubjectKind.USER, first.identities().getFirst().subjectKind());
        assertEquals(SubjectStability.STABLE, second.identities().getFirst().subjectStability());
        assertEquals(Map.of("kind", "destination_ip", "value", "100.100.100.100"),
                first.identities().getFirst().attributes().get("capability_target"));
        assertEquals("100.64.0.1", first.identities().getFirst().sourceAddress());
    }

    @Test void taggedNodeIsThePrincipalAndStatusFailuresRemainDistinct() {
        var tagged = new TailscaleLocalApiProvider("tailnet:example", ignored -> json(200, """
                {"Node":{"StableID":"n123","Tags":["tag:worker"]},
                 "UserProfile":{"ID":99,"LoginName":"owner@example.com"}}
                """)).resolve(context("svc:vgi", null));
        assertEquals("node:n123", tagged.identities().getFirst().subjectKey());
        assertEquals(PeerSubjectKind.TAGGED_NODE, tagged.identities().getFirst().subjectKind());
        assertEquals(PeerIdentityStatus.NO_MATCH, status(404));
        assertEquals(PeerIdentityStatus.PERMISSION_DENIED, status(403));
        assertEquals(PeerIdentityStatus.UNAVAILABLE, status(500));
        assertEquals(PeerIdentityStatus.INVALID, status(302));
        assertEquals(PeerIdentityStatus.INVALID, status(400));
        for (String malformed : new String[]{"[]", "{\"Node\":{},\"UserProfile\":{}}",
                "{\"Node\":{\"ID\":\"legacy\",\"Tags\":[\"tag:worker\"]},\"UserProfile\":{\"ID\":1}}",
                "{\"Node\":{\"StableID\":\"n1\",\"Tags\":\"tag:worker\"},\"UserProfile\":{\"ID\":1}}",
                "{\"Node\":{\"StableID\":\"n1\",\"Tags\":[\"worker\"]},\"UserProfile\":{\"ID\":1}}",
                "{\"Node\":{\"StableID\":\"n1\",\"Tags\":[\"tag:bad name\"]},\"UserProfile\":{\"ID\":1}}",
                "{\"Node\":{},\"UserProfile\":{\"ID\":\"42\"}}",
                "{\"Node\":{},\"UserProfile\":{\"ID\":0}}",
                "{\"Node\":{},\"UserProfile\":{\"ID\":-1}}",
                "{\"Node\":{},\"UserProfile\":{\"ID\":1},\"CapMap\":{\"x\":[],\"x\":[]}}"}) {
            var provider = new TailscaleLocalApiProvider("tailnet:example", ignored -> json(200, malformed));
            assertEquals(PeerIdentityStatus.INVALID, provider.resolve(context(null, null)).status());
        }
    }

    @Test void explicitHttpTransportSetsHostTokenAndDestinationQuery() throws Exception {
        try (ServerSocket listener = new ServerSocket(0)) {
            CompletableFuture<String> request = new CompletableFuture<>();
            Thread server = Thread.ofVirtual().start(() -> {
                try (var socket = listener.accept()) {
                    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                    int state = 0;
                    while (state < 4) {
                        int value = socket.getInputStream().read();
                        if (value < 0) throw new java.io.EOFException();
                        bytes.write(value);
                        state = switch (state) {
                            case 0 -> value == '\r' ? 1 : 0;
                            case 1 -> value == '\n' ? 2 : 0;
                            case 2 -> value == '\r' ? 3 : 0;
                            default -> value == '\n' ? 4 : 0;
                        };
                    }
                    request.complete(bytes.toString(StandardCharsets.ISO_8859_1));
                    byte[] body = "{\"Node\":{},\"UserProfile\":{\"ID\":7}}".getBytes(StandardCharsets.UTF_8);
                    socket.getOutputStream().write(("HTTP/1.1 200 OK\r\nContent-Length: " + body.length
                            + "\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n")
                            .getBytes(StandardCharsets.US_ASCII));
                    socket.getOutputStream().write(body);
                } catch (Throwable error) { request.completeExceptionally(error); }
            });
            var client = new TailscaleLocalApiProvider.HttpLocalApiClient(
                    URI.create("http://127.0.0.1:" + listener.getLocalPort()), "secret", Duration.ofSeconds(2));
            var result = new TailscaleLocalApiProvider("tailnet:example", client)
                    .resolve(context(null, "100.64.0.1:1234"));
            assertEquals(PeerIdentityStatus.AVAILABLE, result.status());
            String headers = request.join();
            assertTrue(headers.startsWith("GET /localapi/v0/whois?addr=100.64.0.1%3A1234&proto=tcp"
                    + "&dst_ip=100.100.100.100 HTTP/1.1\r\n"));
            assertTrue(headers.contains("\r\nHost: local-tailscaled.sock\r\n"));
            assertTrue(headers.contains("\r\nAuthorization: Basic OnNlY3JldA==\r\n"));
            server.join();
        }
    }

    @Test void unixSocketTransportPerformsWhoIs() throws Exception {
        Path socketPath = Files.createTempFile("vgi-localapi-", ".sock");
        Files.delete(socketPath);
        try (ServerSocketChannel listener = ServerSocketChannel.open(StandardProtocolFamily.UNIX)) {
            listener.bind(UnixDomainSocketAddress.of(socketPath));
            Thread server = Thread.ofVirtual().start(() -> {
                try (var channel = listener.accept()) {
                    var input = Channels.newInputStream(channel);
                    int state = 0;
                    while (state < 4) {
                        int value = input.read();
                        if (value < 0) throw new java.io.EOFException();
                        state = switch (state) {
                            case 0 -> value == '\r' ? 1 : 0;
                            case 1 -> value == '\n' ? 2 : 0;
                            case 2 -> value == '\r' ? 3 : 0;
                            default -> value == '\n' ? 4 : 0;
                        };
                    }
                    byte[] body = "{\"Node\":{},\"UserProfile\":{\"ID\":8}}".getBytes(StandardCharsets.UTF_8);
                    var output = Channels.newOutputStream(channel);
                    output.write(("HTTP/1.1 200 OK\r\nContent-Length: " + body.length
                            + "\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n")
                            .getBytes(StandardCharsets.US_ASCII));
                    output.write(body);
                } catch (Exception error) { throw new RuntimeException(error); }
            });
            var result = new TailscaleLocalApiProvider("tailnet:example",
                    new TailscaleLocalApiProvider.UnixLocalApiClient(socketPath, null))
                    .resolve(context(null, null));
            assertEquals(PeerIdentityStatus.AVAILABLE, result.status());
            assertEquals("user:8", result.identities().getFirst().subjectKey());
            server.join();
        } finally {
            Files.deleteIfExists(socketPath);
        }
    }

    @Test void httpTransportRejectsAmbiguousFraming() throws Exception {
        try (ServerSocket listener = new ServerSocket(0)) {
            Thread server = Thread.ofVirtual().start(() -> {
                try (var socket = listener.accept()) {
                    readHeaders(socket.getInputStream());
                    socket.getOutputStream().write(("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n"
                            + "content-length: 2\r\nConnection: close\r\n\r\n{}").getBytes(StandardCharsets.US_ASCII));
                } catch (Exception ignored) {}
            });
            var client = new TailscaleLocalApiProvider.HttpLocalApiClient(
                    URI.create("http://127.0.0.1:" + listener.getLocalPort()), null, Duration.ofSeconds(1));
            assertEquals(PeerIdentityStatus.INVALID,
                    new TailscaleLocalApiProvider("tailnet:example", client).resolve(context(null, null)).status());
            server.join();
        }
    }

    @Test void httpTransportRequiresOneExactJsonContentTypeOnSuccess() throws Exception {
        String body = "{\"Node\":{},\"UserProfile\":{\"ID\":7}}";
        assertEquals(PeerIdentityStatus.AVAILABLE, resolveHttpResponse(
                "Content-Type: application/json\r\n", body));
        assertEquals(PeerIdentityStatus.INVALID, resolveHttpResponse("", body));
        assertEquals(PeerIdentityStatus.INVALID, resolveHttpResponse(
                "Content-Type: text/plain\r\n", body));
        assertEquals(PeerIdentityStatus.INVALID, resolveHttpResponse(
                "Content-Type: application/json\r\ncontent-type: application/json\r\n", body));
    }

    @Test void httpTransportDecodesStrictBoundedChunkedResponses() throws Exception {
        byte[] body = "{\"Node\":{},\"UserProfile\":{\"ID\":7}}".getBytes(StandardCharsets.UTF_8);
        String valid = "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n"
                + "Content-Type: application/json\r\nConnection: close\r\n\r\n"
                + Integer.toHexString(body.length) + "\r\n" + new String(body, StandardCharsets.UTF_8)
                + "\r\n0\r\n\r\n";
        assertEquals(PeerIdentityStatus.AVAILABLE, resolveRawHttpResponse(valid));
        assertEquals(PeerIdentityStatus.INVALID, resolveRawHttpResponse(
                "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n"
                        + "Content-Type: application/json\r\nConnection: close\r\n\r\nzz\r\n{}\r\n0\r\n\r\n"));
        assertEquals(PeerIdentityStatus.INVALID, resolveRawHttpResponse(
                "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nContent-Length: 2\r\n"
                        + "Content-Type: application/json\r\nConnection: close\r\n\r\n0\r\n\r\n"));
        assertEquals(PeerIdentityStatus.INVALID, resolveRawHttpResponse(
                "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n"
                        + "Content-Type: application/json\r\nConnection: close\r\n\r\n10001\r\n"));
    }

    @Test void httpAndUnixSlowDripsCannotExtendTheMonotonicBudget() throws Exception {
        try (ServerSocket listener = new ServerSocket(0)) {
            Thread server = Thread.ofVirtual().start(() -> {
                try (var socket = listener.accept()) {
                    readHeaders(socket.getInputStream());
                    for (byte value : "HTTP/1.1 200 OK\r\n".getBytes(StandardCharsets.US_ASCII)) {
                        socket.getOutputStream().write(value);
                        socket.getOutputStream().flush();
                        Thread.sleep(15);
                    }
                } catch (Exception ignored) {}
            });
            var client = new TailscaleLocalApiProvider.HttpLocalApiClient(
                    URI.create("http://127.0.0.1:" + listener.getLocalPort()), null, Duration.ofSeconds(1));
            assertEquals(PeerIdentityStatus.UNAVAILABLE, new TailscaleLocalApiProvider("tailnet:example", client)
                    .resolve(contextWithDeadline(Instant.now().plusMillis(45))).status());
            server.join();
        }

        Path socketPath = Files.createTempFile("vgi-localapi-stall-", ".sock");
        Files.delete(socketPath);
        try (ServerSocketChannel listener = ServerSocketChannel.open(StandardProtocolFamily.UNIX)) {
            listener.bind(UnixDomainSocketAddress.of(socketPath));
            Thread server = Thread.ofVirtual().start(() -> {
                try (var ignored = listener.accept()) { Thread.sleep(250); }
                catch (Exception ignored) {}
            });
            var client = new TailscaleLocalApiProvider.UnixLocalApiClient(
                    socketPath, null, Duration.ofSeconds(1));
            assertEquals(PeerIdentityStatus.UNAVAILABLE, new TailscaleLocalApiProvider("tailnet:example", client)
                    .resolve(contextWithDeadline(Instant.now().plusMillis(45))).status());
            server.join();
        } finally {
            Files.deleteIfExists(socketPath);
        }
    }

    @Test void rejectsAggregateJsonValueExplosion() {
        String values = "0,".repeat(4_100) + "0";
        var provider = new TailscaleLocalApiProvider("tailnet:example", ignored -> json(200,
                "{\"Node\":{},\"UserProfile\":{\"ID\":1},\"CapMap\":{\"cap\":[" + values + "]}}"));
        assertEquals(PeerIdentityStatus.INVALID, provider.resolve(context(null, null)).status());
    }

    private static PeerIdentityStatus status(int code) {
        return new TailscaleLocalApiProvider("tailnet:example", ignored -> json(code, "{}"))
                .resolve(context(null, null)).status();
    }

    private static TailscaleLocalApiProvider.LocalApiResponse json(int status, String body) {
        return new TailscaleLocalApiProvider.LocalApiResponse(status, body.getBytes(StandardCharsets.UTF_8));
    }

    private static PeerResolutionContext context(String service, String asserted) {
        return new PeerResolutionContext("tcp", "127.0.0.1", "127.0.0.1:5000", asserted,
                "100.100.100.100:9400",
                null, service, Map.of(), Map.of(), Instant.now().plusSeconds(2));
    }

    private static PeerResolutionContext contextWithDeadline(Instant deadline) {
        return new PeerResolutionContext("tcp", "127.0.0.1", "127.0.0.1:5000", null,
                "100.100.100.100:9400", null, null, Map.of(), Map.of(), deadline);
    }

    private static void readHeaders(java.io.InputStream input) throws Exception {
        int state = 0;
        while (state < 4) {
            int value = input.read();
            if (value < 0) throw new java.io.EOFException();
            state = switch (state) {
                case 0 -> value == '\r' ? 1 : 0;
                case 1 -> value == '\n' ? 2 : 0;
                case 2 -> value == '\r' ? 3 : 0;
                default -> value == '\n' ? 4 : 0;
            };
        }
    }

    private static PeerIdentityStatus resolveHttpResponse(String contentTypeHeaders, String body)
            throws Exception {
        try (ServerSocket listener = new ServerSocket(0)) {
            Thread server = Thread.ofVirtual().start(() -> {
                try (var socket = listener.accept()) {
                    readHeaders(socket.getInputStream());
                    byte[] encodedBody = body.getBytes(StandardCharsets.UTF_8);
                    socket.getOutputStream().write(("HTTP/1.1 200 OK\r\nContent-Length: "
                            + encodedBody.length + "\r\n" + contentTypeHeaders
                            + "Connection: close\r\n\r\n").getBytes(StandardCharsets.US_ASCII));
                    socket.getOutputStream().write(encodedBody);
                } catch (Exception ignored) {}
            });
            var client = new TailscaleLocalApiProvider.HttpLocalApiClient(
                    URI.create("http://127.0.0.1:" + listener.getLocalPort()), null, Duration.ofSeconds(1));
            PeerIdentityStatus status = new TailscaleLocalApiProvider("tailnet:example", client)
                    .resolve(context(null, null)).status();
            server.join();
            return status;
        }
    }

    private static PeerIdentityStatus resolveRawHttpResponse(String response) throws Exception {
        try (ServerSocket listener = new ServerSocket(0)) {
            Thread server = Thread.ofVirtual().start(() -> {
                try (var socket = listener.accept()) {
                    readHeaders(socket.getInputStream());
                    socket.getOutputStream().write(response.getBytes(StandardCharsets.UTF_8));
                } catch (Exception ignored) {}
            });
            var client = new TailscaleLocalApiProvider.HttpLocalApiClient(
                    URI.create("http://127.0.0.1:" + listener.getLocalPort()), null, Duration.ofSeconds(1));
            PeerIdentityStatus status = new TailscaleLocalApiProvider("tailnet:example", client)
                    .resolve(context(null, null)).status();
            server.join();
            return status;
        }
    }
}
