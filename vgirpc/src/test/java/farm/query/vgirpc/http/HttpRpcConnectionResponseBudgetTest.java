// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import com.sun.net.httpserver.HttpExchange;
import farm.query.vgirpc.RpcError;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class HttpRpcConnectionResponseBudgetTest {
    private com.sun.net.httpserver.HttpServer server;

    @AfterEach
    void stop() {
        if (server != null) server.stop(0);
    }

    @Test
    void chunked_response_is_bounded_while_streaming() throws Exception {
        AtomicInteger posts = new AtomicInteger();
        String endpoint = start((exchange) -> {
            if ("OPTIONS".equals(exchange.getRequestMethod())) {
                assertEquals(Long.toString(64L << 10), exchange.getRequestHeaders()
                        .getFirst(HttpServer.ACCEPT_MAX_RESPONSE_BYTES_HEADER));
                support(exchange, "true");
                exchange.sendResponseHeaders(204, -1);
                exchange.close();
                return;
            }
            posts.incrementAndGet();
            support(exchange, "true");
            exchange.getResponseHeaders().set("Content-Type", HttpServer.ARROW_CONTENT_TYPE);
            exchange.sendResponseHeaders(200, 0); // chunked
            byte[] chunk = new byte[8192];
            for (int i = 0; i < 9; i++) exchange.getResponseBody().write(chunk);
            exchange.close();
        });

        try (HttpRpcConnection connection = HttpRpcConnection.builder(endpoint)
                .acceptedMaxResponseBytes(64L << 10).build()) {
            RpcError error = assertThrows(RpcError.class,
                    () -> connection.post(endpoint + "/echo", new byte[0], "echo"));
            assertEquals("ResponseTooLargeError", error.errorType());
            assertTrue(error.errorMessage().contains("max_response_bytes"));
        }
        assertEquals(1, posts.get());
    }

    @Test
    void discovery_and_each_rpc_response_require_one_literal_true() throws Exception {
        for (String discoveryValue : new String[]{"TRUE", "duplicate"}) {
            AtomicInteger posts = new AtomicInteger();
            String endpoint = start(exchange -> {
                if ("OPTIONS".equals(exchange.getRequestMethod())) {
                    if ("duplicate".equals(discoveryValue)) {
                        support(exchange, "true");
                        support(exchange, "true");
                    } else {
                        support(exchange, discoveryValue);
                    }
                    exchange.sendResponseHeaders(200, -1);
                } else {
                    posts.incrementAndGet();
                    arrow(exchange, "true");
                }
                exchange.close();
            });
            try (HttpRpcConnection connection = HttpRpcConnection.builder(endpoint).build()) {
                RpcError error = assertThrows(RpcError.class,
                        () -> connection.post(endpoint + "/echo", new byte[0], "echo"));
                assertEquals("ProtocolError", error.errorType());
            }
            assertEquals(0, posts.get());
            server.stop(0);
            server = null;
        }

        for (String rpcValue : new String[]{null, "TRUE", "duplicate"}) {
            String endpoint = start(exchange -> {
                if ("OPTIONS".equals(exchange.getRequestMethod())) {
                    support(exchange, "true");
                    exchange.sendResponseHeaders(200, -1);
                } else {
                    if ("duplicate".equals(rpcValue)) {
                        support(exchange, "true");
                        support(exchange, "true");
                        arrow(exchange, null);
                    } else {
                        arrow(exchange, rpcValue);
                    }
                }
                exchange.close();
            });
            try (HttpRpcConnection connection = HttpRpcConnection.builder(endpoint).build()) {
                RpcError error = assertThrows(RpcError.class,
                        () -> connection.post(endpoint + "/echo", new byte[0], "echo"));
                assertEquals("ProtocolError", error.errorType());
            }
            server.stop(0);
            server = null;
        }
    }

    @Test
    void advertised_response_cap_narrows_the_local_decoded_limit() throws Exception {
        String endpoint = start(exchange -> {
            if ("OPTIONS".equals(exchange.getRequestMethod())) {
                support(exchange, "true");
                exchange.getResponseHeaders().set(HttpServer.MAX_RESPONSE_BYTES_HEADER, "65536");
                exchange.sendResponseHeaders(204, -1);
            } else {
                support(exchange, "true");
                exchange.getResponseHeaders().set("Content-Type", HttpServer.ARROW_CONTENT_TYPE);
                byte[] body = new byte[72 * 1024];
                exchange.sendResponseHeaders(200, body.length);
                exchange.getResponseBody().write(body);
            }
            exchange.close();
        });

        try (HttpRpcConnection connection = HttpRpcConnection.builder(endpoint)
                .acceptedMaxResponseBytes(128L << 10).build()) {
            RpcError error = assertThrows(RpcError.class,
                    () -> connection.post(endpoint + "/echo", new byte[0], "echo"));
            assertEquals("ResponseTooLargeError", error.errorType());
            assertTrue(error.errorMessage().contains("> 65536"), error.errorMessage());
        }
    }

    @Test
    void current_response_cap_also_narrows_the_local_decoded_limit() throws Exception {
        String endpoint = start(exchange -> {
            if ("OPTIONS".equals(exchange.getRequestMethod())) {
                support(exchange, "true");
                exchange.sendResponseHeaders(204, -1);
            } else {
                support(exchange, "true");
                exchange.getResponseHeaders().set(HttpServer.MAX_RESPONSE_BYTES_HEADER, "65536");
                exchange.getResponseHeaders().set("Content-Type", HttpServer.ARROW_CONTENT_TYPE);
                byte[] body = new byte[72 * 1024];
                exchange.sendResponseHeaders(200, body.length);
                exchange.getResponseBody().write(body);
            }
            exchange.close();
        });

        try (HttpRpcConnection connection = HttpRpcConnection.builder(endpoint)
                .acceptedMaxResponseBytes(128L << 10).build()) {
            RpcError error = assertThrows(RpcError.class,
                    () -> connection.post(endpoint + "/echo", new byte[0], "echo"));
            assertEquals("ResponseTooLargeError", error.errorType());
            assertTrue(error.errorMessage().contains("> 65536"), error.errorMessage());
        }
    }

    @Test
    void discovery_rejects_malformed_or_duplicate_advertised_response_cap() throws Exception {
        for (String advertised : new String[]{"invalid", "65535", "duplicate"}) {
            AtomicInteger posts = new AtomicInteger();
            String endpoint = start(exchange -> {
                if ("OPTIONS".equals(exchange.getRequestMethod())) {
                    support(exchange, "true");
                    if ("duplicate".equals(advertised)) {
                        exchange.getResponseHeaders().add(HttpServer.MAX_RESPONSE_BYTES_HEADER, "65536");
                        exchange.getResponseHeaders().add(HttpServer.MAX_RESPONSE_BYTES_HEADER, "65537");
                    } else {
                        exchange.getResponseHeaders().set(
                                HttpServer.MAX_RESPONSE_BYTES_HEADER, advertised);
                    }
                    exchange.sendResponseHeaders(200, -1);
                } else {
                    posts.incrementAndGet();
                    arrow(exchange, "true");
                }
                exchange.close();
            });
            try (HttpRpcConnection connection = HttpRpcConnection.builder(endpoint).build()) {
                RpcError error = assertThrows(RpcError.class,
                        () -> connection.post(endpoint + "/echo", new byte[0], "echo"));
                assertEquals("ProtocolError", error.errorType());
                assertTrue(error.errorMessage().contains(HttpServer.MAX_RESPONSE_BYTES_HEADER));
            }
            assertEquals(0, posts.get());
            server.stop(0);
            server = null;
        }
    }

    private String start(com.sun.net.httpserver.HttpHandler handler) throws IOException {
        server = com.sun.net.httpserver.HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", handler);
        server.start();
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    private static void support(HttpExchange exchange, String value) {
        exchange.getResponseHeaders().add(
                HttpServer.ACCEPT_MAX_RESPONSE_BYTES_SUPPORT_HEADER, value);
    }

    private static void arrow(HttpExchange exchange, String supportValue) throws IOException {
        if (supportValue != null) support(exchange, supportValue);
        exchange.getResponseHeaders().set("Content-Type", HttpServer.ARROW_CONTENT_TYPE);
        byte[] body = "not parsed because support validation comes first"
                .getBytes(StandardCharsets.US_ASCII);
        exchange.sendResponseHeaders(200, body.length);
        exchange.getResponseBody().write(body);
    }
}
