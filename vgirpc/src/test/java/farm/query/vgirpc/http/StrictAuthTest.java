// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import farm.query.vgirpc.AuthContext;
import farm.query.vgirpc.RpcServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link HttpServer.Config#strictAuth()} and {@link HttpServer.Config#wwwAuthenticate()}: every
 * request but {@code HEAD /health} and CORS preflights needs a credential, the configured
 * challenge rides every 401, and authentication runs before the body is read.
 */
final class StrictAuthTest {

    private static final String CHALLENGE = "Bearer error=\"invalid_token\"";
    private static final String GOOD = "good-token";

    public interface EchoService {
        String echo(String value);
    }

    public static final class EchoImpl implements EchoService {
        @Override public String echo(String value) { return value; }
    }

    private HttpServer server;
    private String base;
    private final AtomicInteger authCalls = new AtomicInteger();

    @AfterEach
    void stop() throws Exception {
        if (server != null) server.stop();
    }

    private final Authenticator bearer = request -> {
        authCalls.incrementAndGet();
        String h = request.getHeader("Authorization");
        if (h == null) throw new MissingCredentials("no credential", "Bearer realm=\"ignored\"");
        if (!h.equals("Bearer " + GOOD)) throw new InvalidCredentials("bad credential");
        return new AuthContext("test", true, "alice", Map.of());
    };

    private void start(Authenticator auth) throws Exception {
        server = new HttpServer(new RpcServer(EchoService.class, new EchoImpl()),
                HttpServer.Config.builder()
                        .authenticator(auth)
                        .strictAuth(true)
                        .wwwAuthenticate(CHALLENGE)
                        .corsOrigins(List.of("*"))
                        .maxRequestBytes(1024)
                        .build());
        server.start();
        base = "http://127.0.0.1:" + server.port();
    }

    private HttpResponse<String> send(String method, String path, String token, Map<String, String> headers)
            throws Exception {
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()) {
            HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(base + path))
                    .timeout(Duration.ofSeconds(10))
                    .method(method, "POST".equals(method)
                            ? HttpRequest.BodyPublishers.ofByteArray(new byte[0])
                            : HttpRequest.BodyPublishers.noBody());
            if (token != null) b.header("Authorization", "Bearer " + token);
            headers.forEach(b::header);
            return client.send(b.build(), HttpResponse.BodyHandlers.ofString());
        }
    }

    private static void assert401(HttpResponse<String> r, String reason) {
        assertEquals(401, r.statusCode(), r.body());
        assertEquals(CHALLENGE, r.headers().firstValue("WWW-Authenticate").orElse(null));
        assertEquals(reason, r.headers().firstValue(HttpHeaders.VGI_AUTH_REASON).orElse(null));
        assertTrue(r.headers().firstValue(HttpServer.SUPPORTED_ENCODINGS_HEADER).isPresent(),
                "capability headers ride the 401");
    }

    @Test
    void head_health_is_the_only_anonymous_route() throws Exception {
        start(bearer);
        assertEquals(200, send("HEAD", "/health", null, Map.of()).statusCode());
        assertEquals(0, authCalls.get(), "HEAD /health never reaches the authenticator");
    }

    @Test
    void everything_else_needs_a_credential() throws Exception {
        start(bearer);
        for (String[] mp : new String[][]{
                {"GET", "/health"}, {"OPTIONS", "/health"}, {"GET", "/"}, {"GET", "/nowhere/at/all"},
                {"POST", "/EchoService/echo"}, {"POST", "/NotHosted/echo"}, {"DELETE", "/__session__"},
                {"PUT", "/health"}}) {
            HttpResponse<String> anonymous = send(mp[0], mp[1], null, Map.of("Origin", "https://app.example"));
            assert401(anonymous, "missing_credential");
            assertEquals("*", anonymous.headers().firstValue("Access-Control-Allow-Origin").orElse(null),
                    "a browser can read the 401");
            assert401(send(mp[0], mp[1], "wrong", Map.of()), "invalid_credential");
        }
    }

    @Test
    void a_valid_credential_reaches_the_routes() throws Exception {
        start(bearer);
        assertEquals(200, send("GET", "/health", GOOD, Map.of()).statusCode());
        HttpResponse<String> notHosted = send("POST", "/NotHosted/echo", GOOD, Map.of());
        assertEquals(404, notHosted.statusCode());
        int before = authCalls.get();
        send("POST", "/EchoService/echo", GOOD, Map.of());
        assertEquals(before + 1, authCalls.get(), "routes reuse the identity the gate resolved");
    }

    @Test
    void preflight_needs_no_credential() throws Exception {
        start(bearer);
        HttpResponse<String> r = send("OPTIONS", "/EchoService/echo", null, Map.of(
                "Origin", "https://app.example",
                "Access-Control-Request-Method", "POST",
                "Access-Control-Request-Headers", "authorization"));
        assertEquals(200, r.statusCode());
        assertEquals("*", r.headers().firstValue("Access-Control-Allow-Origin").orElse(null));
    }

    @Test
    void outage_is_503_not_401() throws Exception {
        start(request -> { throw new AuthUnavailableException("keys unavailable"); });
        HttpResponse<String> r = send("GET", "/health", GOOD, Map.of());
        assertEquals(503, r.statusCode());
        assertTrue(r.headers().firstValue("Retry-After").isPresent());
    }

    /**
     * An unauthenticated request with an oversized, never-sent body is refused at once: the gate
     * answers from the headers alone, so it is a 401, not a 413 and not a hang on the body.
     */
    @Test
    void refusal_precedes_the_body() throws Exception {
        start(bearer);
        try (Socket s = new Socket("127.0.0.1", server.port())) {
            s.setSoTimeout(10_000);
            OutputStream out = s.getOutputStream();
            out.write(("POST /EchoService/echo HTTP/1.1\r\nHost: x\r\nContent-Type: application/vnd.apache.arrow.stream\r\n"
                    + "Content-Encoding: zstd\r\nContent-Length: 100000000\r\n\r\n").getBytes(StandardCharsets.US_ASCII));
            out.flush();
            InputStream in = s.getInputStream();
            byte[] buf = new byte[12];
            int n = in.readNBytes(buf, 0, buf.length);
            String status = new String(buf, 0, n, StandardCharsets.US_ASCII);
            assertEquals("HTTP/1.1 401", status);
        }
    }

    @Test
    void config_refuses_strict_without_an_authenticator() {
        assertThrows(IllegalArgumentException.class,
                () -> HttpServer.Config.builder().strictAuth(true).build());
        assertThrows(IllegalArgumentException.class,
                () -> HttpServer.Config.builder().authenticator(Authenticator.ANONYMOUS).strictAuth(true).build());
        assertThrows(IllegalArgumentException.class,
                () -> HttpServer.Config.builder().wwwAuthenticate("a\r\nb").build());
    }

    @Test
    void non_strict_server_is_unchanged() throws Exception {
        server = new HttpServer(new RpcServer(EchoService.class, new EchoImpl()),
                HttpServer.Config.builder().authenticator(bearer).build());
        server.start();
        base = "http://127.0.0.1:" + server.port();
        assertEquals(200, send("GET", "/health", null, Map.of()).statusCode());
        HttpResponse<String> r = send("POST", "/EchoService/echo", null, Map.of());
        assertEquals(401, r.statusCode());
        assertNotEquals(CHALLENGE, r.headers().firstValue("WWW-Authenticate").orElse(""));
    }
}
