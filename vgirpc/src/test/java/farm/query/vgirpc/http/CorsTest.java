// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import farm.query.vgirpc.RpcServer;
import farm.query.vgirpc.http.auth.ProxyProof;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * CORS end to end, the Java-side mirror of the shared {@code TestCors} group.
 *
 * <p>The load-bearing case is {@link #every_advertised_capability_is_exposed}:
 * an advertised-but-unexposed capability header is invisible to a browser and
 * to nothing else, so every other test in this repo — driven by a client that
 * ignores CORS entirely — passes right through it.
 */
final class CorsTest {

    /** The origin the shared conformance suite preflights with. */
    private static final String ORIGIN = "https://conformance.example";

    public interface EchoService {
        String echo(String value);
    }

    public static final class EchoImpl implements EchoService {
        @Override public String echo(String value) { return value; }
    }

    private HttpServer server;
    private String base;

    @AfterEach
    void stop() throws Exception {
        if (server != null) server.stop();
    }

    private void start(Consumer<HttpServer.Config.Builder> configure) throws Exception {
        HttpServer.Config.Builder b = HttpServer.Config.builder().prefix("/vgi");
        configure.accept(b);
        server = new HttpServer(new RpcServer(EchoService.class, new EchoImpl()), b.build());
        server.start();
        base = "http://127.0.0.1:" + server.port() + "/vgi";
    }

    private void startWithCors() throws Exception {
        start(b -> b.corsOrigin(ORIGIN));
    }

    // ---- opt-in ----------------------------------------------------------

    /** Off by default: an unconfigured server grants nothing to any origin. */
    @Test
    void an_unconfigured_server_emits_no_cors_headers() throws Exception {
        start(b -> { });
        HttpResponse<Void> resp = preflight(base + "/echo", "content-type");
        assertEquals(200, resp.statusCode());
        assertFalse(resp.headers().firstValue(CorsPolicy.ALLOW_ORIGIN).isPresent(),
                "an unconfigured server must not grant cross-origin access");
        assertFalse(resp.headers().firstValue(CorsPolicy.EXPOSE_HEADERS).isPresent());
    }

    /** An origin outside the allowlist is refused by omission, not by status. */
    @Test
    void an_unlisted_origin_gets_no_grant() throws Exception {
        startWithCors();
        HttpResponse<Void> resp = options(base + "/echo", Map.of(
                CorsPolicy.ORIGIN, "https://evil.example",
                "Access-Control-Request-Method", "POST"));
        assertFalse(resp.headers().firstValue(CorsPolicy.ALLOW_ORIGIN).isPresent());
    }

    /** A request with no {@code Origin} is same-origin; CORS has nothing to say. */
    @Test
    void a_request_without_an_origin_gets_no_grant() throws Exception {
        startWithCors();
        assertFalse(options(base + "/health", Map.of())
                .headers().firstValue(CorsPolicy.ALLOW_ORIGIN).isPresent());
    }

    // ---- preflight -------------------------------------------------------

    /** The configured origin is echoed, POST is permitted, and the preflight caches. */
    @Test
    void the_preflight_grants_the_configured_origin() throws Exception {
        startWithCors();
        HttpResponse<Void> resp = preflight(base + "/echo", "content-type");
        assertEquals(200, resp.statusCode());
        assertEquals(ORIGIN, header(resp, CorsPolicy.ALLOW_ORIGIN));
        assertEquals(CorsPolicy.ORIGIN, header(resp, "Vary"),
                "a per-origin answer must not be cached across origins");
        assertTrue(header(resp, CorsPolicy.ALLOW_METHODS).contains("POST"),
                "every RPC call is a POST; refusing it blocks all of them");
        assertEquals(Long.toString(HttpServer.Config.DEFAULT_CORS_MAX_AGE_SECONDS),
                header(resp, CorsPolicy.MAX_AGE));
    }

    /**
     * The request half of CORS: a browser sends only the headers the preflight
     * named. Dropping one is invisible on a plain call and takes out whichever
     * feature rode on it — sticky sessions, proxy proof, codec preference.
     */
    @Test
    void the_preflight_permits_every_request_header_a_client_sends() throws Exception {
        startWithCors();
        for (String header : List.of("content-type", HttpHeaders.X_VGI_ACCEPT_ENCODING,
                StickyHeaders.SESSION, StickyHeaders.SESSION_ACCEPT, ProxyProof.PROOF_HEADER)) {
            Set<String> allowed = split(header(preflight(base + "/echo", header),
                    CorsPolicy.ALLOW_HEADERS));
            assertTrue(allowed.contains(header.toLowerCase(Locale.ROOT)),
                    "a browser may not send " + header + "; allowed: " + allowed);
        }
    }

    /** A preflight naming no headers still answers with the request-side surface. */
    @Test
    void a_preflight_without_requested_headers_falls_back_to_the_default_set() throws Exception {
        startWithCors();
        HttpResponse<Void> resp = options(base + "/echo", Map.of(
                CorsPolicy.ORIGIN, ORIGIN,
                "Access-Control-Request-Method", "POST"));
        Set<String> allowed = split(header(resp, CorsPolicy.ALLOW_HEADERS));
        assertTrue(allowed.contains("content-type"));
        assertTrue(allowed.contains(StickyHeaders.SESSION.toLowerCase(Locale.ROOT)));
    }

    /** {@code Access-Control-Max-Age: 0} means "omit", not "do not cache". */
    @Test
    void a_zero_max_age_omits_the_header() throws Exception {
        start(b -> b.corsOrigin(ORIGIN).corsMaxAgeSeconds(0));
        assertFalse(preflight(base + "/echo", "content-type")
                .headers().firstValue(CorsPolicy.MAX_AGE).isPresent());
    }

    /** A wildcard answers every origin with the literal {@code "*"}. */
    @Test
    void a_wildcard_origin_grants_everyone() throws Exception {
        start(b -> b.corsOrigin(CorsPolicy.WILDCARD));
        HttpResponse<Void> resp = options(base + "/echo", Map.of(
                CorsPolicy.ORIGIN, "https://anything.example",
                "Access-Control-Request-Method", "POST"));
        assertEquals(CorsPolicy.WILDCARD, header(resp, CorsPolicy.ALLOW_ORIGIN));
        assertFalse(resp.headers().firstValue("Vary").isPresent(),
                "a wildcard answer is origin-independent, so nothing varies on it");
    }

    // ---- actual responses ------------------------------------------------

    /**
     * The grant has to ride the real response too: a browser re-checks it there
     * and discards the body without it, so a preflight-only implementation
     * fails every call while passing a naive preflight test.
     */
    @Test
    void an_actual_response_carries_the_grant() throws Exception {
        startWithCors();
        try (HttpClient client = newClient()) {
            HttpResponse<String> resp = client.send(
                    HttpRequest.newBuilder(URI.create(base + "/health"))
                            .header(CorsPolicy.ORIGIN, ORIGIN)
                            .timeout(Duration.ofSeconds(10)).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(200, resp.statusCode());
            assertEquals(ORIGIN, resp.headers().firstValue(CorsPolicy.ALLOW_ORIGIN).orElseThrow());
            assertTrue(split(resp.headers().firstValue(CorsPolicy.EXPOSE_HEADERS).orElseThrow())
                    .contains(HttpServer.SUPPORTED_ENCODINGS_HEADER.toLowerCase(Locale.ROOT)));
        }
    }

    // ---- the expose list -------------------------------------------------

    /**
     * Whatever this server advertises, it exposes — checked against what it
     * actually puts on the wire rather than a copy of the list, so a new
     * capability header added to {@code applyCapabilityHeaders} without an
     * expose entry fails here instead of silently shipping.
     */
    @Test
    void every_advertised_capability_is_exposed() throws Exception {
        start(b -> b.corsOrigin(ORIGIN)
                .advertiseMaxRequestBytes(true)
                .advertisedMaxResponseBytes(1 << 20)
                .advertisedMaxExternalizedResponseBytes(1 << 20)
                .proxyProofRequired(true)
                .stickyEnabled(true)
                .stickyEchoHeaders(Map.of("x-echo-marker", "value")));
        HttpResponse<Void> resp = preflight(base + "/health", "content-type");
        Set<String> exposed = split(header(resp, CorsPolicy.EXPOSE_HEADERS));

        Set<String> advertised = resp.headers().map().keySet().stream()
                .map(n -> n.toLowerCase(Locale.ROOT))
                .filter(n -> n.startsWith("vgi-") || n.startsWith("x-vgi-"))
                .collect(Collectors.toCollection(LinkedHashSet::new));
        assertFalse(advertised.isEmpty(), "no capability headers to check against");

        advertised.removeAll(exposed);
        assertTrue(advertised.isEmpty(),
                "advertised but not readable by a browser: " + advertised
                        + " — add them to " + CorsPolicy.EXPOSE_HEADERS);
    }

    /** The error flag rides responses, never {@code /health}, so it needs its own check. */
    @Test
    void the_error_flag_is_exposed() throws Exception {
        startWithCors();
        Set<String> exposed = split(header(preflight(base + "/echo", "content-type"),
                CorsPolicy.EXPOSE_HEADERS));
        assertTrue(exposed.contains(HttpServer.RPC_ERROR_HEADER.toLowerCase(Locale.ROOT)),
                "without it a browser cannot tell an error 200 from a result 200");
    }

    /** Same for the 401 reason code, which describes a rejection rather than a capability. */
    @Test
    void the_auth_reason_is_exposed() throws Exception {
        startWithCors();
        Set<String> exposed = split(header(preflight(base + "/echo", "content-type"),
                CorsPolicy.EXPOSE_HEADERS));
        assertTrue(exposed.contains(HttpHeaders.VGI_AUTH_REASON.toLowerCase(Locale.ROOT)));
    }

    /** Conditional headers stay off the list when the server never emits them. */
    @Test
    void unemitted_headers_are_not_exposed() throws Exception {
        startWithCors();  // no sticky, no proof, no upload URLs
        Set<String> exposed = split(header(preflight(base + "/echo", "content-type"),
                CorsPolicy.EXPOSE_HEADERS));
        assertFalse(exposed.contains(StickyHeaders.SESSION.toLowerCase(Locale.ROOT)));
        assertFalse(exposed.contains(ProxyProof.PROOF_REQUIRED_HEADER.toLowerCase(Locale.ROOT)));
        assertFalse(exposed.contains(HttpServer.UPLOAD_URL_HEADER.toLowerCase(Locale.ROOT)));
    }

    // ---- configuration ---------------------------------------------------

    /** An all-blank origin list is a typo, not a policy — fail rather than allow nothing. */
    @Test
    void a_blank_origin_list_is_rejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new CorsPolicy(List.of("  "), 7200, List.of()));
    }

    @Test
    void a_negative_max_age_is_rejected() {
        assertThrows(IllegalArgumentException.class,
                () -> HttpServer.Config.builder().corsOrigin(ORIGIN).corsMaxAgeSeconds(-1).build());
    }

    // ---- helpers ---------------------------------------------------------

    private static HttpClient newClient() {
        return HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    }

    private static HttpResponse<Void> preflight(String url, String requestHeaders) throws Exception {
        return options(url, Map.of(
                CorsPolicy.ORIGIN, ORIGIN,
                "Access-Control-Request-Method", "POST",
                CorsPolicy.REQUEST_HEADERS, requestHeaders));
    }

    private static HttpResponse<Void> options(String url, Map<String, String> headers) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(url))
                .method("OPTIONS", HttpRequest.BodyPublishers.noBody())
                .timeout(Duration.ofSeconds(10));
        headers.forEach(b::header);
        try (HttpClient client = newClient()) {
            return client.send(b.build(), HttpResponse.BodyHandlers.discarding());
        }
    }

    private static String header(HttpResponse<?> resp, String name) {
        return resp.headers().firstValue(name).orElseThrow(
                () -> new AssertionError("missing response header: " + name));
    }

    /** Split a comma-separated header value into a lowercase name set. */
    private static Set<String> split(String value) {
        return Arrays.stream(value.split(","))
                .map(s -> s.trim().toLowerCase(Locale.ROOT))
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }
}
