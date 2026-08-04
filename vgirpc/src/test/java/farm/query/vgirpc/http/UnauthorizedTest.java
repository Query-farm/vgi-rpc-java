// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import farm.query.vgirpc.RpcServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The standardized 401 of {@code docs/unauthorized-spec.md}, end to end.
 *
 * <p>The cross-language {@code TestUnauthorized} group covers the wire shape
 * against a spawned worker; these cover the two things it structurally cannot
 * reach — the reason a Java exception <em>type</em> maps onto, and the fact
 * that the proxy note is a function of server configuration rather than of the
 * request that happened to be refused.</p>
 */
final class UnauthorizedTest {

    private static final ObjectMapper JSON = new ObjectMapper();

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

    private void start(Authenticator auth, List<String> proxyAuthHeaders) throws Exception {
        server = new HttpServer(new RpcServer(EchoService.class, new EchoImpl()),
                HttpServer.Config.builder()
                        .prefix("/vgi")
                        .authenticator(auth)
                        .proxyAuthHeaders(proxyAuthHeaders)
                        .build());
        server.start();
        base = "http://127.0.0.1:" + server.port() + "/vgi";
    }

    /** POST an empty body at a gated endpoint; auth runs before the body is parsed. */
    private HttpResponse<String> post(String accept) throws Exception {
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()) {
            HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(base + "/echo"))
                    .timeout(Duration.ofSeconds(10))
                    .POST(HttpRequest.BodyPublishers.ofByteArray(new byte[0]));
            if (accept != null) b.header("Accept", accept);
            return client.send(b.build(), HttpResponse.BodyHandlers.ofString());
        }
    }

    // ---- reason classification -------------------------------------------

    /**
     * The exception subtype is the classification. A thrower picking
     * {@link MissingCredentials} has already said nothing was presented, so
     * reading it off the type is a declaration — unlike inspecting the
     * message, which misclassifies the moment someone rewords a string.
     */
    @Test
    void exception_types_carry_their_own_reason() {
        assertEquals(AuthReason.MISSING_CREDENTIAL, new MissingCredentials("no header").reason());
        assertEquals(AuthReason.INVALID_CREDENTIAL, new InvalidCredentials("bad token").reason());
        assertEquals(AuthReason.EXPIRED_CREDENTIAL,
                new AuthFailure(AuthReason.EXPIRED_CREDENTIAL, "stale").reason());
    }

    /** A failure that names no reason lands on the fallback rather than a guess. */
    @Test
    void unnamed_failure_is_unauthorized() {
        assertEquals(AuthReason.UNAUTHORIZED, new AuthFailure("nope").reason());
        assertEquals(AuthReason.UNAUTHORIZED, new AuthFailure(null, "nope").reason());
    }

    // ---- envelope ---------------------------------------------------------

    /** Absent, not empty, when it does not apply — presence alone is the signal. */
    @Test
    void envelope_omits_the_hint_when_it_does_not_apply() {
        Map<String, String> body = Unauthorized.envelope(AuthReason.INVALID_CREDENTIAL, "nope", "");
        assertFalse(body.containsKey("proxy_hint"));
        assertEquals("unauthorized", body.get("error"));
        assertEquals("invalid_credential", body.get("reason"));
        assertEquals("nope", body.get("detail"));
    }

    /** The note has to name the headers an operator must check. */
    @Test
    void proxy_hint_names_the_headers() {
        String hint = Unauthorized.proxyHint(List.of("VGI-Proxy-Proof", "x-forwarded-client-cert"));
        assertTrue(hint.contains("VGI-Proxy-Proof"), hint);
        assertTrue(hint.contains("x-forwarded-client-cert"), hint);
        assertEquals("", Unauthorized.proxyHint(List.of()));
    }

    // ---- wire shape -------------------------------------------------------

    /** Header, body, and cache directive on a service with no proxy dependency. */
    @Test
    void rejection_carries_the_reason_header_and_json_envelope() throws Exception {
        start(request -> { throw new AuthFailure(AuthReason.INSUFFICIENT_SCOPE, "not for you"); }, List.of());
        HttpResponse<String> resp = post("*/*");

        assertEquals(401, resp.statusCode());
        assertEquals("insufficient_scope",
                resp.headers().firstValue(HttpHeaders.VGI_AUTH_REASON).orElseThrow());
        assertTrue(resp.headers().firstValue("Cache-Control").orElseThrow().contains("no-store"));
        assertTrue(resp.headers().firstValue("Content-Type").orElseThrow().startsWith("application/json"));

        JsonNode body = JSON.readTree(resp.body());
        assertEquals("unauthorized", body.get("error").asText());
        // Header and body must agree, or a client reading one and logging the
        // other reports two different stories about the same rejection.
        assertEquals("insufficient_scope", body.get("reason").asText());
        assertEquals("not for you", body.get("detail").asText());
        assertFalse(body.has("proxy_hint"));
        assertTrue(resp.headers().firstValue(HttpHeaders.VGI_AUTH_PROXY_REQUIRED).isEmpty());
    }

    /** §4.2 permits always answering JSON; it forbids answering JSON requests with HTML. */
    @Test
    void a_browser_request_still_gets_the_reason_header() throws Exception {
        start(request -> { throw new AuthFailure("nope"); }, List.of());
        HttpResponse<String> resp = post("text/html,application/xhtml+xml");
        assertEquals(401, resp.statusCode());
        assertEquals("unauthorized",
                resp.headers().firstValue(HttpHeaders.VGI_AUTH_REASON).orElseThrow());
    }

    /**
     * The note is derived from configuration, so it rides every 401 the server
     * produces — including ones whose reason is not {@code proxy_required}.
     * That is what lets it coexist with the uniform-rejection rule: it
     * discloses nothing about which stage refused this attempt.
     */
    @Test
    void a_proxy_dependent_service_notes_it_on_every_rejection() throws Exception {
        start(request -> { throw new InvalidCredentials("bad token"); },
                List.of("x-forwarded-client-cert"));
        HttpResponse<String> resp = post("*/*");

        assertEquals(401, resp.statusCode());
        assertEquals("invalid_credential",
                resp.headers().firstValue(HttpHeaders.VGI_AUTH_REASON).orElseThrow());
        assertEquals("true",
                resp.headers().firstValue(HttpHeaders.VGI_AUTH_PROXY_REQUIRED).orElseThrow());
        String hint = JSON.readTree(resp.body()).get("proxy_hint").asText();
        assertTrue(hint.contains("x-forwarded-client-cert"), hint);
    }

    /** Neither header is a capability advertisement, so success must be quiet. */
    @Test
    void a_successful_response_carries_neither_header() throws Exception {
        start(Authenticator.ANONYMOUS, List.of("x-forwarded-client-cert"));
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()) {
            HttpResponse<String> resp = client.send(
                    HttpRequest.newBuilder(URI.create(base + "/health"))
                            .timeout(Duration.ofSeconds(10)).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(200, resp.statusCode());
            assertTrue(resp.headers().firstValue(HttpHeaders.VGI_AUTH_REASON).isEmpty());
            assertTrue(resp.headers().firstValue(HttpHeaders.VGI_AUTH_PROXY_REQUIRED).isEmpty());
        }
    }
}
