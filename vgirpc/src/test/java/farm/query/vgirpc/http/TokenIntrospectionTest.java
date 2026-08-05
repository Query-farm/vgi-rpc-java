// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import farm.query.vgirpc.AuthContext;
import farm.query.vgirpc.RpcServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The token-introspection guards the cross-language conformance group cannot
 * observe from outside: what reaches the log, what reaches the resolver, and
 * what a misconfiguration does at construction.
 *
 * <p>The wire-visible guards — the allowlist, the JWS refusal, uniform
 * rejections, the closed response key set — are pinned by
 * {@code TestTokenIntrospection} in the shared suite. These are the ones a
 * black-box HTTP client is blind to.
 */
final class TokenIntrospectionTest {

    public interface EchoService { String echo(String value); }

    public static final class EchoImpl implements EchoService {
        @Override public String echo(String value) { return value; }
    }

    private static final String PRINCIPAL_HEADER = "X-Test-Principal";
    private static final String INTROSPECTOR = "proxy@example";
    /** Distinctive enough that a substring search for it in a log is meaningful. */
    private static final String SUBJECT_TOKEN = "opaque-subject-credential-9f3a2b";
    private static final String UNKNOWN_TOKEN = "unknown-credential-7c1d4e";
    private static final String SUBJECT_PRINCIPAL = "alice@example";

    private HttpServer server;
    private String base;

    @AfterEach
    void stop() throws Exception {
        if (server != null) server.stop();
    }

    /** Boot a server whose introspector allowlist is exactly {@link #INTROSPECTOR}. */
    private void start(TokenResolver resolver) throws Exception {
        start(HttpServer.Config.builder()
                .prefix("/vgi")
                .authenticator(principalHeaderAuthenticator())
                .tokenIntrospection(resolver, List.of(INTROSPECTOR)));
    }

    private void start(HttpServer.Config.Builder builder) throws Exception {
        server = new HttpServer(new RpcServer(EchoService.class, new EchoImpl()), builder.build());
        server.start();
        base = "http://127.0.0.1:" + server.port() + "/vgi";
    }

    /** Resolves only {@link #SUBJECT_TOKEN}; everything else does not resolve. */
    private static TokenResolver fixedResolver() {
        return token -> SUBJECT_TOKEN.equals(token)
                ? Optional.of(new TokenIdentity(SUBJECT_PRINCIPAL, "laptop", 120))
                : Optional.empty();
    }

    private static Authenticator principalHeaderAuthenticator() {
        return req -> {
            String principal = req.getHeader(PRINCIPAL_HEADER);
            return principal == null || principal.isEmpty()
                    ? AuthContext.ANONYMOUS
                    : new AuthContext("test", true, principal, Map.of());
        };
    }

    // ---- guard 6: the credential is digested, never logged ----------------

    /**
     * The credential must appear in no log record — on the success path or the
     * rejection path — while the digest must, or the endpoint is undiagnosable.
     *
     * <p>Asserted against captured output rather than by reading the source
     * because the failure mode is a well-meant {@code LOG.debug("token={}")}
     * added later, which no wire-level test can see.
     */
    @Test
    void the_credential_never_reaches_the_log_but_its_digest_does() throws Exception {
        start(fixedResolver());
        PrintStream realErr = System.err;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        String logged;
        try {
            System.setErr(new PrintStream(captured, true, StandardCharsets.UTF_8));
            assertEquals(200, introspect(INTROSPECTOR, SUBJECT_TOKEN).statusCode());
            assertEquals(404, introspect(INTROSPECTOR, UNKNOWN_TOKEN).statusCode());
            assertEquals(403, introspect("someone-else", SUBJECT_TOKEN).statusCode());
            System.err.flush();
        } finally {
            logged = captured.toString(StandardCharsets.UTF_8);
            System.setErr(realErr);
        }

        // Without this the whole test passes vacuously on a silent logger.
        assertTrue(logged.contains(TokenIntrospection.digest(SUBJECT_TOKEN)),
                "the digest must be logged, or a credential's failures cannot be correlated: " + logged);
        assertFalse(logged.contains(SUBJECT_TOKEN), "the subject credential reached the log");
        assertFalse(logged.contains(UNKNOWN_TOKEN), "a rejected credential reached the log");
    }

    /** Nor may either path echo the credential back to the caller. */
    @Test
    void the_credential_never_reaches_the_response() throws Exception {
        start(fixedResolver());
        assertFalse(body(introspect(INTROSPECTOR, SUBJECT_TOKEN)).contains(SUBJECT_TOKEN));
        assertFalse(body(introspect(INTROSPECTOR, UNKNOWN_TOKEN)).contains(UNKNOWN_TOKEN));
    }

    // ---- guard 4: JWS-shaped subjects never reach the resolver -------------

    /**
     * The conformance group can only see the rejection; that it happened
     * <em>before</em> the resolver ran is the actual requirement.
     *
     * <p>Routing a JWS onward hands a third party a bearer token the asker may
     * itself have rejected, so a shape guard that merely reorders the answer
     * after a resolver call has already leaked it.
     */
    @Test
    void a_jws_shaped_subject_is_refused_without_reaching_the_resolver() throws Exception {
        AtomicBoolean consulted = new AtomicBoolean();
        start(token -> {
            consulted.set(true);
            return Optional.of(new TokenIdentity(SUBJECT_PRINCIPAL, "laptop", 120));
        });
        HttpResponse<String> resp = introspect(INTROSPECTOR, "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhIn0.c2ln");
        assertEquals(404, resp.statusCode());
        assertFalse(consulted.get(), "the resolver saw a JWS it should never have been handed");
    }

    // ---- definitive vs transient ------------------------------------------

    /**
     * The type-level half of the distinction: an outage must not be catchable as
     * a rejection, or every {@code catch (AuthException)} in the framework turns
     * it into a 401.
     */
    @Test
    void an_unavailable_authority_is_not_an_auth_exception() {
        assertFalse(AuthException.class.isAssignableFrom(AuthUnavailableException.class),
                "AuthUnavailableException must stay outside the rejection hierarchy");
    }

    /**
     * A chain advances past a rejection, but must propagate an outage.
     *
     * <p>Swallowed here it would emerge as a 401 from the end of the chain,
     * which every caller answers by re-authenticating at once — a thirty-second
     * sidecar blip becoming a fleet-wide re-login storm.
     */
    @Test
    void chain_propagates_an_unavailable_authority_instead_of_advancing() {
        Authenticator down = req -> { throw new AuthUnavailableException("identity sidecar unreachable"); };
        Authenticator wouldSucceed = req -> new AuthContext("test", true, "someone", Map.of());
        AuthUnavailableException e = assertThrows(AuthUnavailableException.class,
                () -> Authenticator.chain(down, wouldSucceed).authenticate(HttpRequestStub.withHeaders(Map.of())));
        assertTrue(e.retryAfterSeconds() >= 1);
    }

    /** ...and at the request boundary it renders 503 + Retry-After, never 401. */
    @Test
    void an_unavailable_authenticator_answers_503_not_401() throws Exception {
        start(HttpServer.Config.builder()
                .prefix("/vgi")
                .authenticator(req -> { throw new AuthUnavailableException("identity sidecar unreachable", 7, null); }));
        HttpResponse<String> resp = post(base + "/echo", "not-arrow-but-auth-runs-first");
        assertEquals(503, resp.statusCode());
        assertNotEquals(401, resp.statusCode());
        assertEquals("7", resp.headers().firstValue("Retry-After").orElseThrow());
    }

    /**
     * A resolver outage is a 503 too, not the 404 that means "did not resolve".
     *
     * <p>The two are the same shape on the wire and opposite in effect: a caller
     * may negative-cache a 404, so a resolver that reported its backing store
     * being down as one would lock out a live credential for the cache's
     * lifetime.
     */
    @Test
    void a_resolver_outage_answers_503_not_a_definitive_rejection() throws Exception {
        start(token -> { throw new AuthUnavailableException("token store unreachable"); });
        HttpResponse<String> resp = introspect(INTROSPECTOR, SUBJECT_TOKEN);
        assertEquals(503, resp.statusCode());
        assertTrue(resp.headers().firstValue("Retry-After").isPresent());
    }

    // ---- guard 3: the allowlist has no permissive default -----------------

    /** Enabling the endpoint without naming an introspector must not build. */
    @Test
    void an_empty_introspector_allowlist_is_rejected_at_construction() {
        HttpServer.Config.Builder b = HttpServer.Config.builder()
                .tokenIntrospection(fixedResolver(), List.of());
        assertThrows(IllegalArgumentException.class, b::build);
    }

    /** An allowlist with no resolver is a config that silently does nothing. */
    @Test
    void an_allowlist_without_a_resolver_is_rejected_at_construction() {
        HttpServer.Config.Builder b = HttpServer.Config.builder()
                .tokenIntrospection(null, List.of(INTROSPECTOR));
        assertThrows(IllegalArgumentException.class, b::build);
    }

    // ---- helpers ----------------------------------------------------------

    private HttpResponse<String> introspect(String caller, String token) throws Exception {
        HttpRequest.Builder req = HttpRequest.newBuilder(URI.create(base + "/" + TokenIntrospection.ENDPOINT))
                .timeout(Duration.ofSeconds(10))
                .header("Content-Type", MediaTypes.APPLICATION_JSON)
                .POST(HttpRequest.BodyPublishers.ofString("{\"token\":\"" + token + "\"}"));
        if (caller != null) req.header(PRINCIPAL_HEADER, caller);
        try (HttpClient client = newClient()) {
            return client.send(req.build(), HttpResponse.BodyHandlers.ofString());
        }
    }

    private static HttpResponse<String> post(String url, String body) throws Exception {
        try (HttpClient client = newClient()) {
            return client.send(HttpRequest.newBuilder(URI.create(url))
                            .timeout(Duration.ofSeconds(10))
                            .header("Content-Type", HttpServer.ARROW_CONTENT_TYPE)
                            .POST(HttpRequest.BodyPublishers.ofString(body)).build(),
                    HttpResponse.BodyHandlers.ofString());
        }
    }

    private static String body(HttpResponse<String> resp) { return resp.body(); }

    private static HttpClient newClient() {
        return HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    }
}
