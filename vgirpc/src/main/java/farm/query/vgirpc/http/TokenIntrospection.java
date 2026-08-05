// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import farm.query.vgirpc.AuthContext;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * {@code POST {prefix}/__introspect_token__} — resolving an opaque bearer
 * credential to a principal, for a reverse proxy that terminates the only
 * public listener.
 *
 * <p>Such a proxy has to know <em>which principal a credential authenticates
 * as</em> before it can authorize anything: that principal becomes the policy
 * principal, the row-rule literal, and the bind parameter of every entitlement
 * query. When the credential is opaque the proxy holds no local copy of it, so
 * it has to ask the worker.
 *
 * <p><strong>The response is an identity assertion made by the thing being
 * protected, and the asker acts on it using credentials the worker does not
 * hold</strong> — storage credentials on a data-plane host, service-credential
 * attachments in an entitlement resolver, policy-tier selection. "Trust it as
 * much as you trust the worker" is therefore the wrong frame: it has to be
 * trusted <em>more</em>, because it steers privileges the worker never has.
 * Every guard below follows from that, and none of them is optional:
 *
 * <ul>
 *   <li><strong>403</strong> — the caller may not introspect. Authentication and
 *   introspection are different capabilities: a deployment where any valid
 *   credential may introspect lets any user test guesses of any other user's
 *   credential at unlimited rate, and resolve a stolen one to its owner. The
 *   allowlist has no permissive default.</li>
 *   <li><strong>404</strong> — the subject credential did not resolve. Unknown,
 *   expired and malformed are byte-identical answers, because reporting which
 *   confirms that a guessed credential exists.</li>
 *   <li>JWS-shaped subjects are refused <em>without reaching the resolver</em>.
 *   A JWS is validated locally against a key set; routing one here hands a
 *   third party a bearer token the asker may itself have rejected, and an
 *   expired access token is still live at its issuer for other resources.</li>
 *   <li>The credential appears in no response, message, or log record. It is
 *   SHA-256 digested for diagnostics.</li>
 * </ul>
 *
 * <p>Both refusals are <em>definitive</em>: a caller may cache them. Anything
 * transient must reach the caller as 5xx so it is retried rather than cached —
 * see {@link AuthUnavailableException}.
 *
 * <p>Cross-language conformance groups: {@code TestTokenIntrospection} and
 * {@code TestTokenIntrospectionOffMode}. Wire contract:
 * {@code docs/WIRE_PROTOCOL.md} §16 in the vgi-rpc reference repository.
 */
public final class TokenIntrospection {

    private static final Logger LOG = LoggerFactory.getLogger(TokenIntrospection.class);

    /**
     * Endpoint path relative to the server's prefix. Matches the de-facto
     * contract the existing proxy client already speaks; changing it would cost
     * a lockstep release for no benefit.
     */
    public static final String ENDPOINT = "__introspect_token__";

    /**
     * Advertised on every response (including {@code OPTIONS /health}) when the
     * route is enabled, so a proxy preflights at boot rather than discovering at
     * first login that the worker it depends on cannot answer. Absent — never
     * {@code "false"} — otherwise.
     */
    public static final String ENABLED_HEADER = "VGI-Token-Introspection";

    /** Default cache lifetime advertised to the asker, in seconds. */
    public static final long DEFAULT_TTL_SECONDS = 300;

    /** Default per-caller request ceiling, per second. */
    public static final int DEFAULT_RATE_LIMIT_PER_SECOND = 20;

    /**
     * Three dot-separated base64url segments — a JWS. Such a credential is
     * validated locally against a key set and must never be routed here.
     */
    private static final Pattern JWS_SHAPED =
            Pattern.compile("^[A-Za-z0-9_-]+\\.[A-Za-z0-9_-]+\\.[A-Za-z0-9_-]*$");

    /**
     * Hard cap on the request body. The generic {@code maxRequestBytes} cap
     * would otherwise admit megabytes into a JSON parse for a body whose only
     * legitimate content is one credential.
     */
    private static final int MAX_BODY_BYTES = 8192;

    /**
     * Cap on a credential we will even attempt to resolve. Anything longer is
     * not a bearer token; refusing early keeps a resolver from being handed
     * megabytes.
     */
    private static final int MAX_TOKEN_CHARS = 4096;

    private static final ObjectMapper JSON = new ObjectMapper();

    private final TokenResolver resolver;
    private final Set<String> introspectors;
    private final long defaultTtlSeconds;
    private final RateLimiter limiter;

    /**
     * @param resolver resolves the subject credential; never the server's own authenticate chain
     * @param introspectors principals permitted to introspect; must be non-empty
     * @param defaultTtlSeconds TTL applied when a {@link TokenIdentity} names none
     * @param rateLimitPerSecond per-caller request ceiling
     */
    TokenIntrospection(TokenResolver resolver, Collection<String> introspectors,
                       long defaultTtlSeconds, int rateLimitPerSecond) {
        this.resolver = resolver;
        this.introspectors = Set.copyOf(normalizeIntrospectors(introspectors));
        this.defaultTtlSeconds = defaultTtlSeconds > 0 ? defaultTtlSeconds : DEFAULT_TTL_SECONDS;
        this.limiter = new RateLimiter(rateLimitPerSecond > 0 ? rateLimitPerSecond : DEFAULT_RATE_LIMIT_PER_SECOND);
    }

    /**
     * Validate the introspector allowlist.
     *
     * <p>There is no permissive default: "any authenticated caller" is precisely
     * the configuration that turns this endpoint into an open oracle, so it must
     * not be reachable by omission.
     *
     * @param principals the configured allowlist
     * @return the non-empty allowlist, blanks dropped
     * @throws IllegalArgumentException if it names no principal
     */
    static Set<String> normalizeIntrospectors(Collection<String> principals) {
        Set<String> allowed = new HashSet<>();
        if (principals != null) {
            for (String p : principals) {
                if (p != null && !p.isEmpty()) allowed.add(p);
            }
        }
        if (allowed.isEmpty()) {
            throw new IllegalArgumentException(
                    "introspectPrincipals must name at least one principal. Introspection is a "
                            + "distinct capability from authentication: allowing any authenticated "
                            + "caller lets any user resolve any other user's credential to its owner.");
        }
        return allowed;
    }

    /**
     * SHA-256 hex digest of {@code token}, for diagnostics.
     *
     * <p>The credential itself must never reach a log, a span, or an error
     * message. A digest is stable enough to correlate one credential's failures
     * across records without being the credential.
     *
     * @param token the opaque credential
     * @return lowercase hex digest
     */
    static String digest(String token) {
        try {
            MessageDigest sha = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(sha.digest(token.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    /** Whether {@code token} looks like a JWS and must be refused unresolved. */
    static boolean isJwsShaped(String token) { return JWS_SHAPED.matcher(token).matches(); }

    /**
     * Answer {@code 404 not_enabled} for a worker that did not turn the feature on.
     *
     * <p>The oracle is still absent in every sense that matters: no resolver is
     * held, nothing is looked up, and the answer does not depend on the request.
     * What this adds is a <em>definitive</em> answer for a caller that asks
     * anyway. Left unrouted the path falls through to the generic {@code {method}}
     * route, which answers a JSON body with a 500 (the Arrow reader finds no IPC
     * stream) — and a caller that classifies {@code 401/403/404} as definitive
     * and everything else as transient, which is the sensible classification and
     * the one the existing proxy client uses, reads that as "try again later"
     * and retries forever against a worker that will never support the feature.
     * A misconfiguration should stop, not spin.
     *
     * <p>Deliberately unauthenticated: "this worker does not do introspection" is
     * not a secret, and a caller needs to learn it at preflight rather than after
     * arranging credentials.
     *
     * @param resp the response to write
     * @throws IOException if the response body cannot be written
     */
    static void writeNotEnabled(HttpServletResponse resp) throws IOException {
        refuse(resp, HttpServletResponse.SC_NOT_FOUND, "not_enabled");
    }

    /**
     * Resolve the posted credential to a principal.
     *
     * @param req the introspection request
     * @param resp the response to write
     * @param auth the caller's authenticated context (never the subject's)
     * @throws IOException if the response body cannot be written
     */
    void handle(HttpServletRequest req, HttpServletResponse resp, AuthContext auth) throws IOException {
        String caller = auth != null && auth.principal() != null ? auth.principal() : "";
        boolean authenticated = auth != null && auth.authenticated();

        // Caller authorization first: an unauthorized caller must not learn
        // anything about a subject credential, including how long it took.
        if (!authenticated || !introspectors.contains(caller)) {
            LOG.warn("introspection refused: caller is not an introspector (remote_addr={}, principal={})",
                    req.getRemoteAddr(), caller);
            refuse(resp, HttpServletResponse.SC_FORBIDDEN, "not_an_introspector");
            return;
        }

        if (!limiter.allow(caller)) {
            LOG.warn("introspection rate limit exceeded (principal={})", caller);
            resp.setHeader("Retry-After", "1");
            refuse(resp, 429, "rate_limited");
            return;
        }

        String token = readToken(req);
        if (token == null) {
            // Indistinguishable from an unresolvable credential: a malformed body
            // is not worth a separate signal, and giving one lets a caller probe
            // the parser.
            refuse(resp, HttpServletResponse.SC_NOT_FOUND, "unresolved");
            return;
        }

        String digest = digest(token);

        if (isJwsShaped(token)) {
            // Refused without ever reaching the resolver. A JWS arriving here is
            // either a caller bug or an attempt to have this worker vouch for a
            // token its asker already rejected.
            LOG.warn("introspection refused: JWS-shaped subject (principal={}, token_digest={})", caller, digest);
            refuse(resp, HttpServletResponse.SC_NOT_FOUND, "unresolved");
            return;
        }

        Optional<TokenIdentity> identity = resolver.resolve(token);
        if (identity == null || identity.isEmpty()) {
            LOG.info("introspection: credential did not resolve (principal={}, token_digest={})", caller, digest);
            refuse(resp, HttpServletResponse.SC_NOT_FOUND, "unresolved");
            return;
        }

        TokenIdentity id = identity.get();
        LOG.info("introspection: resolved (principal={}, token_digest={}, resolved_principal={})",
                caller, digest, id.principal());

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("principal", id.principal());
        body.put("token_name", id.tokenName());
        body.put("ttl_seconds", id.ttlSeconds() > 0 ? id.ttlSeconds() : defaultTtlSeconds);
        resp.setStatus(HttpServletResponse.SC_OK);
        resp.setContentType(MediaTypes.APPLICATION_JSON);
        // A credential's resolution can change; nothing here may sit in a shared cache.
        resp.setHeader("Cache-Control", "no-store");
        resp.getOutputStream().write(JSON.writeValueAsBytes(body));
    }

    /**
     * Write a rejection carrying no detail about why.
     *
     * <p>Hand-rolled rather than serialized so every rejection with the same code
     * is byte-identical — {@code test_rejections_are_indistinguishable} compares
     * response text, not just status.
     */
    private static void refuse(HttpServletResponse resp, int status, String error) throws IOException {
        resp.setStatus(status);
        resp.setContentType(MediaTypes.APPLICATION_JSON);
        resp.setHeader("Cache-Control", "no-store");
        resp.getOutputStream().write(("{\"error\":\"" + error + "\"}").getBytes(StandardCharsets.UTF_8));
    }

    /** The subject credential, or {@code null} when the body is unusable. */
    private static String readToken(HttpServletRequest req) {
        long declared = req.getContentLengthLong();
        if (declared > MAX_BODY_BYTES) return null;
        byte[] raw;
        try (InputStream in = req.getInputStream()) {
            raw = in.readNBytes(MAX_BODY_BYTES + 1);
        } catch (IOException e) {
            return null;
        }
        if (raw.length > MAX_BODY_BYTES) return null;
        JsonNode node;
        try {
            node = JSON.readTree(raw);
        } catch (IOException e) {
            return null;
        }
        if (node == null || !node.isObject()) return null;
        JsonNode token = node.get("token");
        if (token == null || !token.isTextual()) return null;
        String value = token.asText();
        if (value.isEmpty() || value.length() > MAX_TOKEN_CHARS) return null;
        return value;
    }

    /**
     * Fixed-window request limiter, keyed by caller.
     *
     * <p>Present because the endpoint is a credential-to-identity oracle even
     * when correctly restricted: an allowlisted caller whose own credential leaks
     * can still test guesses. Rate limiting does not close that, it bounds it — a
     * lower ceiling on how fast an attacker converts guesses to answers.
     *
     * <p>Fixed-window rather than a token bucket: a window admits at most twice
     * the rate across a boundary, which is a rounding error here, and the state
     * is one integer per caller rather than a float that has to be aged.
     */
    private static final class RateLimiter {
        private final int perWindow;
        private final Map<String, Integer> counts = new HashMap<>();
        private long windowStartNanos;

        RateLimiter(int perWindow) { this.perWindow = perWindow; }

        synchronized boolean allow(String key) {
            long now = System.nanoTime();
            if (now - windowStartNanos >= 1_000_000_000L) {
                // Whole-map reset rather than per-key ageing: an attacker cycling
                // keys cannot grow the map beyond one window's worth.
                counts.clear();
                windowStartNanos = now;
            }
            int count = counts.getOrDefault(key, 0);
            if (count >= perWindow) return false;
            counts.put(key, count + 1);
            return true;
        }
    }
}
