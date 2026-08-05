// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import farm.query.vgirpc.http.auth.ProxyProof;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * The CORS half of the HTTP transport: which browsers may call this worker,
 * and which of its response headers they are allowed to read.
 *
 * <p>Every capability this framework has over HTTP rides on a response header
 * — the size caps, the codec set, the sticky advert, the 401 reason code, the
 * {@code X-VGI-RPC-Error} flag. A browser hides all of them from JavaScript
 * unless the server names them in {@code Access-Control-Expose-Headers}, so
 * the expose list is built in lockstep with
 * {@code HttpServer.applyCapabilityHeaders}: whatever this server advertises,
 * it exposes. Cross-language conformance group: {@code TestCors}.
 *
 * <p>Strictly opt-in — an unconfigured server has no policy at all and emits
 * no CORS header on any response ({@code TestCorsOffMode}).
 */
final class CorsPolicy {

    static final String ORIGIN            = "Origin";
    static final String ALLOW_ORIGIN      = "Access-Control-Allow-Origin";
    static final String ALLOW_METHODS     = "Access-Control-Allow-Methods";
    static final String ALLOW_HEADERS     = "Access-Control-Allow-Headers";
    static final String EXPOSE_HEADERS    = "Access-Control-Expose-Headers";
    static final String MAX_AGE           = "Access-Control-Max-Age";
    static final String REQUEST_HEADERS   = "Access-Control-Request-Headers";
    static final String RESOURCE_POLICY   = "Cross-Origin-Resource-Policy";

    /** The wildcard origin, allowed because this server never sets
     *  {@code Access-Control-Allow-Credentials} (vgi-rpc auth is header-borne,
     *  never cookie-borne), so a wildcard grants no ambient authority. */
    static final String WILDCARD = "*";

    /** Every method the router answers. GET and DELETE are here because health,
     *  {@code describe.json} and session close are as much part of the browser
     *  surface as the RPC POST is. */
    private static final String ALLOW_METHODS_VALUE = "GET, POST, DELETE, OPTIONS";

    /** Answer for a preflight that named no headers — the request-side surface
     *  a browser client actually needs. Real preflights always name headers and
     *  take the echo path below. */
    private static final String DEFAULT_ALLOW_HEADERS = String.join(", ", List.of(
            HttpHeaders.CONTENT_TYPE,
            HttpHeaders.AUTHORIZATION,
            HttpHeaders.API_KEY,
            HttpHeaders.X_VGI_ACCEPT_ENCODING,
            StickyHeaders.SESSION,
            StickyHeaders.SESSION_ACCEPT,
            ProxyProof.PROOF_HEADER));

    /** Lowercased for the case-insensitive origin match; empty when {@link #wildcard}. */
    private final List<String> origins;
    private final boolean wildcard;
    private final String exposeHeaders;
    /** Rendered {@code Access-Control-Max-Age} value, or {@code null} to omit it. */
    private final String maxAge;

    /**
     * @param origins       allowed origins; a single {@code "*"} entry allows all
     * @param maxAgeSeconds preflight cache lifetime; {@code 0} omits the header
     * @param exposeHeaders response headers a browser may read, already deduped
     */
    CorsPolicy(List<String> origins, long maxAgeSeconds, List<String> exposeHeaders) {
        List<String> normalized = new ArrayList<>();
        boolean any = false;
        for (String o : origins) {
            String trimmed = o.trim();
            if (trimmed.isEmpty()) continue;
            if (WILDCARD.equals(trimmed)) any = true;
            else normalized.add(trimmed.toLowerCase(Locale.ROOT));
        }
        if (!any && normalized.isEmpty()) {
            throw new IllegalArgumentException("corsOrigins must name at least one origin");
        }
        this.wildcard = any;
        this.origins = List.copyOf(normalized);
        this.exposeHeaders = String.join(", ", exposeHeaders);
        this.maxAge = maxAgeSeconds > 0 ? Long.toString(maxAgeSeconds) : null;
    }

    /**
     * Stamp the CORS headers for {@code req} onto {@code resp}, if the request
     * came from an allowed origin.
     *
     * <p>Applied to every response, not just the preflight: a browser re-checks
     * {@code Access-Control-Allow-Origin} on the actual response and discards
     * the body without it, so a preflight-only implementation fails every real
     * call while looking correct from a test client.
     */
    void apply(HttpServletRequest req, HttpServletResponse resp) {
        String origin = req.getHeader(ORIGIN);
        if (origin == null || origin.isBlank()) return;  // same-origin request: nothing to grant
        String allow = resolve(origin);
        if (allow == null) return;

        resp.setHeader(ALLOW_ORIGIN, allow);
        if (!wildcard) {
            // The answer depends on the request's Origin, so a shared cache that
            // keyed only on the URL would hand one origin's grant to another.
            resp.setHeader("Vary", ORIGIN);
        }
        resp.setHeader(ALLOW_METHODS, ALLOW_METHODS_VALUE);
        // Echoing what the preflight asked for keeps any client-side header
        // working without the server enumerating it — the same answer the Go,
        // Rust and Python ports give.
        String requested = req.getHeader(REQUEST_HEADERS);
        resp.setHeader(ALLOW_HEADERS,
                requested != null && !requested.isBlank() ? requested : DEFAULT_ALLOW_HEADERS);
        resp.setHeader(EXPOSE_HEADERS, exposeHeaders);
        // Opt into cross-origin embedding so the worker stays usable from
        // cross-origin-isolated pages (COEP: require-corp) — e.g. browsers
        // running multithreaded WASM against it.
        resp.setHeader(RESOURCE_POLICY, "cross-origin");
        if (maxAge != null && "OPTIONS".equals(req.getMethod())) {
            resp.setHeader(MAX_AGE, maxAge);
        }
    }

    /** The {@code Access-Control-Allow-Origin} value for {@code origin}, or {@code null} if it is not allowed. */
    private String resolve(String origin) {
        if (wildcard) return WILDCARD;
        return origins.contains(origin.trim().toLowerCase(Locale.ROOT)) ? origin : null;
    }

    /** The exposed set, for tests and diagnostics. */
    String exposeHeaders() { return exposeHeaders; }
}
