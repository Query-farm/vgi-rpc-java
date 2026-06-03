// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http.auth;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import farm.query.vgirpc.AuthContext;
import farm.query.vgirpc.http.AuthException;
import farm.query.vgirpc.http.Authenticator;
import farm.query.vgirpc.http.HttpPreHandler;
import farm.query.vgirpc.http.InvalidCredentials;
import farm.query.vgirpc.http.MediaTypes;
import farm.query.vgirpc.http.MissingCredentials;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Self-contained OAuth 2.1 PKCE (RFC 7636 + 6749 + 8252) flow helper.
 *
 * <p>Callers attach two things to the {@link farm.query.vgirpc.http.HttpServer}:
 * the {@link #preHandler()} intercepts requests to the {@code callbackPath} and
 * redirects unauthenticated browser requests to the authorization server; the
 * {@link #authenticator()} reads the signed auth cookie on authenticated
 * requests and produces an {@link AuthContext}.</p>
 *
 * <p>Cookies are HMAC-SHA256 signed + URL-safe base64 using
 * {@link SignedCookie}; the ID token claims live inside the auth cookie as
 * JSON (not the raw JWT — the server-side validation has already happened).</p>
 */
public final class OAuthPkce {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> STRING_OBJECT_MAP = new TypeReference<>() {};
    private static final TypeReference<Map<String, String>> STRING_STRING_MAP = new TypeReference<>() {};

    private static final String SESSION_COOKIE = "_vgi_oauth_session";
    private static final String AUTH_COOKIE = "_vgi_auth";
    private static final long SESSION_TTL_SECONDS = 600;       // 10 min — authorization round-trip
    private static final long DEFAULT_AUTH_TTL_SECONDS = 3600; // 1 hour default if id_token has no exp

    private final String clientId;
    private final String redirectUri;
    private final String callbackPath;
    private final String scope;
    private final byte[] sessionKey;
    private final byte[] authKey;
    private final long authTtlSeconds;
    private final OidcMetadata oidc;
    private final JwtAuthenticator idTokenValidator;
    private final HttpClient http;
    private final String domain;

    /**
     * Start building an {@code OAuthPkce} flow helper.
     *
     * @return a fresh {@link Builder}
     */
    public static Builder builder() { return new Builder(); }

    private OAuthPkce(Builder b) {
        this.clientId = Objects.requireNonNull(b.clientId, "clientId");
        this.redirectUri = Objects.requireNonNull(b.redirectUri, "redirectUri");
        this.callbackPath = Objects.requireNonNull(b.callbackPath, "callbackPath");
        this.scope = b.scope;
        this.sessionKey = b.sessionKey.clone();
        this.authKey = b.authKey.clone();
        this.authTtlSeconds = b.authTtlSeconds;
        this.oidc = Objects.requireNonNull(b.oidc, "oidc");
        this.idTokenValidator = Objects.requireNonNull(b.idTokenValidator, "idTokenValidator");
        this.domain = b.domain;
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    }

    /** Mount on {@code HttpServer}'s preHandler list to route {@code callbackPath} requests. */
    public HttpPreHandler preHandler() {
        return (req, resp) -> {
            String path = req.getRequestURI();
            if (path != null && path.equals(callbackPath)) {
                handleCallback(req, resp);
                return true;
            }
            return false;
        };
    }

    /** Authenticator that reads the auth cookie set by the callback handler. */
    public Authenticator authenticator() {
        return request -> {
            String token = cookie(request, AUTH_COOKIE);
            if (token == null) throw new MissingCredentials("Missing auth cookie");
            try {
                byte[] payload = SignedCookie.verify(token, authKey);
                SignedCookie.TimestampedPayload tp =
                        SignedCookie.TimestampedPayload.unpack(payload, authTtlSeconds);
                Map<String, Object> claims = JSON.readValue(tp.payload(), STRING_OBJECT_MAP);
                String principal = String.valueOf(claims.getOrDefault("sub", ""));
                return new AuthContext(domain, true, principal, claims);
            } catch (Exception e) {
                throw new InvalidCredentials("Invalid auth cookie: " + e.getMessage());
            }
        };
    }

    /**
     * Build a URL to start the PKCE flow: generates verifier + challenge, stores
     * them in a signed session cookie, and returns the authorize URL to redirect
     * the browser to. Callers typically invoke this in a 401 handler.
     */
    public String beginAuthorization(HttpServletResponse resp, String returnTo) {
        String verifier = Pkce.generateCodeVerifier();
        String challenge = Pkce.codeChallengeS256(verifier);
        String state = Pkce.generateStateNonce();

        Map<String, String> session = new LinkedHashMap<>();
        session.put("verifier", verifier);
        session.put("state", state);
        if (returnTo != null) session.put("return_to", returnTo);
        try {
            byte[] payload = SignedCookie.TimestampedPayload.pack(JSON.writeValueAsBytes(session));
            String signed = SignedCookie.sign(payload, sessionKey);
            Cookie c = new Cookie(SESSION_COOKIE, signed);
            c.setHttpOnly(true);
            c.setPath("/");
            c.setMaxAge((int) SESSION_TTL_SECONDS);
            resp.addCookie(c);
        } catch (Exception e) {
            throw new IllegalStateException("failed to build session cookie", e);
        }

        StringBuilder url = new StringBuilder(oidc.authorizationEndpoint().toString());
        url.append('?');
        url.append("response_type=code");
        url.append("&client_id=").append(enc(clientId));
        url.append("&redirect_uri=").append(enc(redirectUri));
        url.append("&code_challenge=").append(enc(challenge));
        url.append("&code_challenge_method=S256");
        url.append("&state=").append(enc(state));
        if (scope != null) url.append("&scope=").append(enc(scope));
        return url.toString();
    }

    private void handleCallback(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        String code = req.getParameter("code");
        String state = req.getParameter("state");
        if (code == null || state == null) {
            badRequest(resp, "missing code or state");
            return;
        }
        String sessionCookie = cookie(req, SESSION_COOKIE);
        if (sessionCookie == null) {
            badRequest(resp, "missing session cookie");
            return;
        }
        Map<String, String> session;
        try {
            byte[] raw = SignedCookie.verify(sessionCookie, sessionKey);
            SignedCookie.TimestampedPayload tp = SignedCookie.TimestampedPayload.unpack(raw, SESSION_TTL_SECONDS);
            session = JSON.readValue(tp.payload(), STRING_STRING_MAP);
        } catch (Exception e) {
            badRequest(resp, "invalid session cookie: " + e.getMessage());
            return;
        }
        if (!state.equals(session.get("state"))) {
            badRequest(resp, "state mismatch — possible CSRF");
            return;
        }
        String verifier = session.get("verifier");
        String returnTo = session.getOrDefault("return_to", "/");

        // Exchange code for tokens
        Map<String, String> form = new LinkedHashMap<>();
        form.put("grant_type", "authorization_code");
        form.put("code", code);
        form.put("redirect_uri", redirectUri);
        form.put("client_id", clientId);
        form.put("code_verifier", verifier);
        String body = form.entrySet().stream()
                .map(e -> enc(e.getKey()) + "=" + enc(e.getValue()))
                .collect(Collectors.joining("&"));
        HttpResponse<String> tokenResp;
        try {
            tokenResp = http.send(
                    HttpRequest.newBuilder(oidc.tokenEndpoint())
                            .header("Content-Type", "application/x-www-form-urlencoded")
                            .header("Accept", MediaTypes.APPLICATION_JSON)
                            .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            badRequest(resp, "token endpoint interrupted");
            return;
        }
        if (tokenResp.statusCode() / 100 != 2) {
            badRequest(resp, "token endpoint HTTP " + tokenResp.statusCode() + ": "
                    + tokenResp.body());
            return;
        }
        JsonNode tokens = JSON.readTree(tokenResp.body());
        JsonNode idTokenNode = tokens.get("id_token");
        if (idTokenNode == null || !idTokenNode.isTextual()) {
            badRequest(resp, "token response missing id_token");
            return;
        }
        String idToken = idTokenNode.asText();

        // Validate id_token via JwtAuthenticator (runs JWKS + signature + issuer/audience).
        AuthContext claims;
        try {
            claims = idTokenValidator.validateBearer(idToken);
        } catch (AuthException ae) {
            badRequest(resp, "id_token validation failed: " + ae.getMessage());
            return;
        }

        // Package claims into the auth cookie.
        try {
            byte[] claimsBytes = JSON.writeValueAsBytes(claims.claims());
            byte[] wrapped = SignedCookie.TimestampedPayload.pack(claimsBytes);
            String signed = SignedCookie.sign(wrapped, authKey);
            Cookie c = new Cookie(AUTH_COOKIE, signed);
            c.setHttpOnly(true);
            c.setPath("/");
            c.setMaxAge((int) authTtlSeconds);
            resp.addCookie(c);
            // Clear the session cookie.
            Cookie sc = new Cookie(SESSION_COOKIE, "");
            sc.setPath("/");
            sc.setMaxAge(0);
            resp.addCookie(sc);
        } catch (Exception e) {
            badRequest(resp, "failed to issue auth cookie: " + e.getMessage());
            return;
        }

        resp.setStatus(HttpServletResponse.SC_FOUND);
        resp.setHeader("Location", returnTo);
    }

    private static String cookie(HttpServletRequest req, String name) {
        Cookie[] cookies = req.getCookies();
        if (cookies == null) return null;
        for (Cookie c : cookies) if (c.getName().equals(name)) return c.getValue();
        return null;
    }

    private static String enc(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8);
    }

    private static void badRequest(HttpServletResponse resp, String msg) throws IOException {
        resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        resp.setContentType("text/plain; charset=utf-8");
        resp.getOutputStream().write(("oauth callback error: " + msg).getBytes(StandardCharsets.UTF_8));
    }

    public static final class Builder {
        private String clientId;
        private String redirectUri;
        private String callbackPath = "/_oauth/callback";
        private String scope = "openid";
        private byte[] sessionKey;
        private byte[] authKey;
        private long authTtlSeconds = DEFAULT_AUTH_TTL_SECONDS;
        private OidcMetadata oidc;
        private JwtAuthenticator idTokenValidator;
        private String domain = "oauth-pkce";

        /** OAuth client id registered with the authorization server. Required. */
        public Builder clientId(String v) { this.clientId = v; return this; }
        /** Absolute redirect URI registered with the authorization server (the {@code callbackPath} URL). Required. */
        public Builder redirectUri(String v) { this.redirectUri = v; return this; }
        /** Local path the {@link #preHandler()} intercepts for the OAuth callback (default {@code "/_oauth/callback"}). */
        public Builder callbackPath(String v) { this.callbackPath = v; return this; }
        /** Requested OAuth scope (default {@code "openid"}). */
        public Builder scope(String v) { this.scope = v; return this; }
        /** HMAC key for signing the short-lived PKCE session cookie. The bytes are copied. Required. */
        public Builder sessionKey(byte[] v) { this.sessionKey = v.clone(); return this; }
        /** HMAC key for signing the authenticated user cookie. The bytes are copied. Required. */
        public Builder authKey(byte[] v) { this.authKey = v.clone(); return this; }
        /** Lifetime of the auth cookie in seconds when the id token carries no {@code exp} (default one hour). */
        public Builder authTtlSeconds(long v) { this.authTtlSeconds = v; return this; }
        /** Authorization-server endpoints (authorize/token). Required. */
        public Builder oidcMetadata(OidcMetadata v) { this.oidc = v; return this; }
        /** Validator that verifies the returned {@code id_token} (JWKS + signature + issuer/audience). Required. */
        public Builder idTokenValidator(JwtAuthenticator v) { this.idTokenValidator = v; return this; }
        /** Auth domain label recorded on the resulting {@link AuthContext} (default {@code "oauth-pkce"}). */
        public Builder domain(String v) { this.domain = v; return this; }

        /**
         * Build the flow helper.
         *
         * @throws NullPointerException if any required field is unset
         */
        public OAuthPkce build() { return new OAuthPkce(this); }
    }
}
