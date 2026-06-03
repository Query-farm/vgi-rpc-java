// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http.auth;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.jwk.source.JWKSource;
import com.nimbusds.jose.jwk.source.JWKSourceBuilder;
import com.nimbusds.jose.proc.JWSKeySelector;
import com.nimbusds.jose.proc.JWSVerificationKeySelector;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.proc.ConfigurableJWTProcessor;
import com.nimbusds.jwt.proc.DefaultJWTClaimsVerifier;
import com.nimbusds.jwt.proc.DefaultJWTProcessor;
import farm.query.vgirpc.AuthContext;
import farm.query.vgirpc.http.AuthException;
import farm.query.vgirpc.http.Authenticator;
import farm.query.vgirpc.http.HttpHeaders;
import farm.query.vgirpc.http.InvalidCredentials;
import farm.query.vgirpc.http.MissingCredentials;
import jakarta.servlet.http.HttpServletRequest;

import java.net.URI;
import java.net.URL;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Bearer-token authenticator that validates JWTs against a remote JWKS
 * endpoint. Backed by {@code nimbus-jose-jwt}; matches the Python
 * {@code jwt_authenticate} factory: multiple issuers/audiences accepted,
 * automatic JWKS refresh on unknown {@code kid}, principal pulled from a
 * configurable claim (default {@code sub}).
 */
public final class JwtAuthenticator implements Authenticator {

    private static final String CHALLENGE = "Bearer realm=\"vgi-rpc\"";

    private final ConfigurableJWTProcessor<SecurityContext> processor;
    private final List<String> issuers;
    private final List<String> audiences;
    private final String principalClaim;
    private final String domain;

    /**
     * Start building a {@code JwtAuthenticator}.
     *
     * @return a fresh {@link Builder}
     */
    public static Builder builder() { return new Builder(); }

    private JwtAuthenticator(Builder b) {
        this.issuers = List.copyOf(b.issuers);
        this.audiences = List.copyOf(b.audiences);
        this.principalClaim = b.principalClaim;
        this.domain = b.domain;

        URL jwksUri = resolveJwksUri(b);
        try {
            long cacheMs = b.cacheTtlSeconds * 1000L;
            long refreshAheadMs = Math.max(1_000L, cacheMs / 5);
            JWKSource<SecurityContext> jwkSource = JWKSourceBuilder.create(jwksUri)
                    .retrying(true)
                    .cache(cacheMs, refreshAheadMs)
                    .build();

            DefaultJWTProcessor<SecurityContext> p = new DefaultJWTProcessor<>();
            Set<JWSAlgorithm> algs = b.allowedAlgorithms.isEmpty()
                    ? Set.of(JWSAlgorithm.RS256, JWSAlgorithm.RS384, JWSAlgorithm.RS512,
                             JWSAlgorithm.ES256, JWSAlgorithm.ES384, JWSAlgorithm.ES512,
                             JWSAlgorithm.PS256, JWSAlgorithm.PS384, JWSAlgorithm.PS512)
                    : b.allowedAlgorithms;
            JWSKeySelector<SecurityContext> ksel = new JWSVerificationKeySelector<>(algs, jwkSource);
            p.setJWSKeySelector(ksel);

            JWTClaimsSet.Builder requiredBuilder = new JWTClaimsSet.Builder();
            Set<String> required = new HashSet<>(Set.of("iss", "aud", "exp"));
            required.addAll(b.requiredClaims);
            p.setJWTClaimsSetVerifier(new IssuerAudienceVerifier(issuers, audiences, required));
            this.processor = p;
        } catch (Exception e) {
            throw new IllegalStateException("failed to build JWT processor: " + e.getMessage(), e);
        }
    }

    private URL resolveJwksUri(Builder b) {
        if (b.jwksUri != null) return b.jwksUri;
        if (issuers.isEmpty()) throw new IllegalStateException("jwksUri or at least one issuer required");
        try {
            return OidcMetadata.discover(issuers.get(0)).jwksUri().toURL();
        } catch (Exception e) {
            throw new IllegalStateException("OIDC discovery failed: " + e.getMessage(), e);
        }
    }

    /**
     * Validate a bare JWT string (no HTTP request wrapping). Runs JWKS key
     * selection, signature verification, and the issuer/audience/required-claim
     * checks, then maps the configured principal claim into the returned context.
     *
     * @param token the raw compact JWT
     * @return an authenticated {@link AuthContext} carrying all token claims
     * @throws MissingCredentials if {@code token} is null or empty
     * @throws InvalidCredentials if signature, issuer, audience, or a required claim fails
     */
    public AuthContext validateBearer(String token) throws AuthException {
        if (token == null || token.isEmpty()) {
            throw new MissingCredentials("Missing bearer token", CHALLENGE);
        }
        try {
            JWTClaimsSet claims = processor.process(token, null);
            Object rawPrincipal = claims.getClaim(principalClaim);
            String principal = rawPrincipal == null ? "" : rawPrincipal.toString();
            Map<String, Object> claimsMap = new LinkedHashMap<>(claims.getClaims());
            return new AuthContext(domain, true, principal, claimsMap);
        } catch (Exception e) {
            throw new InvalidCredentials("Invalid JWT: " + e.getMessage(), CHALLENGE);
        }
    }

    /**
     * Extract the {@code Bearer} token from the {@code Authorization} header and
     * validate it via {@link #validateBearer(String)}.
     *
     * @param request the inbound HTTP request
     * @return an authenticated {@link AuthContext}
     * @throws MissingCredentials if the {@code Authorization: Bearer} header is absent or malformed
     * @throws InvalidCredentials if the token fails validation
     */
    @Override
    public AuthContext authenticate(HttpServletRequest request) throws AuthException {
        String header = request.getHeader(HttpHeaders.AUTHORIZATION);
        if (header == null || !header.regionMatches(true, 0, HttpHeaders.BEARER_PREFIX, 0, HttpHeaders.BEARER_PREFIX.length())) {
            throw new MissingCredentials("Missing or invalid Authorization header", CHALLENGE);
        }
        return validateBearer(header.substring(HttpHeaders.BEARER_PREFIX.length()).trim());
    }

    /** Issuer + audience whitelist with at-least-one-of semantics + required-claims. */
    static final class IssuerAudienceVerifier extends DefaultJWTClaimsVerifier<SecurityContext> {
        private final Set<String> issuers;
        private final Set<String> audiences;

        IssuerAudienceVerifier(List<String> issuers, List<String> audiences, Set<String> required) {
            super(null, required);
            this.issuers = new HashSet<>(issuers);
            this.audiences = new HashSet<>(audiences);
        }

        @Override
        public void verify(JWTClaimsSet claims, SecurityContext ctx) throws com.nimbusds.jwt.proc.BadJWTException {
            super.verify(claims, ctx);
            String iss = claims.getIssuer();
            if (iss == null || !issuers.contains(iss)) {
                throw new com.nimbusds.jwt.proc.BadJWTException("issuer not accepted: " + iss);
            }
            List<String> aud = claims.getAudience();
            if (aud == null || aud.stream().noneMatch(audiences::contains)) {
                throw new com.nimbusds.jwt.proc.BadJWTException("audience not accepted: " + aud);
            }
        }
    }

    /** Fluent builder for {@link JwtAuthenticator}. */
    public static final class Builder {
        private final java.util.ArrayList<String> issuers = new java.util.ArrayList<>();
        private final java.util.ArrayList<String> audiences = new java.util.ArrayList<>();
        private URL jwksUri;
        private String principalClaim = "sub";
        private String domain = "jwt";
        private long cacheTtlSeconds = 300;
        private Set<JWSAlgorithm> allowedAlgorithms = Collections.emptySet();
        private Set<String> requiredClaims = Collections.emptySet();

        /** Accept tokens from this issuer ({@code iss}); may be called more than once. At least one is required. */
        public Builder issuer(String iss) { issuers.add(iss); return this; }
        /** Accept tokens for this audience ({@code aud}); may be called more than once. At least one is required. */
        public Builder audience(String aud) { audiences.add(aud); return this; }
        /** Set the JWKS endpoint explicitly. When unset, it is discovered from the first issuer's OIDC metadata. */
        public Builder jwksUri(URL uri) { this.jwksUri = uri; return this; }
        /**
         * Set the JWKS endpoint from a string URL.
         *
         * @throws IllegalArgumentException if {@code uri} is not a valid URL
         */
        public Builder jwksUri(String uri) {
            try { this.jwksUri = URI.create(uri).toURL(); return this; }
            catch (Exception e) { throw new IllegalArgumentException("bad jwksUri: " + uri, e); }
        }
        /** JWT claim mapped to {@link AuthContext#principal()} (default {@code "sub"}). */
        public Builder principalClaim(String c) { this.principalClaim = c; return this; }
        /** Auth domain label recorded on the resulting {@link AuthContext} (default {@code "jwt"}). */
        public Builder domain(String d) { this.domain = d; return this; }
        /** JWKS cache lifetime in seconds (default 300); a refresh-ahead window is derived from it. */
        public Builder cacheTtlSeconds(long seconds) { this.cacheTtlSeconds = seconds; return this; }
        /** Restrict accepted JWS signature algorithms; when empty, the common RSA/EC/PS family is allowed. */
        public Builder allowedAlgorithms(Set<JWSAlgorithm> algs) { this.allowedAlgorithms = Set.copyOf(algs); return this; }
        /** Require an additional claim to be present (beyond the always-required {@code iss}/{@code aud}/{@code exp}). */
        public Builder requireClaim(String name) {
            Set<String> n = new HashSet<>(requiredClaims); n.add(name); this.requiredClaims = n; return this;
        }

        /**
         * Build the authenticator.
         *
         * @throws IllegalStateException if no issuer or no audience was configured
         */
        public JwtAuthenticator build() {
            if (issuers.isEmpty()) throw new IllegalStateException("issuer required");
            if (audiences.isEmpty()) throw new IllegalStateException("audience required");
            return new JwtAuthenticator(this);
        }
    }
}
