// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http.auth;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
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
import jakarta.servlet.http.HttpServletRequest;

import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
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
        // OIDC discovery: fetch {issuer}/.well-known/openid-configuration and read jwks_uri.
        String discovery = issuers.get(0).replaceAll("/+$", "") + "/.well-known/openid-configuration";
        try {
            HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
            HttpResponse<String> resp = client.send(
                    HttpRequest.newBuilder(URI.create(discovery)).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() / 100 != 2) {
                throw new IllegalStateException("OIDC discovery failed: HTTP " + resp.statusCode());
            }
            String body = resp.body();
            // crude extraction of "jwks_uri": - any JSON lib works but minimising deps
            int idx = body.indexOf("\"jwks_uri\"");
            if (idx < 0) throw new IllegalStateException("OIDC metadata missing jwks_uri");
            int q1 = body.indexOf('"', body.indexOf(':', idx) + 1);
            int q2 = body.indexOf('"', q1 + 1);
            return URI.create(body.substring(q1 + 1, q2)).toURL();
        } catch (Exception e) {
            throw new IllegalStateException("OIDC discovery failed: " + e.getMessage(), e);
        }
    }

    @Override
    public AuthContext authenticate(HttpServletRequest request) throws AuthException {
        String header = request.getHeader("Authorization");
        if (header == null || !header.regionMatches(true, 0, "Bearer ", 0, 7)) {
            throw new AuthException("Missing or invalid Authorization header", CHALLENGE);
        }
        String token = header.substring(7).trim();
        try {
            JWTClaimsSet claims = processor.process(token, null);
            String principal = String.valueOf(claims.getClaim(principalClaim));
            if (principal == null || "null".equals(principal)) principal = "";
            Map<String, Object> claimsMap = new LinkedHashMap<>(claims.getClaims());
            return new AuthContext(domain, true, principal, claimsMap);
        } catch (Exception e) {
            throw new AuthException("Invalid JWT: " + e.getMessage(), CHALLENGE);
        }
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

        public Builder issuer(String iss) { issuers.add(iss); return this; }
        public Builder audience(String aud) { audiences.add(aud); return this; }
        public Builder jwksUri(URL uri) { this.jwksUri = uri; return this; }
        public Builder jwksUri(String uri) {
            try { this.jwksUri = URI.create(uri).toURL(); return this; }
            catch (Exception e) { throw new IllegalArgumentException("bad jwksUri: " + uri, e); }
        }
        public Builder principalClaim(String c) { this.principalClaim = c; return this; }
        public Builder domain(String d) { this.domain = d; return this; }
        public Builder cacheTtlSeconds(long seconds) { this.cacheTtlSeconds = seconds; return this; }
        public Builder allowedAlgorithms(Set<JWSAlgorithm> algs) { this.allowedAlgorithms = algs; return this; }
        public Builder requireClaim(String name) {
            Set<String> n = new HashSet<>(requiredClaims); n.add(name); this.requiredClaims = n; return this;
        }

        public JwtAuthenticator build() {
            if (issuers.isEmpty()) throw new IllegalStateException("issuer required");
            if (audiences.isEmpty()) throw new IllegalStateException("audience required");
            return new JwtAuthenticator(this);
        }
    }

    // Unused, kept for future header access if needed.
    @SuppressWarnings("unused")
    private static JWSHeader headerFor(JWSAlgorithm a) { return new JWSHeader(a); }
}
