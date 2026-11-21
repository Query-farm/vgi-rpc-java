// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http.auth;

import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.Payload;
import com.nimbusds.jose.jwk.source.JWKSource;
import com.nimbusds.jose.jwk.source.JWKSourceBuilder;
import com.nimbusds.jose.proc.BadJOSEException;
import com.nimbusds.jose.proc.JOSEObjectTypeVerifier;
import com.nimbusds.jose.proc.JWSKeySelector;
import com.nimbusds.jose.proc.JWSVerificationKeySelector;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.JWTParser;
import com.nimbusds.jwt.proc.BadJWTException;
import com.nimbusds.jwt.proc.ConfigurableJWTProcessor;
import com.nimbusds.jwt.proc.DefaultJWTClaimsVerifier;
import com.nimbusds.jwt.proc.DefaultJWTProcessor;
import farm.query.vgirpc.AuthContext;
import farm.query.vgirpc.http.AuthException;
import farm.query.vgirpc.http.AuthFailure;
import farm.query.vgirpc.http.AuthReason;
import farm.query.vgirpc.http.AuthUnavailableException;
import farm.query.vgirpc.http.Authenticator;
import farm.query.vgirpc.http.HttpHeaders;
import farm.query.vgirpc.http.InvalidCredentials;
import farm.query.vgirpc.http.MissingCredentials;
import jakarta.servlet.http.HttpServletRequest;

import java.net.URI;
import java.net.URL;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;

/**
 * Bearer-token authenticator that validates JWTs against a remote JWKS
 * endpoint. Backed by {@code nimbus-jose-jwt}; matches the Python
 * {@code jwt_authenticate} factory: multiple issuers/audiences accepted,
 * automatic JWKS refresh on unknown {@code kid}, principal pulled from a
 * configurable non-blank string claim (default {@code sub}).
 *
 * <p>The hardening options are opt-in, so existing deployments behave as before:
 * an algorithm allow-list ({@code none} and {@code HS*} are always refused), a required JOSE
 * {@code typ} (e.g. {@code at+jwt} for RFC 9068 access tokens), a single-valued {@code aud},
 * a clock-skew leeway, and a JWKS cache that fails closed ({@link Builder#jwksMaxAgeSeconds},
 * {@link Builder#jwksRefreshMinIntervalSeconds}, {@link Builder#jwksFetchTimeout}). An expired
 * token is rejected with {@link AuthReason#EXPIRED_CREDENTIAL}, any other bad token with
 * {@link AuthReason#INVALID_CREDENTIAL}, and a JWKS that cannot be fetched (with no keys inside
 * their maximum age) with {@link AuthUnavailableException}, which the HTTP server answers 503.
 */
public final class JwtAuthenticator implements Authenticator {

    private static final String DEFAULT_CHALLENGE = "Bearer realm=\"vgi-rpc\"";

    private final ConfigurableJWTProcessor<SecurityContext> processor;
    private final List<String> issuers;
    private final List<String> audiences;
    private final String principalClaim;
    private final String domain;
    private final String challenge;
    private final boolean jsonClaims;

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
        this.challenge = b.challenge;
        this.jsonClaims = b.jsonClaims;

        URL jwksUri = resolveJwksUri(b);
        try {
            JWKSource<SecurityContext> jwkSource;
            if (b.strictJwks()) {
                jwkSource = new StrictJwkSource(jwksUri, b.jwksMaxAgeSeconds, b.jwksRefreshMinIntervalSeconds,
                        b.jwksFetchTimeout != null ? b.jwksFetchTimeout : Duration.ofSeconds(10),
                        b.clock != null ? b.clock : System::nanoTime);
            } else {
                long cacheMs = b.cacheTtlSeconds * 1000L;
                long refreshAheadMs = Math.max(1_000L, cacheMs / 5);
                jwkSource = JWKSourceBuilder.create(jwksUri)
                        .retrying(true)
                        .cache(cacheMs, refreshAheadMs)
                        .build();
            }

            DefaultJWTProcessor<SecurityContext> p = new DefaultJWTProcessor<>();
            Set<JWSAlgorithm> algs = b.allowedAlgorithms.isEmpty()
                    ? Set.of(JWSAlgorithm.RS256, JWSAlgorithm.RS384, JWSAlgorithm.RS512,
                             JWSAlgorithm.ES256, JWSAlgorithm.ES384, JWSAlgorithm.ES512,
                             JWSAlgorithm.PS256, JWSAlgorithm.PS384, JWSAlgorithm.PS512)
                    : b.allowedAlgorithms;
            JWSKeySelector<SecurityContext> ksel = new JWSVerificationKeySelector<>(algs, jwkSource);
            p.setJWSKeySelector(ksel);
            if (b.requiredTyp != null) p.setJWSTypeVerifier(new RequiredTypVerifier(b.requiredTyp));

            Set<String> required = new HashSet<>(Set.of("iss", "aud", "exp"));
            required.addAll(b.requiredClaims);
            IssuerAudienceVerifier verifier =
                    new IssuerAudienceVerifier(issuers, audiences, required, b.singleAudience, b.wallClock);
            if (b.leewaySeconds != null) verifier.setMaxClockSkew(b.leewaySeconds);
            p.setJWTClaimsSetVerifier(verifier);
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
     * @throws AuthFailure with {@link AuthReason#EXPIRED_CREDENTIAL} if the token has expired
     * @throws InvalidCredentials if signature, issuer, audience, a required
     *         claim, or the principal claim fails validation
     * @throws AuthUnavailableException if the JWKS cannot be fetched and no keys within their
     *         maximum age exist (only with {@link Builder#jwksMaxAgeSeconds} and friends)
     */
    public AuthContext validateBearer(String token) throws AuthException {
        if (token == null || token.isEmpty()) {
            throw new MissingCredentials("Missing bearer token", challenge);
        }
        JWTClaimsSet claims;
        Map<String, Object> rawClaims;
        try {
            claims = processor.process(token, null);
            rawClaims = new Payload(JWTParser.parse(token).getParsedParts()[1]).toJSONObject();
        } catch (AuthUnavailableException e) {
            // Not a verdict on the token: the keys could not be fetched. Propagate, so the
            // server answers 503 + Retry-After rather than a 401 that invites re-login.
            throw e;
        } catch (ExpiredJwtException e) {
            throw new AuthFailure(AuthReason.EXPIRED_CREDENTIAL, "Invalid JWT: " + e.getMessage(), challenge);
        } catch (Exception e) {
            throw new InvalidCredentials("Invalid JWT: " + e.getMessage(), challenge);
        }

        Object rawPrincipal = rawClaims == null ? null : rawClaims.get(principalClaim);
        if (!(rawPrincipal instanceof String principal) || principal.isBlank()) {
            throw new InvalidCredentials(
                    "Invalid JWT: principal claim '" + principalClaim
                            + "' must be a non-blank string",
                    challenge);
        }

        Map<String, Object> claimsMap = new LinkedHashMap<>(jsonClaims ? rawClaims : claims.getClaims());
        return new AuthContext(domain, true, principal, claimsMap);
    }

    /**
     * Extract the {@code Bearer} token from the {@code Authorization} header and
     * validate it via {@link #validateBearer(String)}.
     *
     * @param request the inbound HTTP request
     * @return an authenticated {@link AuthContext}
     * @throws MissingCredentials if the {@code Authorization} header is absent
     * @throws InvalidCredentials if the header is not a {@code Bearer} credential, or the token
     *         fails validation
     */
    @Override
    public AuthContext authenticate(HttpServletRequest request) throws AuthException {
        String header = request.getHeader(HttpHeaders.AUTHORIZATION);
        if (header == null || header.isEmpty()) {
            throw new MissingCredentials("Missing Authorization header", challenge);
        }
        if (!header.regionMatches(true, 0, HttpHeaders.BEARER_PREFIX, 0, HttpHeaders.BEARER_PREFIX.length())) {
            throw new InvalidCredentials("Authorization header is not a Bearer credential", challenge);
        }
        return validateBearer(header.substring(HttpHeaders.BEARER_PREFIX.length()).trim());
    }

    /** A token past its {@code exp} (plus leeway): reported as {@code expired_credential}. */
    static final class ExpiredJwtException extends BadJWTException {
        private static final long serialVersionUID = 1L;
        ExpiredJwtException() { super("Expired JWT"); }
    }

    /**
     * Required JOSE {@code typ}: present, compared case-insensitively, with the
     * {@code application/} prefix optional (RFC 7515 §4.1.9, RFC 9068 §2.1).
     */
    static final class RequiredTypVerifier implements JOSEObjectTypeVerifier<SecurityContext> {
        private final String expected;

        RequiredTypVerifier(String required) { this.expected = normalizeTyp(required); }

        @Override
        public void verify(JOSEObjectType type, SecurityContext context) throws BadJOSEException {
            if (type == null || !expected.equals(normalizeTyp(type.getType()))) {
                throw new BadJOSEException("JOSE header 'typ' must be '" + expected + "'");
            }
        }
    }

    static String normalizeTyp(String value) {
        String v = value == null ? "" : value.strip().toLowerCase(Locale.ROOT);
        return v.startsWith("application/") ? v.substring("application/".length()) : v;
    }

    /** Issuer + audience whitelist with at-least-one-of semantics + required-claims. */
    static final class IssuerAudienceVerifier extends DefaultJWTClaimsVerifier<SecurityContext> {
        private final Set<String> issuers;
        private final Set<String> audiences;
        private final boolean singleAudience;
        private final LongSupplier wallClockMillis;

        IssuerAudienceVerifier(List<String> issuers, List<String> audiences, Set<String> required,
                               boolean singleAudience, LongSupplier wallClockMillis) {
            super(null, required);
            this.issuers = new HashSet<>(issuers);
            this.audiences = new HashSet<>(audiences);
            this.singleAudience = singleAudience;
            this.wallClockMillis = wallClockMillis;
        }

        @Override
        protected Date currentTime() {
            return wallClockMillis != null ? new Date(wallClockMillis.getAsLong()) : super.currentTime();
        }

        @Override
        public void verify(JWTClaimsSet claims, SecurityContext ctx) throws BadJWTException {
            // Checked here first so an expired token is told apart from a bad one: the two call
            // for different actions (refresh and retry, versus stop and re-authenticate).
            Date exp = claims.getExpirationTime();
            if (exp != null && currentTime().getTime() - exp.getTime() > getMaxClockSkew() * 1000L) {
                throw new ExpiredJwtException();
            }
            super.verify(claims, ctx);
            String iss = claims.getIssuer();
            if (iss == null || !issuers.contains(iss)) {
                throw new BadJWTException("issuer not accepted: " + iss);
            }
            List<String> aud = claims.getAudience();
            if (aud == null || aud.stream().noneMatch(audiences::contains)) {
                throw new BadJWTException("audience not accepted: " + aud);
            }
            if (singleAudience && aud.size() != 1) {
                throw new BadJWTException("aud must name exactly one audience");
            }
        }
    }

    /**
     * Reject {@code none} and HMAC algorithms: {@code none} is unsigned, and an HMAC algorithm
     * checked against a JWKS lets anyone holding the <em>public</em> key mint tokens.
     */
    private static JWSAlgorithm checkedAlgorithm(JWSAlgorithm alg) {
        if (alg == null || alg.getName() == null || alg.getName().isBlank()) {
            throw new IllegalArgumentException("invalid algorithm name: " + alg);
        }
        String name = alg.getName().strip().toLowerCase(Locale.ROOT);
        if (name.equals("none") || name.startsWith("hs")) {
            throw new IllegalArgumentException("algorithm '" + alg.getName()
                    + "' is not allowed for JWKS-verified tokens");
        }
        return alg;
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
        private String requiredTyp;
        private boolean singleAudience;
        private Integer leewaySeconds;
        private Long jwksMaxAgeSeconds;
        private Long jwksRefreshMinIntervalSeconds;
        private Duration jwksFetchTimeout;
        private String challenge = DEFAULT_CHALLENGE;
        private boolean jsonClaims;
        private LongSupplier clock;
        private LongSupplier wallClock;

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
        /**
         * JWT string claim mapped to {@link AuthContext#principal()} (default
         * {@code "sub"}). Tokens where this claim is absent, null, non-string,
         * or blank are rejected.
         */
        public Builder principalClaim(String c) { this.principalClaim = c; return this; }
        /** Auth domain label recorded on the resulting {@link AuthContext} (default {@code "jwt"}). */
        public Builder domain(String d) { this.domain = d; return this; }
        /**
         * JWKS cache lifetime in seconds (default 300); a refresh-ahead window is derived from it.
         * Ignored once any of the fail-closed JWKS options is set.
         */
        public Builder cacheTtlSeconds(long seconds) { this.cacheTtlSeconds = seconds; return this; }
        /**
         * Restrict accepted JWS signature algorithms; when empty, the common RSA/EC/PS family is allowed.
         *
         * @throws IllegalArgumentException if the set names {@code none} or an HMAC algorithm
         */
        public Builder allowedAlgorithms(Set<JWSAlgorithm> algs) {
            Set<JWSAlgorithm> checked = new LinkedHashSet<>();
            for (JWSAlgorithm a : algs) checked.add(checkedAlgorithm(a));
            this.allowedAlgorithms = Set.copyOf(checked);
            return this;
        }
        /**
         * Restrict accepted JWS signature algorithms by name (e.g. {@code ["ES256"]}).
         *
         * @throws IllegalArgumentException if the collection is empty or names {@code none} or an
         *         HMAC algorithm
         */
        public Builder allowedAlgorithmNames(Collection<String> names) {
            if (names == null || names.isEmpty()) throw new IllegalArgumentException("algorithms must not be empty");
            Set<JWSAlgorithm> algs = new LinkedHashSet<>();
            for (String n : names) {
                if (n == null || n.isBlank()) throw new IllegalArgumentException("invalid algorithm name: " + n);
                algs.add(checkedAlgorithm(JWSAlgorithm.parse(n.strip())));
            }
            this.allowedAlgorithms = Set.copyOf(algs);
            return this;
        }
        /** Require an additional claim to be present (beyond the always-required {@code iss}/{@code aud}/{@code exp}). */
        public Builder requireClaim(String name) {
            Set<String> n = new HashSet<>(requiredClaims); n.add(name); this.requiredClaims = n; return this;
        }
        /**
         * Require the JOSE header {@code typ} to be present and match, case-insensitively and with
         * the {@code application/} prefix optional (e.g. {@code "at+jwt"} for RFC 9068 access tokens).
         *
         * @throws IllegalArgumentException if {@code typ} is blank
         */
        public Builder requiredTyp(String typ) {
            if (typ == null || normalizeTyp(typ).isEmpty()) {
                throw new IllegalArgumentException("requiredTyp must not be empty");
            }
            this.requiredTyp = typ;
            return this;
        }
        /**
         * Require {@code aud} to name exactly one audience. A token naming several is refused
         * even when one of them matches.
         */
        public Builder singleAudience(boolean v) { this.singleAudience = v; return this; }
        /**
         * Clock skew allowed on {@code exp} and {@code nbf}, in seconds (nimbus's default, 60, when unset).
         *
         * @throws IllegalArgumentException if negative
         */
        public Builder leewaySeconds(int seconds) {
            if (seconds < 0) throw new IllegalArgumentException("leewaySeconds must not be negative");
            this.leewaySeconds = seconds;
            return this;
        }
        /**
         * Hard upper bound, in seconds, on how long fetched JWKS keys are used. A shorter
         * {@code Cache-Control: max-age} on the JWKS response wins. When it passes the keys are
         * discarded; if the re-fetch then fails, verification fails with
         * {@link AuthUnavailableException} (HTTP 503) rather than using stale keys.
         *
         * @throws IllegalArgumentException if negative
         */
        public Builder jwksMaxAgeSeconds(long seconds) {
            if (seconds < 0) throw new IllegalArgumentException("jwksMaxAgeSeconds must not be negative");
            this.jwksMaxAgeSeconds = seconds;
            return this;
        }
        /**
         * Minimum seconds between JWKS fetches triggered by an unknown {@code kid}. Within the
         * interval a token whose {@code kid} is unknown is rejected without a fetch.
         *
         * @throws IllegalArgumentException if negative
         */
        public Builder jwksRefreshMinIntervalSeconds(long seconds) {
            if (seconds < 0) throw new IllegalArgumentException("jwksRefreshMinIntervalSeconds must not be negative");
            this.jwksRefreshMinIntervalSeconds = seconds;
            return this;
        }
        /** Timeout for each JWKS request (default 10 s with the fail-closed JWKS options). */
        public Builder jwksFetchTimeout(Duration timeout) {
            if (timeout == null || timeout.isNegative() || timeout.isZero()) {
                throw new IllegalArgumentException("jwksFetchTimeout must be positive");
            }
            this.jwksFetchTimeout = timeout;
            return this;
        }
        /**
         * The {@code WWW-Authenticate} challenge carried by this authenticator's rejections
         * (default {@code Bearer realm="vgi-rpc"}).
         */
        public Builder challenge(String challenge) { this.challenge = challenge; return this; }
        /**
         * Put the token's claims into {@link AuthContext#claims()} exactly as decoded from its
         * JSON payload (numbers stay numbers, so {@code exp} is a {@code Long}, not a
         * {@code Date}), as vgi-rpc-python does. Default {@code false}, which keeps nimbus's
         * typed claims.
         */
        public Builder jsonClaims(boolean v) { this.jsonClaims = v; return this; }
        /** Monotonic nanosecond clock for the fail-closed JWKS cache; tests only. */
        Builder clock(LongSupplier nanos) { this.clock = nanos; return this; }
        /** Wall clock (epoch millis) for {@code exp}/{@code nbf}; tests only. */
        Builder wallClock(LongSupplier millis) { this.wallClock = millis; return this; }

        private boolean strictJwks() {
            return jwksMaxAgeSeconds != null || jwksRefreshMinIntervalSeconds != null || jwksFetchTimeout != null;
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
