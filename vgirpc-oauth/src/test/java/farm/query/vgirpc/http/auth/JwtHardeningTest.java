// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http.auth;

import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.crypto.ECDSASigner;
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.Curve;
import com.nimbusds.jose.jwk.ECKey;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.ECKeyGenerator;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.sun.net.httpserver.HttpServer;
import farm.query.vgirpc.AuthContext;
import farm.query.vgirpc.http.AuthException;
import farm.query.vgirpc.http.AuthReason;
import farm.query.vgirpc.http.AuthUnavailableException;
import farm.query.vgirpc.http.HttpRequestStub;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The opt-in hardening of {@link JwtAuthenticator}, mirroring vgi-rpc-python's
 * {@code tests/test_jwt_hardening.py}: algorithm allow-list, required {@code typ}, single
 * audience, required {@code exp} with leeway, JSON claim types, and the fail-closed JWKS cache.
 */
final class JwtHardeningTest {

    private static final String ISSUER = "https://auth.orchard.test";
    private static final String AUDIENCE = "https://desk.orchard.test/d/dep_123";
    private static final long SEC = 1_000_000_000L;

    private HttpServer jwksHttp;
    private String jwksUri;
    private ECKey ec;
    private RSAKey rsa;
    private final AtomicReference<List<JWK>> published = new AtomicReference<>();
    private final AtomicReference<String> cacheControl = new AtomicReference<>();
    private final AtomicInteger status = new AtomicInteger(200);
    private final AtomicInteger fetches = new AtomicInteger();
    private final AtomicLong now = new AtomicLong(1_000 * SEC);

    @BeforeEach
    void start() throws Exception {
        ec = new ECKeyGenerator(Curve.P_256).keyID("desk-es256-1").generate();
        rsa = new RSAKeyGenerator(2048).keyID("desk-rs256-1").generate();
        published.set(List.of(ec.toPublicJWK(), rsa.toPublicJWK()));
        jwksHttp = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        jwksHttp.createContext("/jwks", exchange -> {
            fetches.incrementAndGet();
            byte[] body = new JWKSet(published.get()).toString().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            if (cacheControl.get() != null) exchange.getResponseHeaders().set("Cache-Control", cacheControl.get());
            exchange.sendResponseHeaders(status.get(), body.length);
            exchange.getResponseBody().write(body);
            exchange.getResponseBody().close();
        });
        jwksHttp.start();
        jwksUri = "http://127.0.0.1:" + jwksHttp.getAddress().getPort() + "/jwks";
    }

    @AfterEach
    void stop() {
        if (jwksHttp != null) jwksHttp.stop(0);
    }

    private JwtAuthenticator hardened() {
        return JwtAuthenticator.builder()
                .issuer(ISSUER).audience(AUDIENCE).jwksUri(jwksUri)
                .allowedAlgorithmNames(List.of("ES256"))
                .requiredTyp("at+jwt")
                .singleAudience(true)
                .leewaySeconds(60)
                .jwksMaxAgeSeconds(600)
                .jwksRefreshMinIntervalSeconds(60)
                .jwksFetchTimeout(Duration.ofSeconds(5))
                .domain("orchard")
                .jsonClaims(true)
                .challenge("Bearer error=\"invalid_token\"")
                .clock(now::get)
                .build();
    }

    private String pass(JWSSigner signer, JWSAlgorithm alg, String kid, String typ,
                        Map<String, Object> overrides) throws Exception {
        long t = System.currentTimeMillis() / 1000;
        JWTClaimsSet.Builder c = new JWTClaimsSet.Builder()
                .issuer(ISSUER).audience(AUDIENCE).subject("user_01M7KNJYC18T")
                .claim("kind", "user").claim("email_verified", true).claim("org_id", "org_01M7")
                .issueTime(new Date(t * 1000)).notBeforeTime(new Date(t * 1000))
                .expirationTime(new Date((t + 900) * 1000));
        overrides.forEach(c::claim);
        JWSHeader.Builder h = new JWSHeader.Builder(alg).keyID(kid);
        if (typ != null) h.type(new JOSEObjectType(typ));
        SignedJWT jwt = new SignedJWT(h.build(), c.build());
        jwt.sign(signer);
        return jwt.serialize();
    }

    private String es256(Map<String, Object> overrides) throws Exception {
        return pass(new ECDSASigner(ec), JWSAlgorithm.ES256, ec.getKeyID(), "at+jwt", overrides);
    }

    private static AuthException reject(JwtAuthenticator a, String token) {
        return assertThrows(AuthException.class, () -> a.authenticate(HttpRequestStub.withBearer(token)));
    }

    @Test
    void valid_pass_carries_json_typed_claims() throws Exception {
        AuthContext ctx = hardened().authenticate(HttpRequestStub.withBearer(es256(Map.of())));
        assertEquals("user_01M7KNJYC18T", ctx.principal());
        assertEquals("orchard", ctx.domain());
        assertEquals(Boolean.TRUE, ctx.claims().get("email_verified"));
        assertInstanceOf(Long.class, ctx.claims().get("exp"));
        assertEquals(AUDIENCE, ctx.claims().get("aud"));
    }

    @Test
    void typ_is_required_case_insensitive_with_optional_prefix() throws Exception {
        JwtAuthenticator a = hardened();
        a.authenticate(HttpRequestStub.withBearer(
                pass(new ECDSASigner(ec), JWSAlgorithm.ES256, ec.getKeyID(), "application/AT+JWT", Map.of())));
        assertEquals(AuthReason.INVALID_CREDENTIAL,
                reject(a, pass(new ECDSASigner(ec), JWSAlgorithm.ES256, ec.getKeyID(), "JWT", Map.of())).reason());
        assertEquals(AuthReason.INVALID_CREDENTIAL,
                reject(a, pass(new ECDSASigner(ec), JWSAlgorithm.ES256, ec.getKeyID(), null, Map.of())).reason());
    }

    @Test
    void only_allowed_algorithms() throws Exception {
        JwtAuthenticator a = hardened();
        reject(a, pass(new RSASSASigner(rsa), JWSAlgorithm.RS256, rsa.getKeyID(), "at+jwt", Map.of()));
        reject(a, pass(new MACSigner(new byte[32]), JWSAlgorithm.HS256, ec.getKeyID(), "at+jwt", Map.of()));
        assertThrows(IllegalArgumentException.class,
                () -> JwtAuthenticator.builder().allowedAlgorithmNames(List.of("ES256", "HS256")));
        assertThrows(IllegalArgumentException.class,
                () -> JwtAuthenticator.builder().allowedAlgorithmNames(List.of("none")));
        assertThrows(IllegalArgumentException.class,
                () -> JwtAuthenticator.builder().allowedAlgorithms(Set.of(JWSAlgorithm.HS512)));
    }

    @Test
    void iss_aud_exp_rules() throws Exception {
        JwtAuthenticator a = hardened();
        long t = System.currentTimeMillis() / 1000;
        reject(a, es256(Map.of("iss", "https://evil.test")));
        reject(a, es256(Map.of("aud", "https://desk.orchard.test/d/other")));
        reject(a, es256(Map.of("aud", List.of(AUDIENCE, "https://desk.orchard.test/d/other"))));
        // One-element list is a single audience.
        a.authenticate(HttpRequestStub.withBearer(es256(Map.of("aud", List.of(AUDIENCE)))));
        assertEquals(AuthReason.EXPIRED_CREDENTIAL, reject(a, es256(Map.of("exp", t - 120))).reason());
        // Within the 60 s leeway.
        a.authenticate(HttpRequestStub.withBearer(es256(Map.of("exp", t - 30))));
        assertEquals(AuthReason.INVALID_CREDENTIAL, reject(a, es256(Map.of("nbf", t + 120))).reason());
    }

    @Test
    void missing_exp_rejected() throws Exception {
        JwtAuthenticator a = hardened();
        JWTClaimsSet c = new JWTClaimsSet.Builder().issuer(ISSUER).audience(AUDIENCE).subject("u").build();
        SignedJWT jwt = new SignedJWT(new JWSHeader.Builder(JWSAlgorithm.ES256).keyID(ec.getKeyID())
                .type(new JOSEObjectType("at+jwt")).build(), c);
        jwt.sign(new ECDSASigner(ec));
        assertEquals(AuthReason.INVALID_CREDENTIAL, reject(a, jwt.serialize()).reason());
    }

    @Test
    void tampered_and_garbage_rejected() throws Exception {
        JwtAuthenticator a = hardened();
        String[] parts = es256(Map.of()).split("\\.");
        String forged = java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(
                ("{\"iss\":\"" + ISSUER + "\",\"aud\":\"" + AUDIENCE + "\",\"sub\":\"mallory\",\"exp\":9999999999}")
                        .getBytes(StandardCharsets.UTF_8));
        reject(a, parts[0] + "." + forged + "." + parts[2]);
        reject(a, "not-a-jwt");
        assertEquals(AuthReason.MISSING_CREDENTIAL,
                assertThrows(AuthException.class, () -> a.authenticate(HttpRequestStub.withHeaders(Map.of()))).reason());
        assertEquals(AuthReason.INVALID_CREDENTIAL, assertThrows(AuthException.class,
                () -> a.authenticate(HttpRequestStub.withHeaders(Map.of("Authorization", "Basic Zm9v")))).reason());
        assertEquals("Bearer error=\"invalid_token\"", reject(a, "not-a-jwt").wwwAuthenticate());
    }

    @Test
    void jwks_expiry_fails_closed() throws Exception {
        JwtAuthenticator a = hardened();
        String token = es256(Map.of());
        a.authenticate(HttpRequestStub.withBearer(token));
        assertEquals(1, fetches.get());
        // Still within max age: no fetch, even with the endpoint down.
        status.set(500);
        now.addAndGet(599 * SEC);
        a.authenticate(HttpRequestStub.withBearer(token));
        assertEquals(1, fetches.get());
        // Past max age with the endpoint failing: 503, never stale keys -- every time.
        now.addAndGet(2 * SEC);
        assertThrows(AuthUnavailableException.class, () -> a.authenticate(HttpRequestStub.withBearer(token)));
        assertThrows(AuthUnavailableException.class, () -> a.authenticate(HttpRequestStub.withBearer(token)));
        // Endpoint back: verification resumes.
        status.set(200);
        a.authenticate(HttpRequestStub.withBearer(token));
    }

    @Test
    void first_fetch_failure_is_unavailable() throws Exception {
        status.set(503);
        JwtAuthenticator a = hardened();
        assertThrows(AuthUnavailableException.class,
                () -> a.authenticate(HttpRequestStub.withBearer(es256(Map.of()))));
    }

    @Test
    void cache_control_max_age_shortens_the_cap() throws Exception {
        cacheControl.set("public, max-age=30");
        JwtAuthenticator a = hardened();
        String token = es256(Map.of());
        a.authenticate(HttpRequestStub.withBearer(token));
        now.addAndGet(29 * SEC);
        a.authenticate(HttpRequestStub.withBearer(token));
        assertEquals(1, fetches.get());
        now.addAndGet(2 * SEC);
        a.authenticate(HttpRequestStub.withBearer(token));
        assertEquals(2, fetches.get());
        // A longer advertised max-age never extends the 600 s cap.
        cacheControl.set("max-age=86400");
        now.addAndGet(31 * SEC);
        a.authenticate(HttpRequestStub.withBearer(token));
        assertEquals(3, fetches.get());
        now.addAndGet(601 * SEC);
        a.authenticate(HttpRequestStub.withBearer(token));
        assertEquals(4, fetches.get());
    }

    @Test
    void unknown_kid_refetch_is_rate_limited() throws Exception {
        JwtAuthenticator a = hardened();
        a.authenticate(HttpRequestStub.withBearer(es256(Map.of())));
        assertEquals(1, fetches.get());
        ECKey rotated = new ECKeyGenerator(Curve.P_256).keyID("desk-es256-2").generate();
        String rotatedPass = pass(new ECDSASigner(rotated), JWSAlgorithm.ES256, rotated.getKeyID(), "at+jwt", Map.of());
        List<JWK> keys = new ArrayList<>(published.get());
        keys.add(rotated.toPublicJWK());
        published.set(keys);
        // Within 60 s of the last fetch: rejected without a fetch.
        now.addAndGet(10 * SEC);
        assertEquals(AuthReason.INVALID_CREDENTIAL, reject(a, rotatedPass).reason());
        assertEquals(1, fetches.get());
        // After the interval: one re-fetch, and the rotated key verifies.
        now.addAndGet(51 * SEC);
        a.authenticate(HttpRequestStub.withBearer(rotatedPass));
        assertEquals(2, fetches.get());
        // An unknown kid right after is rejected without another fetch.
        ECKey stranger = new ECKeyGenerator(Curve.P_256).keyID("stranger").generate();
        reject(a, pass(new ECDSASigner(stranger), JWSAlgorithm.ES256, "stranger", "at+jwt", Map.of()));
        assertEquals(2, fetches.get());
    }

    @Test
    void cache_control_parsing() {
        assertEquals(30L, StrictJwkSource.cacheControlMaxAge("public, max-age=30"));
        assertEquals(30L, StrictJwkSource.cacheControlMaxAge("max-age=\"30\", must-revalidate"));
        assertEquals(null, StrictJwkSource.cacheControlMaxAge("s-maxage=30"));
        assertEquals(null, StrictJwkSource.cacheControlMaxAge(null));
    }
}
