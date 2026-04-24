// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http.auth;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.sun.net.httpserver.HttpServer;
import farm.query.vgirpc.AuthContext;
import farm.query.vgirpc.http.AuthException;
import farm.query.vgirpc.http.HttpRequestStub;
import jakarta.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.interfaces.RSAPrivateKey;
import java.time.Instant;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end JWT validation test: spins up an in-process JWKS endpoint and
 * exercises the authenticator with valid, expired, and tampered tokens.
 */
final class JwtAuthenticatorTest {

    private static final String ISSUER = "https://issuer.example";
    private static final String AUDIENCE = "my-api";

    private HttpServer jwksHttp;
    private int jwksPort;
    private RSAKey signingKey;

    @BeforeEach
    void startJwksEndpoint() throws Exception {
        signingKey = new com.nimbusds.jose.jwk.gen.RSAKeyGenerator(2048)
                .keyID("kid-" + UUID.randomUUID())
                .generate();
        RSAKey pub = signingKey.toPublicJWK();
        String jwks = new JWKSet(pub).toString();

        jwksHttp = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        jwksHttp.createContext("/jwks.json", exchange -> {
            byte[] body = jwks.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.getResponseBody().close();
        });
        jwksHttp.start();
        jwksPort = jwksHttp.getAddress().getPort();
    }

    @AfterEach
    void stopJwksEndpoint() {
        if (jwksHttp != null) jwksHttp.stop(0);
    }

    private String issueToken(String issuer, String audience, String subject, long expSeconds) throws Exception {
        JWTClaimsSet claims = new JWTClaimsSet.Builder()
                .issuer(issuer)
                .audience(audience)
                .subject(subject)
                .issueTime(new Date())
                .expirationTime(Date.from(Instant.now().plusSeconds(expSeconds)))
                .claim("role", "admin")
                .build();
        SignedJWT jwt = new SignedJWT(
                new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(signingKey.getKeyID()).build(),
                claims);
        jwt.sign(new RSASSASigner((RSAPrivateKey) signingKey.toPrivateKey()));
        return jwt.serialize();
    }

    private JwtAuthenticator buildAuthenticator() {
        return JwtAuthenticator.builder()
                .issuer(ISSUER)
                .audience(AUDIENCE)
                .jwksUri("http://127.0.0.1:" + jwksPort + "/jwks.json")
                .build();
    }

    private static HttpServletRequest bearer(String token) {
        return token == null
                ? HttpRequestStub.withHeaders(Map.of())
                : HttpRequestStub.withBearer(token);
    }

    @Test
    void valid_token_authenticates() throws Exception {
        String token = issueToken(ISSUER, AUDIENCE, "alice", 300);
        AuthContext ctx = buildAuthenticator().authenticate(bearer(token));
        assertTrue(ctx.authenticated());
        assertEquals("alice", ctx.principal());
        assertEquals("admin", ctx.claims().get("role"));
    }

    @Test
    void missing_header_rejected() {
        assertThrows(AuthException.class, () -> buildAuthenticator().authenticate(bearer(null)));
    }

    @Test
    void wrong_issuer_rejected() throws Exception {
        String token = issueToken("https://other.example", AUDIENCE, "alice", 300);
        assertThrows(AuthException.class, () -> buildAuthenticator().authenticate(bearer(token)));
    }

    @Test
    void wrong_audience_rejected() throws Exception {
        String token = issueToken(ISSUER, "other-api", "alice", 300);
        assertThrows(AuthException.class, () -> buildAuthenticator().authenticate(bearer(token)));
    }

    @Test
    void expired_token_rejected() throws Exception {
        String token = issueToken(ISSUER, AUDIENCE, "alice", -60);
        assertThrows(AuthException.class, () -> buildAuthenticator().authenticate(bearer(token)));
    }

    @Test
    void tampered_signature_rejected() throws Exception {
        String token = issueToken(ISSUER, AUDIENCE, "alice", 300);
        // Flip one character well inside the signature so it definitely decodes differently.
        int idx = token.lastIndexOf('.') + 10;
        char orig = token.charAt(idx);
        char flipped = orig == 'x' ? 'y' : 'x';
        String tampered = token.substring(0, idx) + flipped + token.substring(idx + 1);
        assertThrows(AuthException.class, () -> buildAuthenticator().authenticate(bearer(tampered)));
    }
}
