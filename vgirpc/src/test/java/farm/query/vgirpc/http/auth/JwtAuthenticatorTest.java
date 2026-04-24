// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http.auth;

import com.nimbusds.jose.JOSEException;
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
import jakarta.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
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

    private JwtAuthenticator buildAuthenticator(String issuer, String audience) {
        return JwtAuthenticator.builder()
                .issuer(issuer)
                .audience(audience)
                .jwksUri("http://127.0.0.1:" + jwksPort + "/jwks.json")
                .build();
    }

    private static HttpServletRequest stub(String authHeader) {
        Map<String, String> headers = new HashMap<>();
        if (authHeader != null) headers.put("Authorization", authHeader);
        return (HttpServletRequest) java.lang.reflect.Proxy.newProxyInstance(
                JwtAuthenticatorTest.class.getClassLoader(),
                new Class<?>[]{HttpServletRequest.class},
                (proxy, method, args) -> {
                    if ("getHeader".equals(method.getName()) && args != null && args.length == 1) {
                        String key = (String) args[0];
                        for (Map.Entry<String, String> e : headers.entrySet()) {
                            if (e.getKey().equalsIgnoreCase(key)) return e.getValue();
                        }
                        return null;
                    }
                    return switch (method.getName()) {
                        case "hashCode" -> 0;
                        case "toString" -> "stub";
                        case "equals" -> proxy == args[0];
                        default -> null;
                    };
                });
    }

    @Test
    void valid_token_authenticates() throws Exception {
        String token = issueToken("https://issuer.example", "my-api", "alice", 300);
        AuthContext ctx = buildAuthenticator("https://issuer.example", "my-api")
                .authenticate(stub("Bearer " + token));
        assertTrue(ctx.authenticated());
        assertEquals("alice", ctx.principal());
        assertEquals("admin", ctx.claims().get("role"));
    }

    @Test
    void missing_header_rejected() {
        assertThrows(AuthException.class,
                () -> buildAuthenticator("https://issuer.example", "my-api").authenticate(stub(null)));
    }

    @Test
    void wrong_issuer_rejected() throws Exception {
        String token = issueToken("https://other.example", "my-api", "alice", 300);
        assertThrows(AuthException.class, () -> buildAuthenticator("https://issuer.example", "my-api")
                .authenticate(stub("Bearer " + token)));
    }

    @Test
    void wrong_audience_rejected() throws Exception {
        String token = issueToken("https://issuer.example", "other-api", "alice", 300);
        assertThrows(AuthException.class, () -> buildAuthenticator("https://issuer.example", "my-api")
                .authenticate(stub("Bearer " + token)));
    }

    @Test
    void expired_token_rejected() throws Exception {
        String token = issueToken("https://issuer.example", "my-api", "alice", -60);
        assertThrows(AuthException.class, () -> buildAuthenticator("https://issuer.example", "my-api")
                .authenticate(stub("Bearer " + token)));
    }

    @Test
    void tampered_signature_rejected() throws Exception {
        String token = issueToken("https://issuer.example", "my-api", "alice", 300);
        // Tamper inside the signature (last segment) by flipping one character
        // well inside the signature to something that definitely decodes
        // differently — cheap and reliable.
        int lastDot = token.lastIndexOf('.');
        int idx = lastDot + 10; // well inside the signature
        char orig = token.charAt(idx);
        char flipped = orig == 'x' ? 'y' : 'x';
        String tampered = token.substring(0, idx) + flipped + token.substring(idx + 1);
        assertThrows(AuthException.class, () -> buildAuthenticator("https://issuer.example", "my-api")
                .authenticate(stub("Bearer " + tampered)));
    }
}
