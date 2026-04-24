// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http.auth;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;

/**
 * RFC 7636 PKCE helpers: code verifier + S256 code challenge + state nonce.
 * All values are URL-safe base64 without padding, matching what OIDC
 * authorization servers expect.
 */
public final class Pkce {

    private static final SecureRandom RNG = new SecureRandom();

    private Pkce() {}

    /** 32 random bytes → 43 URL-safe base64 chars without padding. */
    public static String generateCodeVerifier() {
        byte[] bytes = new byte[32];
        RNG.nextBytes(bytes);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }

    /** SHA-256 of the verifier, URL-safe base64 without padding. */
    public static String codeChallengeS256(String codeVerifier) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(codeVerifier.getBytes(StandardCharsets.US_ASCII));
            return Base64.getUrlEncoder().withoutPadding().encodeToString(digest);
        } catch (Exception e) {
            throw new RuntimeException("SHA-256 unavailable — JVM broken", e);
        }
    }

    /** 24 random bytes → URL-safe base64 state nonce for CSRF protection. */
    public static String generateStateNonce() {
        byte[] bytes = new byte[24];
        RNG.nextBytes(bytes);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }
}
