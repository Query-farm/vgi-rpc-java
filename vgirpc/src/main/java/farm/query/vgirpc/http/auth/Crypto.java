// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http.auth;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Objects;

/**
 * Shared HMAC-SHA256 + constant-time-equality primitives used by
 * {@link SignedCookie}, {@link Pkce}, and {@code StateToken}. Kept in one place
 * so there is a single copy of "JVM broken — SHA-256 missing" handling.
 */
public final class Crypto {

    private static final String HMAC_ALG = "HmacSHA256";

    private Crypto() {}

    /** HMAC-SHA256 of {@code data} keyed by {@code key}. */
    public static byte[] hmacSha256(byte[] key, byte[] data) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(data, "data");
        try {
            Mac mac = Mac.getInstance(HMAC_ALG);
            mac.init(new SecretKeySpec(key, HMAC_ALG));
            return mac.doFinal(data);
        } catch (Exception e) {
            throw new IllegalStateException("HMAC-SHA256 unavailable — JVM broken", e);
        }
    }

    /** Constant-time equality — delegates to {@link MessageDigest#isEqual}. */
    public static boolean constantTimeEquals(byte[] a, byte[] b) {
        return MessageDigest.isEqual(a, b);
    }

    /**
     * Derive a per-principal key for HTTP stream-state tokens so that a token
     * minted for principal A cannot be presented by principal B. Uses HMAC-SHA256
     * as a KDF (NIST SP 800-108) with a fixed domain-separation label to prevent
     * cross-use with other HMACs that happen to share the signing key.
     *
     * @param signingKey base HMAC key; same across principals.
     * @param principal  authenticated principal; {@code null} is treated as {@code ""}
     *                   (anonymous). Bytes are UTF-8.
     */
    public static byte[] deriveStateTokenKey(byte[] signingKey, String principal) {
        String label = "vgi-rpc/state-token/v1\0";
        String p = principal != null ? principal : "";
        return hmacSha256(signingKey, (label + p).getBytes(StandardCharsets.UTF_8));
    }
}
