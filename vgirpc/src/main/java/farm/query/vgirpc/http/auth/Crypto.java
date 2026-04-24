// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http.auth;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
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
}
