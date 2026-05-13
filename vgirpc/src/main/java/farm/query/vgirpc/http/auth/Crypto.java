// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http.auth;

import javax.crypto.AEADBadTagException;
import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Objects;

/**
 * Shared crypto primitives. HMAC-SHA256 + constant-time-equality remain in
 * use by {@link SignedCookie} and {@link Pkce}; ChaCha20-Poly1305 AEAD seals
 * HTTP stream-state tokens.  All "JVM broken" handling is centralised here
 * so callers only have to think about IllegalArgumentException on tampered
 * input vs. IllegalStateException on a misconfigured runtime.
 */
public final class Crypto {

    private static final String HMAC_ALG = "HmacSHA256";
    private static final String AEAD_ALG = "ChaCha20-Poly1305";
    private static final String CHACHA_KEY_ALG = "ChaCha20";

    /** JDK ChaCha20-Poly1305 requires a 12-byte nonce. */
    public static final int AEAD_NONCE_LEN = 12;
    /** Poly1305 tag length appended by the AEAD construction. */
    public static final int AEAD_TAG_LEN = 16;

    private static final SecureRandom RNG = new SecureRandom();

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
     * Seal {@code plaintext} with ChaCha20-Poly1305 under {@code key} and
     * {@code aad}.  Generates a fresh random 12-byte nonce; the returned
     * blob is {@code nonce || ciphertext_with_tag}.
     *
     * @throws IllegalArgumentException if the key length is not 32.
     * @throws IllegalStateException if the JDK lacks ChaCha20-Poly1305.
     */
    public static byte[] chacha20Poly1305Seal(byte[] key, byte[] plaintext, byte[] aad) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(plaintext, "plaintext");
        Objects.requireNonNull(aad, "aad");
        if (key.length != 32) {
            throw new IllegalArgumentException("ChaCha20-Poly1305 key must be 32 bytes");
        }
        byte[] nonce = new byte[AEAD_NONCE_LEN];
        RNG.nextBytes(nonce);
        try {
            Cipher cipher = Cipher.getInstance(AEAD_ALG);
            cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(key, CHACHA_KEY_ALG),
                    new IvParameterSpec(nonce));
            cipher.updateAAD(aad);
            byte[] ciphertext = cipher.doFinal(plaintext);
            byte[] out = new byte[nonce.length + ciphertext.length];
            System.arraycopy(nonce, 0, out, 0, nonce.length);
            System.arraycopy(ciphertext, 0, out, nonce.length, ciphertext.length);
            return out;
        } catch (Exception e) {
            throw new IllegalStateException("ChaCha20-Poly1305 unavailable — JVM broken", e);
        }
    }

    /**
     * Open a {@code nonce || ciphertext_with_tag} blob produced by
     * {@link #chacha20Poly1305Seal}.  AAD must match exactly.
     *
     * @throws IllegalArgumentException on any authenticity failure (bad
     *         tag, wrong key, wrong AAD, truncated/malformed blob).
     * @throws IllegalStateException if the JDK lacks ChaCha20-Poly1305.
     */
    public static byte[] chacha20Poly1305Open(byte[] key, byte[] sealed, byte[] aad) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(sealed, "sealed");
        Objects.requireNonNull(aad, "aad");
        if (key.length != 32) {
            throw new IllegalArgumentException("ChaCha20-Poly1305 key must be 32 bytes");
        }
        if (sealed.length < AEAD_NONCE_LEN + AEAD_TAG_LEN) {
            throw new IllegalArgumentException("Malformed sealed payload");
        }
        byte[] nonce = new byte[AEAD_NONCE_LEN];
        System.arraycopy(sealed, 0, nonce, 0, AEAD_NONCE_LEN);
        byte[] ciphertext = new byte[sealed.length - AEAD_NONCE_LEN];
        System.arraycopy(sealed, AEAD_NONCE_LEN, ciphertext, 0, ciphertext.length);
        try {
            Cipher cipher = Cipher.getInstance(AEAD_ALG);
            cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(key, CHACHA_KEY_ALG),
                    new IvParameterSpec(nonce));
            cipher.updateAAD(aad);
            return cipher.doFinal(ciphertext);
        } catch (AEADBadTagException e) {
            throw new IllegalArgumentException("Authentication failed", e);
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("ChaCha20-Poly1305 unavailable — JVM broken", e);
        }
    }
}
