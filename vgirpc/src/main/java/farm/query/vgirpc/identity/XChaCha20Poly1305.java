// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.identity;

import javax.crypto.AEADBadTagException;
import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.util.Arrays;

/**
 * XChaCha20-Poly1305 (libsodium's {@code crypto_aead_xchacha20poly1305_ietf}), built from the
 * JDK's IETF ChaCha20-Poly1305 plus an HChaCha20 subkey derivation.
 *
 * <p>The sealed-grant envelope (IDENTITY_V1_SPEC.md §9) is the reference's stream-state envelope:
 * {@code version || nonce(24) || ciphertext || tag(16)} under XChaCha20-Poly1305. The JDK ships
 * only the 12-byte-nonce IETF construction, so this derives the subkey with HChaCha20 from the
 * key and the first 16 nonce bytes, and seals under the IETF cipher with nonce
 * {@code 0x00000000 || nonce[16..24]} -- exactly libsodium's definition, which is what makes a
 * grant minted here open in every other port and the reverse.
 */
final class XChaCha20Poly1305 {

    static final int KEY_LEN = 32;
    static final int NONCE_LEN = 24;
    static final int TAG_LEN = 16;

    private XChaCha20Poly1305() {}

    /**
     * Seal {@code plaintext}.
     *
     * @return {@code ciphertext || tag}
     */
    static byte[] seal(byte[] key, byte[] nonce, byte[] plaintext, byte[] aad) {
        return run(Cipher.ENCRYPT_MODE, key, nonce, plaintext, aad);
    }

    /**
     * Open {@code ciphertext || tag}.
     *
     * @throws AEADBadTagException when the tag does not verify (wrong key, AAD, or tampering)
     */
    static byte[] open(byte[] key, byte[] nonce, byte[] sealed, byte[] aad) throws AEADBadTagException {
        if (sealed.length < TAG_LEN) throw new AEADBadTagException("sealed body is shorter than a tag");
        try {
            return run(Cipher.DECRYPT_MODE, key, nonce, sealed, aad);
        } catch (IllegalStateException e) {
            if (e.getCause() instanceof AEADBadTagException bad) throw bad;
            throw e;
        }
    }

    private static byte[] run(int mode, byte[] key, byte[] nonce, byte[] input, byte[] aad) {
        if (key.length != KEY_LEN) throw new IllegalArgumentException("XChaCha20-Poly1305 key must be 32 bytes");
        if (nonce.length != NONCE_LEN) throw new IllegalArgumentException("XChaCha20-Poly1305 nonce must be 24 bytes");
        byte[] subkey = hchacha20(key, Arrays.copyOfRange(nonce, 0, 16));
        byte[] ietfNonce = new byte[12];
        System.arraycopy(nonce, 16, ietfNonce, 4, 8);
        try {
            Cipher cipher = Cipher.getInstance("ChaCha20-Poly1305");
            cipher.init(mode, new SecretKeySpec(subkey, "ChaCha20"), new IvParameterSpec(ietfNonce));
            cipher.updateAAD(aad);
            return cipher.doFinal(input);
        } catch (AEADBadTagException e) {
            throw new IllegalStateException(e);
        } catch (java.security.GeneralSecurityException e) {
            throw new IllegalStateException("ChaCha20-Poly1305 unavailable", e);
        } finally {
            Arrays.fill(subkey, (byte) 0);
        }
    }

    /** HChaCha20: the ChaCha20 core over (constants, key, 16-byte nonce), no final addition. */
    static byte[] hchacha20(byte[] key, byte[] nonce16) {
        int[] s = new int[16];
        s[0] = 0x61707865;
        s[1] = 0x3320646e;
        s[2] = 0x79622d32;
        s[3] = 0x6b206574;
        for (int i = 0; i < 8; i++) s[4 + i] = le32(key, i * 4);
        for (int i = 0; i < 4; i++) s[12 + i] = le32(nonce16, i * 4);
        for (int round = 0; round < 10; round++) {
            quarter(s, 0, 4, 8, 12);
            quarter(s, 1, 5, 9, 13);
            quarter(s, 2, 6, 10, 14);
            quarter(s, 3, 7, 11, 15);
            quarter(s, 0, 5, 10, 15);
            quarter(s, 1, 6, 11, 12);
            quarter(s, 2, 7, 8, 13);
            quarter(s, 3, 4, 9, 14);
        }
        byte[] out = new byte[32];
        for (int i = 0; i < 4; i++) putLe32(out, i * 4, s[i]);
        for (int i = 0; i < 4; i++) putLe32(out, 16 + i * 4, s[12 + i]);
        return out;
    }

    private static void quarter(int[] s, int a, int b, int c, int d) {
        s[a] += s[b]; s[d] = Integer.rotateLeft(s[d] ^ s[a], 16);
        s[c] += s[d]; s[b] = Integer.rotateLeft(s[b] ^ s[c], 12);
        s[a] += s[b]; s[d] = Integer.rotateLeft(s[d] ^ s[a], 8);
        s[c] += s[d]; s[b] = Integer.rotateLeft(s[b] ^ s[c], 7);
    }

    private static int le32(byte[] b, int off) {
        return (b[off] & 0xff) | (b[off + 1] & 0xff) << 8 | (b[off + 2] & 0xff) << 16 | (b[off + 3] & 0xff) << 24;
    }

    private static void putLe32(byte[] b, int off, int v) {
        b[off] = (byte) v;
        b[off + 1] = (byte) (v >>> 8);
        b[off + 2] = (byte) (v >>> 16);
        b[off + 3] = (byte) (v >>> 24);
    }
}
