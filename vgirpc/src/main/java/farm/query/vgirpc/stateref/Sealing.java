// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.stateref;

import farm.query.vgirpc.http.auth.Crypto;

/**
 * The versioned AEAD envelope the {@link StateRef} tags, stored objects and argument
 * references use: {@code 0x01 || nonce || ciphertext+tag} (ChaCha20-Poly1305 under the
 * server's token key).
 *
 * <p>The reference uses a 24-byte XChaCha nonce; this SDK's {@link Crypto} is
 * ChaCha20-Poly1305 with a 12-byte random nonce, as its cursor tokens already are. No
 * cross-SDK compatibility is required: every envelope is minted and opened by the same
 * server (STATE_REF_V1.md §7).</p>
 */
final class Sealing {

    static final byte VERSION = 0x01;

    private Sealing() {}

    /** A sealed value that does not open (wrong key, AAD, version or tampering). */
    static final class SealException extends Exception {
        private static final long serialVersionUID = 1L;

        SealException() {
            super("sealed value does not open");
        }
    }

    static byte[] seal(byte[] plaintext, byte[] key, byte[] aad) {
        byte[] sealed = Crypto.chacha20Poly1305Seal(key, plaintext, aad);
        byte[] out = new byte[1 + sealed.length];
        out[0] = VERSION;
        System.arraycopy(sealed, 0, out, 1, sealed.length);
        return out;
    }

    static byte[] open(byte[] envelope, byte[] key, byte[] aad) throws SealException {
        if (envelope == null || envelope.length < 1 + Crypto.AEAD_NONCE_LEN + Crypto.AEAD_TAG_LEN
                || envelope[0] != VERSION) {
            throw new SealException();
        }
        byte[] sealed = new byte[envelope.length - 1];
        System.arraycopy(envelope, 1, sealed, 0, sealed.length);
        try {
            return Crypto.chacha20Poly1305Open(key, sealed, aad);
        } catch (IllegalArgumentException | IllegalStateException e) {
            throw new SealException();
        }
    }

    static byte[] concat(byte[]... parts) {
        int n = 0;
        for (byte[] p : parts) n += p.length;
        byte[] out = new byte[n];
        int at = 0;
        for (byte[] p : parts) {
            System.arraycopy(p, 0, out, at, p.length);
            at += p.length;
        }
        return out;
    }
}
