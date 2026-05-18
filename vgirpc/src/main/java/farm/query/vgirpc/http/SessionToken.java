// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import farm.query.vgirpc.http.auth.Crypto;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * AEAD-sealed {@code VGI-Session} sticky-session token.
 *
 * <p>Wire format (v1):
 * <pre>
 *   base64url(
 *     [1B  version = 1]
 *     [12B ChaCha20-Poly1305 nonce (random)]
 *     [..  ciphertext + Poly1305 tag]
 *          plaintext:
 *            [8B  created_at uint64 LE]
 *            [8B  expires_at uint64 LE]
 *            [1B  server_id_len]
 *            [..  server_id bytes (UTF-8)]
 *            [12B session_id]
 *   )
 * </pre>
 *
 * <p>Tokens are server-minted and server-verified only — the wire shape
 * does not need to match the Python reference byte-for-byte. AAD binds
 * the token to the authenticated principal so it cannot be replayed
 * under a different identity. Failing decryption (wrong key, tampering,
 * cross-principal replay) is indistinguishable from a wrong-server-id
 * mismatch — clients see a uniform {@link farm.query.vgirpc.SessionLostError}.
 */
public record SessionToken(
        String serverId,
        byte[] sessionId,   // exactly SESSION_ID_LEN bytes
        long createdAtSeconds,
        long expiresAtSeconds) {

    public static final int SESSION_ID_LEN = 12;
    private static final byte VERSION = 1;
    private static final int VERSION_LEN = 1;
    private static final byte[] AAD_PREFIX = "vgi_rpc.session.v1\0".getBytes(StandardCharsets.UTF_8);

    public SessionToken {
        if (sessionId == null || sessionId.length != SESSION_ID_LEN) {
            throw new IllegalArgumentException("sessionId must be " + SESSION_ID_LEN + " bytes");
        }
        sessionId = sessionId.clone();
    }

    @Override public byte[] sessionId() { return sessionId.clone(); }

    /** Hex-encoded session id, suitable for access logs / debugging. */
    public String sessionIdHex() {
        StringBuilder sb = new StringBuilder(SESSION_ID_LEN * 2);
        for (byte b : sessionId) sb.append(String.format("%02x", b));
        return sb.toString();
    }

    public byte[] pack(byte[] tokenKey, String principal) {
        byte[] serverIdBytes = serverId.getBytes(StandardCharsets.UTF_8);
        if (serverIdBytes.length > 255) {
            throw new IllegalArgumentException("server_id exceeds 255 bytes");
        }
        int payloadLen = 8 + 8 + 1 + serverIdBytes.length + SESSION_ID_LEN;
        ByteBuffer plain = ByteBuffer.allocate(payloadLen).order(ByteOrder.LITTLE_ENDIAN);
        plain.putLong(createdAtSeconds);
        plain.putLong(expiresAtSeconds);
        plain.put((byte) serverIdBytes.length);
        plain.put(serverIdBytes);
        plain.put(sessionId);
        byte[] sealed = Crypto.chacha20Poly1305Seal(tokenKey, plain.array(), aad(principal));
        byte[] wire = new byte[VERSION_LEN + sealed.length];
        wire[0] = VERSION;
        System.arraycopy(sealed, 0, wire, VERSION_LEN, sealed.length);
        return Base64.getUrlEncoder().withoutPadding().encode(wire);
    }

    public static SessionToken unpack(byte[] b64, byte[] tokenKey, String principal) {
        byte[] raw;
        try {
            raw = Base64.getUrlDecoder().decode(b64);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Malformed session token", e);
        }
        if (raw.length < VERSION_LEN + Crypto.AEAD_NONCE_LEN + Crypto.AEAD_TAG_LEN) {
            throw new IllegalArgumentException("Malformed session token");
        }
        if (raw[0] != VERSION) {
            throw new IllegalArgumentException("Unsupported session token version " + raw[0]);
        }
        byte[] sealed = new byte[raw.length - VERSION_LEN];
        System.arraycopy(raw, VERSION_LEN, sealed, 0, sealed.length);
        byte[] plain;
        try {
            plain = Crypto.chacha20Poly1305Open(tokenKey, sealed, aad(principal));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Session token signature verification failed", e);
        }
        if (plain.length < 8 + 8 + 1 + SESSION_ID_LEN) {
            throw new IllegalArgumentException("Malformed session token");
        }
        ByteBuffer bb = ByteBuffer.wrap(plain).order(ByteOrder.LITTLE_ENDIAN);
        long createdAt = bb.getLong();
        long expiresAt = bb.getLong();
        int sidLen = bb.get() & 0xff;
        // sidLen + the trailing fixed-length session id must fit in what
        // remains; refuse anything that overruns to avoid reading garbage
        // from a tampered-but-decrypts payload (theoretical with AEAD).
        if (sidLen < 0 || sidLen + SESSION_ID_LEN > bb.remaining()) {
            throw new IllegalArgumentException("Malformed session token");
        }
        byte[] sidBytes = new byte[sidLen];
        bb.get(sidBytes);
        byte[] sessionId = new byte[SESSION_ID_LEN];
        bb.get(sessionId);
        return new SessionToken(new String(sidBytes, StandardCharsets.UTF_8),
                sessionId, createdAt, expiresAt);
    }

    static byte[] aad(String principal) {
        String p = principal != null ? principal : "";
        byte[] tail;
        if (p.isEmpty()) {
            tail = new byte[]{0x00, 'a', 'n', 'o', 'n', 'y', 'm', 'o', 'u', 's'};
        } else {
            byte[] pBytes = p.getBytes(StandardCharsets.UTF_8);
            tail = new byte[1 + pBytes.length];
            tail[0] = 0x01;
            System.arraycopy(pBytes, 0, tail, 1, pBytes.length);
        }
        byte[] out = new byte[AAD_PREFIX.length + tail.length];
        System.arraycopy(AAD_PREFIX, 0, out, 0, AAD_PREFIX.length);
        System.arraycopy(tail, 0, out, AAD_PREFIX.length, tail.length);
        return out;
    }
}
