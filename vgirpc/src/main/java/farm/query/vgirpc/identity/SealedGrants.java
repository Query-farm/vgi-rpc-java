// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.identity;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Sealed grants: the framework's own {@code issue_grant} credential, and its verifier
 * (IDENTITY_V1_SPEC.md §9.1).
 *
 * <p>{@code issue_grant} mints a standing delegation that unattended automation later presents
 * <em>as an ordinary bearer</em>, and until sealed grants nothing accepted one. With grant keys
 * configured the framework mints grants itself (unless the worker supplies its own
 * {@code mintGrant}) and accepts them back; without keys nothing changes.
 *
 * <pre>
 * token    = "vgig1." base64url_nopad( kid(8) || envelope )
 * envelope = 0x01 || nonce(24) || XChaCha20-Poly1305(payload, aad)      ; ciphertext || tag(16)
 * kid      = SHA-256("vgi_rpc.grant.kid.v1" 0x00 || key)[0:8]
 * aad      = "vgi_rpc.grant.v1" 0x00 || kid || UTF-8(audience)
 * payload  = issued_at i64 LE || expires_at i64 LE || grant_id || principal || purpose
 *            || scope_count u16 LE || scopes        ; each string: u16 LE length || UTF-8
 * </pre>
 *
 * <p>Every port mints and verifies byte-identically; the reference's
 * {@code grant_token_vectors.json} pins it. <strong>Not individually revocable:</strong> a sealed
 * grant is valid until it expires; the levers are a short maximum lifetime and removing a key,
 * which revokes every grant it minted.
 */
public final class SealedGrants {

    /** Token prefix. The format version is in it, so an incompatible format routes elsewhere. */
    public static final String TOKEN_PREFIX = "vgig1.";

    /** Longest token text considered at all -- the cap {@code introspect_token} applies. */
    public static final int MAX_TOKEN_CHARS = 4096;

    private static final int KID_LEN = 8;
    private static final byte ENVELOPE_VERSION = 0x01;
    private static final int MAX_FIELD = 0xFFFF;
    private static final byte[] KID_DOMAIN = "vgi_rpc.grant.kid.v1\0".getBytes(StandardCharsets.US_ASCII);
    private static final Pattern B64URL = Pattern.compile("[A-Za-z0-9_-]+");
    private static final SecureRandom RNG = new SecureRandom();

    private SealedGrants() {}

    /**
     * The 8-byte key id a token names its sealing key with.
     *
     * @param key a 32-byte grant key
     * @return {@code SHA-256("vgi_rpc.grant.kid.v1\0" || key)[0:8]}
     */
    public static byte[] keyId(byte[] key) {
        try {
            MessageDigest sha = MessageDigest.getInstance("SHA-256");
            sha.update(KID_DOMAIN);
            sha.update(key);
            byte[] digest = sha.digest();
            byte[] out = new byte[KID_LEN];
            System.arraycopy(digest, 0, out, 0, KID_LEN);
            return out;
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    /** A minted token and the claims it carries. */
    public record Minted(String token, GrantClaims claims) {}

    /**
     * Mint a sealed grant with the first configured key.
     *
     * @param keys the deployment's grant configuration
     * @param principal the caller the grant is for
     * @param scopes what it may do
     * @param purpose why it is being minted
     * @param ttlSeconds requested lifetime; capped at {@link GrantKeys#maxTtlSeconds()}
     * @return the token and its claims
     * @throws IllegalArgumentException a non-positive lifetime, or a field too long to encode
     */
    public static Minted mint(GrantKeys keys, String principal, List<String> scopes, String purpose,
                              long ttlSeconds) {
        byte[] id = new byte[16];
        RNG.nextBytes(id);
        byte[] nonce = new byte[XChaCha20Poly1305.NONCE_LEN];
        RNG.nextBytes(nonce);
        return mint(keys, principal, scopes, purpose, ttlSeconds,
                System.currentTimeMillis() / 1000L, HexFormat.of().formatHex(id), nonce);
    }

    /**
     * Mint with a fixed clock, grant id and nonce. <strong>For test vectors only</strong>:
     * reusing a nonce under one key destroys XChaCha20-Poly1305's confidentiality and
     * authenticity.
     *
     * @param keys the deployment's grant configuration
     * @param principal the caller the grant is for
     * @param scopes what it may do
     * @param purpose why it is being minted
     * @param ttlSeconds requested lifetime; capped at {@link GrantKeys#maxTtlSeconds()}
     * @param now issued_at, in seconds
     * @param grantId the grant id
     * @param nonce a 24-byte nonce
     * @return the token and its claims
     */
    public static Minted mint(GrantKeys keys, String principal, List<String> scopes, String purpose,
                              long ttlSeconds, long now, String grantId, byte[] nonce) {
        if (ttlSeconds <= 0) throw new IllegalArgumentException("ttl_seconds must be positive");
        GrantClaims claims = new GrantClaims(principal, scopes, purpose, grantId, now,
                now + Math.min(ttlSeconds, keys.maxTtlSeconds()));
        byte[] kid = keys.mintingKid();
        byte[] sealed = XChaCha20Poly1305.seal(keys.mintingKey(), nonce, encodePayload(claims), keys.aad(kid));
        byte[] raw = new byte[KID_LEN + 1 + nonce.length + sealed.length];
        System.arraycopy(kid, 0, raw, 0, KID_LEN);
        raw[KID_LEN] = ENVELOPE_VERSION;
        System.arraycopy(nonce, 0, raw, KID_LEN + 1, nonce.length);
        System.arraycopy(sealed, 0, raw, KID_LEN + 1 + nonce.length, sealed.length);
        return new Minted(TOKEN_PREFIX + b64url(raw), claims);
    }

    /**
     * Verify a sealed grant at the current time.
     *
     * @param keys the deployment's grant configuration
     * @param token the bearer credential, exactly as presented
     * @return the verified claims
     * @throws GrantInvalidException for every cause
     */
    public static GrantClaims verify(GrantKeys keys, String token) {
        return verify(keys, token, System.currentTimeMillis() / 1000.0);
    }

    /**
     * Verify a sealed grant. The order is normative: prefix, length, base64url, key id, AEAD
     * open, payload, then lifetime -- the lifetime is inside the ciphertext, so it is trusted
     * only once the tag verified.
     *
     * @param keys the deployment's grant configuration
     * @param token the bearer credential, exactly as presented
     * @param now the clock, in seconds since the Unix epoch
     * @return the verified claims
     * @throws GrantInvalidException for every cause; {@link GrantInvalidException#expired()} only
     *     for an authentic grant outside its lifetime
     */
    public static GrantClaims verify(GrantKeys keys, String token, double now) {
        if (token == null || !token.startsWith(TOKEN_PREFIX)) throw new GrantInvalidException("not a sealed grant");
        if (token.length() > MAX_TOKEN_CHARS) throw new GrantInvalidException("grant token is too long");
        byte[] raw = b64urlStrict(token.substring(TOKEN_PREFIX.length()));
        if (raw.length < KID_LEN + 1 + XChaCha20Poly1305.NONCE_LEN + XChaCha20Poly1305.TAG_LEN) {
            throw new GrantInvalidException("grant failed verification");
        }
        byte[] kid = java.util.Arrays.copyOfRange(raw, 0, KID_LEN);
        byte[] key = keys.keyFor(kid);
        if (key == null) {
            throw new GrantInvalidException("grant was sealed with a key this deployment does not hold");
        }
        if (raw[KID_LEN] != ENVELOPE_VERSION) throw new GrantInvalidException("grant failed verification");
        byte[] nonce = java.util.Arrays.copyOfRange(raw, KID_LEN + 1, KID_LEN + 1 + XChaCha20Poly1305.NONCE_LEN);
        byte[] body = java.util.Arrays.copyOfRange(raw, KID_LEN + 1 + XChaCha20Poly1305.NONCE_LEN, raw.length);
        byte[] payload;
        try {
            payload = XChaCha20Poly1305.open(key, nonce, body, keys.aad(kid));
        } catch (javax.crypto.AEADBadTagException e) {
            throw new GrantInvalidException("grant failed verification");
        }
        GrantClaims claims = decodePayload(payload);
        if (claims.principal().isEmpty()) throw new GrantInvalidException("grant names no principal");
        if (claims.expiresAt() <= claims.issuedAt()
                || claims.expiresAt() - claims.issuedAt() > keys.maxTtlSeconds()) {
            throw new GrantInvalidException("grant lifetime exceeds this deployment's maximum");
        }
        long skew = keys.clockSkewSeconds();
        if (claims.issuedAt() > now + skew) throw new GrantInvalidException("grant is not yet valid", true);
        if (now >= claims.expiresAt() + skew) throw new GrantInvalidException("grant has expired", true);
        return claims;
    }

    /**
     * A {@code mintGrant} hook that issues sealed grants -- what {@link IdentityImpl} installs
     * when grant keys are configured and the worker supplied no hook of its own.
     *
     * @param keys the deployment's grant configuration
     * @return the hook
     */
    public static GrantMintHook minter(GrantKeys keys) {
        return (principal, purpose, scopes, ttlSeconds) -> {
            if (ttlSeconds <= 0) throw new GrantRefusedError("ttl_seconds must be positive");
            Minted minted;
            try {
                minted = mint(keys, principal, scopes == null ? List.of() : scopes, purpose, ttlSeconds);
            } catch (IllegalArgumentException e) {
                throw new GrantRefusedError(e.getMessage());
            }
            return new IssuedGrant(minted.token(), (double) minted.claims().expiresAt(),
                    minted.claims().grantId());
        };
    }

    // --- payload ---------------------------------------------------------------------------

    static byte[] encodePayload(GrantClaims c) {
        if (c.scopes().size() > MAX_FIELD) throw new IllegalArgumentException("too many scopes");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteBuffer longs = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
        longs.putLong(c.issuedAt()).putLong(c.expiresAt());
        out.writeBytes(longs.array());
        packText(out, c.grantId());
        packText(out, c.principal());
        packText(out, c.purpose());
        packU16(out, c.scopes().size());
        for (String s : c.scopes()) packText(out, s);
        return out.toByteArray();
    }

    private static void packText(ByteArrayOutputStream out, String value) {
        byte[] raw = (value == null ? "" : value).getBytes(StandardCharsets.UTF_8);
        if (raw.length > MAX_FIELD) throw new IllegalArgumentException("grant field longer than 65535 bytes");
        packU16(out, raw.length);
        out.writeBytes(raw);
    }

    private static void packU16(ByteArrayOutputStream out, int v) {
        out.write(v & 0xff);
        out.write((v >>> 8) & 0xff);
    }

    /** Parse strictly: exact lengths, valid UTF-8, no trailing bytes. */
    static GrantClaims decodePayload(byte[] payload) {
        ByteBuffer b = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);
        try {
            long issuedAt = b.getLong();
            long expiresAt = b.getLong();
            String grantId = text(b);
            String principal = text(b);
            String purpose = text(b);
            int count = Short.toUnsignedInt(b.getShort());
            List<String> scopes = new ArrayList<>(count);
            for (int i = 0; i < count; i++) scopes.add(text(b));
            if (b.hasRemaining()) throw new GrantInvalidException("grant payload has trailing bytes");
            return new GrantClaims(principal, scopes, purpose, grantId, issuedAt, expiresAt);
        } catch (java.nio.BufferUnderflowException e) {
            throw new GrantInvalidException("grant payload is truncated");
        }
    }

    private static String text(ByteBuffer b) {
        int len = Short.toUnsignedInt(b.getShort());
        if (len > b.remaining()) throw new GrantInvalidException("grant payload is truncated");
        byte[] raw = new byte[len];
        b.get(raw);
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(raw)).toString();
        } catch (CharacterCodingException e) {
            throw new GrantInvalidException("grant payload is not UTF-8");
        }
    }

    // --- base64url -------------------------------------------------------------------------

    static String b64url(byte[] data) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(data);
    }

    /**
     * Decode unpadded base64url, rejecting any non-canonical spelling: re-encoding must give the
     * same text, which rejects non-zero trailing bits -- one token, one spelling.
     */
    static byte[] b64urlStrict(String text) {
        if (!B64URL.matcher(text).matches() || text.length() % 4 == 1) {
            throw new GrantInvalidException("grant token is not unpadded base64url");
        }
        byte[] raw;
        try {
            raw = Base64.getUrlDecoder().decode(text);
        } catch (IllegalArgumentException e) {
            throw new GrantInvalidException("grant token is not unpadded base64url");
        }
        if (!b64url(raw).equals(text)) throw new GrantInvalidException("grant token is not canonical base64url");
        return raw;
    }
}
