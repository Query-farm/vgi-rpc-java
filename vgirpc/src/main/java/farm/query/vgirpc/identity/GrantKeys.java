// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.identity;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A deployment's sealed-grant configuration (IDENTITY_V1_SPEC.md §9.1).
 *
 * <p>The first key mints; every key verifies. Rotation: add the new key first, keep the old one
 * after it until every grant it minted has expired, then drop it. Without a configuration grants
 * are off and nothing changes -- absent beats hosted-and-refusing, so no worker grows a credential
 * issuer by upgrading.
 */
public final class GrantKeys {

    /** Comma-separated standard-base64 keys, 32 bytes each, minting key first. */
    public static final String KEYS_ENV = "VGI_RPC_GRANT_KEYS";
    /** Audience bound into every token's associated data. Optional, default {@code ""}. */
    public static final String AUDIENCE_ENV = "VGI_RPC_GRANT_AUDIENCE";
    /** Lifetime ceiling in seconds. Optional, default {@link #DEFAULT_MAX_TTL_SECONDS}. */
    public static final String MAX_TTL_ENV = "VGI_RPC_GRANT_MAX_TTL_SECONDS";

    /** Seven days: short on purpose, because expiry is the only revocation a sealed grant has. */
    public static final long DEFAULT_MAX_TTL_SECONDS = 7L * 24 * 3600;
    /** Allowance for clocks disagreeing between the minting and the verifying worker. */
    public static final long DEFAULT_CLOCK_SKEW_SECONDS = 60;

    static final int KEY_LEN = 32;

    private final List<byte[]> keys;
    private final List<byte[]> kids;
    private final String audience;
    private final long maxTtlSeconds;
    private final long clockSkewSeconds;

    /**
     * @param keys 32-byte keys, minting key first
     * @param audience bound into every token's AAD; two deployments that share a key still cannot
     *     accept each other's grants when their audiences differ
     * @param maxTtlSeconds ceiling on a grant's lifetime, at minting and at verification
     * @param clockSkewSeconds tolerance applied to {@code issued_at} and {@code expires_at}
     * @throws IllegalArgumentException no key, a key that is not 32 bytes, two equal keys, or a
     *     non-positive lifetime -- a worker refuses to start rather than run misconfigured
     */
    public GrantKeys(List<byte[]> keys, String audience, long maxTtlSeconds, long clockSkewSeconds) {
        if (keys == null || keys.isEmpty()) {
            throw new IllegalArgumentException("grant configuration needs at least one key");
        }
        List<byte[]> copy = new ArrayList<>();
        List<byte[]> ids = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        for (byte[] k : keys) {
            if (k == null || k.length != KEY_LEN) {
                throw new IllegalArgumentException("every grant key must be exactly " + KEY_LEN + " bytes");
            }
            byte[] kid = SealedGrants.keyId(k);
            if (!seen.add(java.util.HexFormat.of().formatHex(kid))) {
                throw new IllegalArgumentException("grant keys must be distinct");
            }
            copy.add(k.clone());
            ids.add(kid);
        }
        if (maxTtlSeconds <= 0) throw new IllegalArgumentException("max_ttl_seconds must be positive");
        if (clockSkewSeconds < 0) throw new IllegalArgumentException("clock_skew_seconds must not be negative");
        String aud = audience == null ? "" : audience;
        if (aud.getBytes(StandardCharsets.UTF_8).length > 0xFFFF) {
            throw new IllegalArgumentException("audience is too long");
        }
        this.keys = Collections.unmodifiableList(copy);
        this.kids = Collections.unmodifiableList(ids);
        this.audience = aud;
        this.maxTtlSeconds = maxTtlSeconds;
        this.clockSkewSeconds = clockSkewSeconds;
    }

    /**
     * Keys with the default skew.
     *
     * @param keys 32-byte keys, minting key first
     * @param audience see {@link #audience()}
     * @param maxTtlSeconds see {@link #maxTtlSeconds()}
     */
    public GrantKeys(List<byte[]> keys, String audience, long maxTtlSeconds) {
        this(keys, audience, maxTtlSeconds, DEFAULT_CLOCK_SKEW_SECONDS);
    }

    /**
     * Build from standard base64 key text (padding optional), minting key first.
     *
     * @param encodedKeys each key as base64 of exactly 32 bytes
     * @param audience see {@link #audience()}
     * @param maxTtlSeconds see {@link #maxTtlSeconds()}
     * @return the configuration
     * @throws IllegalArgumentException a key that is not base64 of exactly 32 bytes
     */
    public static GrantKeys parse(List<String> encodedKeys, String audience, long maxTtlSeconds) {
        List<byte[]> keys = new ArrayList<>();
        int index = 0;
        for (String text : encodedKeys) {
            index++;
            String stripped = text.strip();
            byte[] key;
            try {
                String padded = stripped + "=".repeat((4 - stripped.length() % 4) % 4);
                if (!padded.matches("[A-Za-z0-9+/]*={0,2}")) throw new IllegalArgumentException();
                key = java.util.Base64.getDecoder().decode(padded);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("grant key #" + index + " is not valid base64");
            }
            if (key.length != KEY_LEN) {
                throw new IllegalArgumentException("grant key #" + index + " decodes to " + key.length
                        + " bytes; exactly " + KEY_LEN + " are required");
            }
            keys.add(key);
        }
        return new GrantKeys(keys, audience, maxTtlSeconds);
    }

    /**
     * Read {@code VGI_RPC_GRANT_KEYS} / {@code _AUDIENCE} / {@code _MAX_TTL_SECONDS}.
     *
     * @param env the environment to read
     * @return the configuration, or {@code null} when no key is set -- grants off
     * @throws IllegalArgumentException a malformed key or lifetime
     */
    public static GrantKeys fromEnv(Map<String, String> env) {
        String raw = env.getOrDefault(KEYS_ENV, "");
        raw = raw == null ? "" : raw.strip();
        if (raw.isEmpty()) return null;
        String ttlRaw = env.getOrDefault(MAX_TTL_ENV, "");
        ttlRaw = ttlRaw == null ? "" : ttlRaw.strip();
        long ttl;
        try {
            ttl = ttlRaw.isEmpty() ? DEFAULT_MAX_TTL_SECONDS : Long.parseLong(ttlRaw);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(MAX_TTL_ENV + "='" + ttlRaw + "' is not an integer");
        }
        List<String> parts = new ArrayList<>();
        for (String p : raw.split(",")) if (!p.isBlank()) parts.add(p);
        String audience = env.getOrDefault(AUDIENCE_ENV, "");
        return parse(parts, audience == null ? "" : audience, ttl);
    }

    /**
     * {@link #fromEnv(Map)} over the process environment.
     *
     * @return the configuration, or {@code null} when grants are off
     */
    public static GrantKeys fromEnv() {
        return fromEnv(System.getenv());
    }

    /** @return the keys, minting key first (defensive copies) */
    public List<byte[]> keys() {
        List<byte[]> out = new ArrayList<>();
        for (byte[] k : keys) out.add(k.clone());
        return out;
    }

    /** @return the audience bound into every token */
    public String audience() { return audience; }

    /** @return the lifetime ceiling in seconds */
    public long maxTtlSeconds() { return maxTtlSeconds; }

    /** @return the clock-skew tolerance in seconds */
    public long clockSkewSeconds() { return clockSkewSeconds; }

    byte[] mintingKey() { return keys.get(0); }

    byte[] mintingKid() { return kids.get(0); }

    /** The key a token's kid names, or {@code null}. */
    byte[] keyFor(byte[] kid) {
        for (int i = 0; i < kids.size(); i++) {
            if (Arrays.equals(kids.get(i), kid)) return keys.get(i);
        }
        return null;
    }

    byte[] aad(byte[] kid) {
        byte[] domain = "vgi_rpc.grant.v1\0".getBytes(StandardCharsets.US_ASCII);
        byte[] aud = audience.getBytes(StandardCharsets.UTF_8);
        byte[] out = new byte[domain.length + kid.length + aud.length];
        System.arraycopy(domain, 0, out, 0, domain.length);
        System.arraycopy(kid, 0, out, domain.length, kid.length);
        System.arraycopy(aud, 0, out, domain.length + kid.length, aud.length);
        return out;
    }

    @Override
    public String toString() {
        // Never the keys.
        return "GrantKeys[keys=" + keys.size() + ", audience=" + audience + ", maxTtlSeconds=" + maxTtlSeconds + "]";
    }
}
