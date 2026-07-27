// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http.auth;

import farm.query.vgirpc.AuthContext;
import farm.query.vgirpc.http.Authenticator;
import farm.query.vgirpc.http.InvalidCredentials;
import jakarta.servlet.http.HttpServletRequest;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Base64;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.regex.Pattern;

/**
 * Proxy proof: HMAC evidence that a request arrived through a trusted proxy.
 *
 * <p>A proxy mints a per-request HMAC-SHA256 over a timestamp, a fresh nonce and the worker's own
 * identifier, keyed by a secret shared only with that worker. The proof establishes the <em>hop</em>,
 * never the caller: it is ANDed with whatever authenticates the user rather than replacing it.
 *
 * <p>Unlike a forwarded assertion about what happened at a TLS terminator, a proof cannot be
 * produced by someone who merely reaches the worker directly — without the secret there is nothing
 * to replay.
 *
 * <p>The normative cross-language contract is {@code docs/proxy-proof-spec.md} in the vgi-rpc
 * repository.
 */
public final class ProxyProof {

    /** Header carrying the proof on the wire. */
    public static final String PROOF_HEADER = "VGI-Proxy-Proof";

    /** Header advertising that this worker rejects unproofed requests. */
    public static final String PROOF_REQUIRED_HEADER = "VGI-Proxy-Proof-Required";

    private static final String VERSION = "v1";
    private static final byte[] DOMAIN_PREFIX = "vgi.proxy.proof.v1".getBytes(StandardCharsets.UTF_8);
    private static final byte[] DERIVE_LABEL = "vgi.proxy.proof.v1/".getBytes(StandardCharsets.UTF_8);
    private static final int MAX_HEADER_LEN = 512;
    private static final int SECRET_LEN = 32;
    private static final String CLAIMS_KEY = "vgi_proxy_proof";
    private static final int DEFAULT_REPLAY_CAPACITY = 100_000;

    // Charsets are load-bearing, not cosmetic: the canonical string is NUL-separated, so framing is
    // only unambiguous because no field can contain a NUL (and kid cannot contain the '.' that
    // separates wire fields).
    private static final Pattern KID_RE = Pattern.compile("\\A[A-Za-z0-9_-]{1,64}\\z");
    private static final Pattern TS_RE = Pattern.compile("\\A[0-9]{1,20}\\z");
    private static final Pattern NONCE_RE = Pattern.compile("\\A[A-Za-z0-9_-]{22}\\z");
    private static final Pattern ORIGIN_RE = Pattern.compile("\\A[A-Za-z0-9._:/-]{1,255}\\z");
    // The MAC is charset-checked rather than left to the base64 decoder: decoders disagree about
    // invalid input across languages, and the reason code is part of the wire contract.
    private static final Pattern MAC_RE = Pattern.compile("\\A[A-Za-z0-9_-]{43}\\z");

    private static final Base64.Encoder B64 = Base64.getUrlEncoder().withoutPadding();
    private static final Base64.Decoder B64D = Base64.getUrlDecoder();

    private ProxyProof() {}

    /** How strictly a worker treats the proof header. */
    public enum Mode {
        /** Install no gate at all — zero per-request cost. */
        OFF,
        /** Verify and record but never deny. A rollout lever. */
        ALLOW,
        /** Reject a request whose proof does not verify. */
        REQUIRE
    }

    /**
     * One accepted key and the proxy label it attributes to.
     *
     * @param secret the 32-byte shared secret
     * @param label operator-facing name of the calling proxy
     */
    public record Secret(byte[] secret, String label) {}

    /** Worker-side proof configuration. */
    public static final class Config {
        private final Mode mode;
        private final String originId;
        private final Map<String, Secret> secrets;
        private final int skewSeconds;
        private final int replayCapacity;
        private final boolean replayCache;
        private final LongSupplier clock;

        private Config(
                Mode mode,
                String originId,
                Map<String, Secret> secrets,
                int skewSeconds,
                int replayCapacity,
                boolean replayCache,
                LongSupplier clock) {
            this.mode = mode;
            this.originId = originId;
            this.secrets = Map.copyOf(secrets);
            this.skewSeconds = skewSeconds;
            this.replayCapacity = replayCapacity;
            this.replayCache = replayCache;
            this.clock = clock;
        }

        /**
         * Create a configuration with the standard defaults.
         *
         * @param mode enforcement mode; must not be {@link Mode#OFF}
         * @param originId this worker's identifier
         * @param secrets accepted key ids
         * @return a validated configuration
         */
        public static Config of(Mode mode, String originId, Map<String, Secret> secrets) {
            return new Config(mode, originId, secrets, 30, DEFAULT_REPLAY_CAPACITY, true, null);
        }

        /**
         * Return a copy with a different acceptance half-window.
         *
         * @param seconds half-width of the timestamp window
         * @return the adjusted configuration
         */
        public Config withSkewSeconds(int seconds) {
            return new Config(mode, originId, secrets, seconds, replayCapacity, replayCache, clock);
        }

        /**
         * Return a copy with nonce replay tracking disabled.
         *
         * @return the adjusted configuration
         */
        public Config withoutReplayCache() {
            return new Config(mode, originId, secrets, skewSeconds, replayCapacity, false, clock);
        }

        /**
         * Return a copy using an injected clock, for deterministic tests.
         *
         * @param clock supplier of Unix seconds
         * @return the adjusted configuration
         */
        public Config withClock(LongSupplier clock) {
            return new Config(mode, originId, secrets, skewSeconds, replayCapacity, replayCache, clock);
        }

        long now() {
            return clock != null ? clock.getAsLong() : System.currentTimeMillis() / 1000L;
        }
    }

    /** A proof rejection carrying its reason code. */
    public static final class ProofFailure extends Exception {
        private static final long serialVersionUID = 1L;
        private final String reason;

        ProofFailure(String reason) {
            super(reason);
            this.reason = reason;
        }

        /**
         * The reason code from the specification's closed set.
         *
         * @return the reason code
         */
        public String reason() {
            return reason;
        }
    }

    /**
     * Derive the secret shared between one proxy and one worker.
     *
     * <p>A worker is configured with its derived secret only, never the base key — otherwise it
     * could mint proofs its siblings would accept.
     *
     * @param baseKey the proxy's 32-byte root key
     * @param proxyId the minting proxy's identity
     * @param originId the destination worker's identity
     * @return the 32-byte derived secret
     */
    public static byte[] deriveSecret(byte[] baseKey, String proxyId, String originId) {
        if (baseKey.length != SECRET_LEN) {
            throw new IllegalArgumentException("base key must be exactly " + SECRET_LEN + " bytes");
        }
        requireMatch(ORIGIN_RE, proxyId, "proxyId");
        requireMatch(ORIGIN_RE, originId, "originId");
        byte[] p = proxyId.getBytes(StandardCharsets.UTF_8);
        byte[] o = originId.getBytes(StandardCharsets.UTF_8);
        byte[] msg = new byte[DERIVE_LABEL.length + p.length + 1 + o.length];
        int at = 0;
        System.arraycopy(DERIVE_LABEL, 0, msg, at, DERIVE_LABEL.length);
        at += DERIVE_LABEL.length;
        System.arraycopy(p, 0, msg, at, p.length);
        at += p.length;
        // NUL-separated, and neither identifier may contain NUL — so ("a", "b\0c") cannot collide
        // with ("a\0b", "c").
        msg[at++] = 0;
        System.arraycopy(o, 0, msg, at, o.length);
        return Crypto.hmacSha256(baseKey, msg);
    }

    /**
     * Build the MAC input.
     *
     * <p>{@code originId} is folded in but never transmitted: the worker supplies its own, which is
     * what binds a proof to a single audience.
     */
    static byte[] canonicalString(String kid, String ts, String nonce, String originId) {
        byte[][] parts = {
            DOMAIN_PREFIX,
            kid.getBytes(StandardCharsets.UTF_8),
            ts.getBytes(StandardCharsets.UTF_8),
            nonce.getBytes(StandardCharsets.UTF_8),
            originId.getBytes(StandardCharsets.UTF_8)
        };
        int total = parts.length - 1;
        for (byte[] p : parts) {
            total += p.length;
        }
        byte[] out = new byte[total];
        int at = 0;
        for (int i = 0; i < parts.length; i++) {
            if (i > 0) {
                out[at++] = 0;
            }
            System.arraycopy(parts[i], 0, out, at, parts[i].length);
            at += parts[i].length;
        }
        return out;
    }

    /**
     * Mint a proof token. Primarily for tests and for clients fronting a worker.
     *
     * @param secret the shared secret
     * @param kid key id, which also identifies the minting proxy
     * @param originId the destination worker's identity
     * @param now Unix seconds
     * @param nonce a fresh nonce, 22 base64url characters
     * @return the header value
     */
    public static String mint(byte[] secret, String kid, String originId, long now, String nonce) {
        requireMatch(KID_RE, kid, "kid");
        requireMatch(ORIGIN_RE, originId, "originId");
        String ts = Long.toString(now);
        byte[] mac = Crypto.hmacSha256(secret, canonicalString(kid, ts, nonce, originId));
        return VERSION + "." + kid + "." + ts + "." + nonce + "." + B64.encodeToString(mac);
    }

    /**
     * Verify a proof token, returning the claims to record.
     *
     * <p>Cheap rejections run before any MAC is computed, so an unparseable header costs a few
     * pattern matches rather than a hash.
     *
     * @param token the raw header value
     * @param config worker configuration
     * @param cache replay cache, or {@code null} to skip nonce tracking
     * @return claims describing the verified proxy
     * @throws ProofFailure on any rejection
     */
    public static Map<String, Object> verify(String token, Config config, NonceCache cache)
            throws ProofFailure {
        if (token.length() > MAX_HEADER_LEN) {
            throw new ProofFailure("malformed");
        }
        String[] parts = token.split("\\.", -1);
        if (parts.length != 5) {
            throw new ProofFailure("malformed");
        }
        String version = parts[0];
        String kid = parts[1];
        String tsRaw = parts[2];
        String nonce = parts[3];
        String macB64 = parts[4];
        if (!VERSION.equals(version)
                || !KID_RE.matcher(kid).matches()
                || !TS_RE.matcher(tsRaw).matches()
                || !NONCE_RE.matcher(nonce).matches()
                || !MAC_RE.matcher(macB64).matches()) {
            throw new ProofFailure("malformed");
        }

        Secret entry = config.secrets.get(kid);
        if (entry == null) {
            throw new ProofFailure("unknown_kid");
        }

        long ts;
        try {
            ts = Long.parseLong(tsRaw);
        } catch (NumberFormatException e) {
            throw new ProofFailure("malformed");
        }
        // Two-sided. Checking only the upper bound would let a far-future timestamp pass forever —
        // the defect present in this package's own signed-cookie helper.
        long age = config.now() - ts;
        if (age > config.skewSeconds) {
            throw new ProofFailure("expired");
        }
        if (-age > config.skewSeconds) {
            throw new ProofFailure("not_yet_valid");
        }

        byte[] expected =
                Crypto.hmacSha256(entry.secret(), canonicalString(kid, tsRaw, nonce, config.originId));
        byte[] received;
        try {
            received = B64D.decode(macB64);
        } catch (IllegalArgumentException e) {
            throw new ProofFailure("malformed");
        }
        // kid is public, so selecting one candidate secret is a safe branch; only the resulting MAC
        // needs the constant-time compare.
        if (!Crypto.constantTimeEquals(received, expected)) {
            throw new ProofFailure("bad_mac");
        }

        if (cache != null && !cache.checkAndAdd(nonce, config.now())) {
            throw new ProofFailure("replayed");
        }

        Map<String, Object> claims = new LinkedHashMap<>();
        claims.put("verified", "true");
        claims.put("proxy", entry.label());
        claims.put("kid", kid);
        claims.put("origin_id", config.originId);
        claims.put("reason", "ok");
        return Collections.unmodifiableMap(claims);
    }

    /**
     * Wrap an authenticator with a proof precondition.
     *
     * <p>The gate runs first; on failure {@code inner} is never invoked. This is an AND, not an
     * alternative — do not pass a proof gate to {@link Authenticator#chain}, whose
     * first-authenticated-wins semantics would let any later credential bypass it.
     *
     * @param config worker configuration; mode must not be {@link Mode#OFF}
     * @param inner the credential establishing caller identity, or {@code null} for proof-only
     * @return an authenticator enforcing both
     */
    public static Authenticator require(Config config, Authenticator inner) {
        Objects.requireNonNull(config, "config");
        if (config.mode == Mode.OFF) {
            throw new IllegalArgumentException("mode=OFF installs no gate; do not build one");
        }
        requireMatch(ORIGIN_RE, config.originId, "originId");
        if (config.secrets.isEmpty()) {
            throw new IllegalArgumentException("at least one secret is required");
        }
        for (Map.Entry<String, Secret> e : config.secrets.entrySet()) {
            requireMatch(KID_RE, e.getKey(), "kid");
            if (e.getValue().secret().length != SECRET_LEN) {
                throw new IllegalArgumentException(
                        "secret for kid " + e.getKey() + " must be " + SECRET_LEN + " bytes");
            }
        }
        if (config.skewSeconds <= 0) {
            throw new IllegalArgumentException("skewSeconds must be positive");
        }

        NonceCache cache =
                config.replayCache ? new NonceCache(config.skewSeconds, config.replayCapacity) : null;
        boolean required = config.mode == Mode.REQUIRE;

        return request -> {
            Map<String, Object> claims;
            try {
                claims = verifyRequest(request, config, cache);
            } catch (ProofFailure failure) {
                if (required) {
                    // Uniform message: the caller controls kid, so echoing any detail would reflect
                    // attacker-supplied text.
                    throw new InvalidCredentials("proxy proof required");
                }
                Map<String, Object> unverified = new LinkedHashMap<>();
                unverified.put("verified", "false");
                unverified.put("proxy", "");
                unverified.put("kid", "");
                unverified.put("origin_id", config.originId);
                unverified.put("reason", failure.reason());
                claims = Collections.unmodifiableMap(unverified);
            }

            if (inner == null) {
                Map<String, Object> wrapped = new HashMap<>();
                wrapped.put(CLAIMS_KEY, claims);
                return new AuthContext(CLAIMS_KEY, true, (String) claims.get("proxy"), wrapped);
            }
            AuthContext ctx = inner.authenticate(request);
            Map<String, Object> merged = new HashMap<>(ctx.claims());
            merged.put(CLAIMS_KEY, claims);
            return new AuthContext(ctx.domain(), ctx.authenticated(), ctx.principal(), merged);
        };
    }

    private static Map<String, Object> verifyRequest(
            HttpServletRequest request, Config config, NonceCache cache) throws ProofFailure {
        String raw = request.getHeader(PROOF_HEADER);
        if (raw == null || raw.isEmpty()) {
            throw new ProofFailure("no_proof");
        }
        if (raw.indexOf(',') >= 0) {
            throw new ProofFailure("malformed");
        }
        return verify(raw, config, cache);
    }

    /**
     * Parse a {@code kid:hex,kid:hex} secret specification.
     *
     * <p>The kid doubles as the proxy's label, so attribution needs no extra configuration. Any
     * malformed entry fails the whole parse rather than silently dropping one proxy's access.
     *
     * @param raw the specification
     * @return the parsed secrets
     */
    public static Map<String, Secret> parseSecrets(String raw) {
        Map<String, Secret> out = new HashMap<>();
        for (String chunk : raw.split(",")) {
            String item = chunk.trim();
            if (item.isEmpty()) {
                continue;
            }
            int sep = item.indexOf(':');
            if (sep < 0) {
                throw new IllegalArgumentException("expected 'kid:hex', got " + item);
            }
            String kid = item.substring(0, sep);
            String hex = item.substring(sep + 1);
            requireMatch(KID_RE, kid, "kid");
            if (hex.length() != SECRET_LEN * 2) {
                throw new IllegalArgumentException(
                        "secret for kid " + kid + " must be " + (SECRET_LEN * 2) + " hex chars");
            }
            byte[] secret = new byte[SECRET_LEN];
            for (int i = 0; i < SECRET_LEN; i++) {
                int hi = Character.digit(hex.charAt(i * 2), 16);
                int lo = Character.digit(hex.charAt(i * 2 + 1), 16);
                if (hi < 0 || lo < 0) {
                    throw new IllegalArgumentException("secret for kid " + kid + " is not valid hex");
                }
                secret[i] = (byte) ((hi << 4) | lo);
            }
            out.put(kid, new Secret(secret, kid));
        }
        if (out.isEmpty()) {
            throw new IllegalArgumentException("no secrets parsed");
        }
        return out;
    }

    private static void requireMatch(Pattern pattern, String value, String name) {
        if (value == null || !pattern.matcher(value).matches()) {
            throw new IllegalArgumentException(name + " must match " + pattern.pattern());
        }
    }

    /**
     * A bounded, TTL-expiring set of recently-seen nonces.
     *
     * <p>The capacity cap is not optional: a TTL bounds how long an entry lives, never how many
     * arrive inside the window, so a TTL-only cache is a remote memory-exhaustion vector.
     */
    public static final class NonceCache {
        private final Deque<String> order = new ArrayDeque<>();
        private final Map<String, Long> expiry = new HashMap<>();
        private final Set<String> seen = new HashSet<>();
        private final long ttl;
        private final int capacity;

        /**
         * Create a cache retaining nonces for {@code ttlSeconds}, bounded by {@code capacity}.
         *
         * @param ttlSeconds retention window
         * @param capacity hard upper bound on retained entries
         */
        public NonceCache(long ttlSeconds, int capacity) {
            this.ttl = ttlSeconds;
            this.capacity = capacity;
        }

        /**
         * Atomically report whether a nonce is fresh, remembering it if so.
         *
         * <p>Test and insert are one synchronized operation: a separate contains-then-add would let
         * two concurrent replays both observe "not seen" and both be accepted.
         *
         * @param nonce the nonce from a proof token
         * @param now Unix seconds
         * @return {@code true} if unseen
         */
        public synchronized boolean checkAndAdd(String nonce, long now) {
            // Uniform TTL means insertion order is expiry order, so expired entries are always a
            // prefix and this sweep is exact.
            while (!order.isEmpty()) {
                String oldest = order.peekFirst();
                Long expires = expiry.get(oldest);
                if (expires != null && expires > now) {
                    break;
                }
                order.pollFirst();
                expiry.remove(oldest);
                seen.remove(oldest);
            }
            if (seen.contains(nonce)) {
                return false;
            }
            // Evict oldest rather than refuse: a burst past capacity is an availability problem, not
            // an authentication one, and the timestamp window still bounds the evicted nonce.
            while (order.size() >= capacity) {
                String oldest = order.pollFirst();
                expiry.remove(oldest);
                seen.remove(oldest);
            }
            order.addLast(nonce);
            expiry.put(nonce, now + ttl);
            seen.add(nonce);
            return true;
        }

        /**
         * Number of retained nonces.
         *
         * @return the current size
         */
        public synchronized int size() {
            return seen.size();
        }
    }
}
