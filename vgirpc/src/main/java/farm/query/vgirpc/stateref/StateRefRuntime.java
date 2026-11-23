// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.stateref;

import farm.query.vgirpc.external.ObjectStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * One HTTP server's {@link StateRef} machinery: key, storage, budgets and object cache
 * (STATE_REF_V1.md §5, §9, §11).
 *
 * <p>Built by {@link farm.query.vgirpc.http.HttpServer}; the byte-stream transports never
 * need one, because their state never leaves the process.</p>
 *
 * <h2>Turn budget</h2>
 * <pre>
 * turn_budget            = floor((max_request_bytes - 4096) / 1.34) - call_token_bytes
 * state_ref_inline_bytes = min(VGI_RPC_STATE_REF_INLINE_BYTES, floor(turn_budget / 4))
 * </pre>
 * <p>An unset (or {@code <= 0}) request cap means an infinite budget. Below a
 * {@value #CURSOR_RULE_FLOOR_BYTES}-byte cap the server logs a startup warning and
 * disables the cursor rule for its lifetime; the inline limit is still computed (floored
 * at 0, so everything over the limit is stored when storage exists).</p>
 */
public final class StateRefRuntime {

    private static final Logger LOG = LoggerFactory.getLogger("farm.query.vgirpc.state_ref");

    /** Default {@value #STATE_REF_INLINE_BYTES_ENV}. */
    public static final int DEFAULT_STATE_REF_INLINE_BYTES = 16384;
    /** Environment variable capping the inline payload size. */
    public static final String STATE_REF_INLINE_BYTES_ENV = "VGI_RPC_STATE_REF_INLINE_BYTES";
    /** Room left in a continuation request for everything but the two tokens. */
    public static final long TURN_BUDGET_HEADROOM_BYTES = 4096;
    /** A request cap below this disables the cursor rule (with a startup warning). */
    public static final long CURSOR_RULE_FLOOR_BYTES = 16384;
    /** Default maximum age of cursor tokens, call tokens and stored tags: 6 hours. */
    public static final long DEFAULT_TOKEN_TTL_SECONDS = 21600;

    /** What the server does with a cursor over its turn budget. */
    public enum CursorRule {
        /** Log {@code cursor_large} (and send the client a WARN log); the stream continues. */
        WARN,
        /** Refuse the turn with {@code cursor_too_large} (only when a request cap is set). */
        ERROR;

        /**
         * @param text {@code "warn"} or {@code "error"} (any case)
         * @return the rule
         * @throws IllegalArgumentException for anything else
         */
        public static CursorRule parse(String text) {
            if (text != null) {
                switch (text.strip().toLowerCase(java.util.Locale.ROOT)) {
                    case "warn": return WARN;
                    case "error": return ERROR;
                    default: break;
                }
            }
            throw new IllegalArgumentException("cursor_rule must be 'warn' or 'error', got '" + text + "'");
        }
    }

    private static final ThreadLocal<StateRefRuntime> CURRENT = new ThreadLocal<>();

    private final byte[] tokenKey;
    private final long tokenTtlSeconds;
    private final Supplier<?> storage;
    private final long maxFetchBytes;
    private final Long maxRequestBytes;
    private final int inlineBytesCap;
    private final CursorRule cursorRule;
    private final ObjectCache cache;
    private final LongSupplier clockSeconds;
    private final boolean cursorRuleDisabled;

    private StateRefRuntime(Builder b) {
        if (b.tokenKey == null || b.tokenKey.length != 32) {
            throw new IllegalArgumentException("tokenKey must be 32 bytes");
        }
        if (b.inlineBytesCap <= 0) {
            throw new IllegalArgumentException("state_ref_inline_bytes must be a positive integer, got "
                    + b.inlineBytesCap);
        }
        if (b.tokenTtlSeconds < 0) throw new IllegalArgumentException("tokenTtlSeconds must be >= 0");
        if (b.maxFetchBytes <= 0) throw new IllegalArgumentException("maxFetchBytes must be > 0");
        this.tokenKey = b.tokenKey.clone();
        this.tokenTtlSeconds = b.tokenTtlSeconds;
        this.storage = b.storage;
        this.maxFetchBytes = b.maxFetchBytes;
        this.maxRequestBytes = b.maxRequestBytes != null && b.maxRequestBytes > 0
                && b.maxRequestBytes != Long.MAX_VALUE ? b.maxRequestBytes : null;
        this.inlineBytesCap = b.inlineBytesCap;
        this.cursorRule = b.cursorRule;
        this.cache = b.cache != null ? b.cache : ObjectCache.defaultCache();
        this.clockSeconds = b.clockSeconds;
        this.cursorRuleDisabled = maxRequestBytes != null && maxRequestBytes < CURSOR_RULE_FLOOR_BYTES;
        if (cursorRuleDisabled) {
            LOG.warn("max_request_bytes ({}) is below {}: the cursor rule is disabled and stream "
                    + "continuations may not fit", maxRequestBytes, CURSOR_RULE_FLOOR_BYTES);
        }
    }

    /** @return a builder */
    public static Builder builder() {
        return new Builder();
    }

    /** Builder for {@link StateRefRuntime}. */
    public static final class Builder {
        private byte[] tokenKey;
        private long tokenTtlSeconds = DEFAULT_TOKEN_TTL_SECONDS;
        private Supplier<?> storage = () -> null;
        private long maxFetchBytes = farm.query.vgirpc.external.ExternalLocationConfig.DEFAULT_MAX_FETCH_BYTES;
        private Long maxRequestBytes;
        private int inlineBytesCap = DEFAULT_STATE_REF_INLINE_BYTES;
        private CursorRule cursorRule = CursorRule.WARN;
        private ObjectCache cache;
        private LongSupplier clockSeconds = () -> System.currentTimeMillis() / 1000;

        private Builder() {}

        /** @param key the server's 32-byte AEAD key, shared by every replica @return this */
        public Builder tokenKey(byte[] key) { this.tokenKey = key; return this; }
        /** @param seconds maximum age of a stored tag; {@code 0} disables @return this */
        public Builder tokenTtlSeconds(long seconds) { this.tokenTtlSeconds = seconds; return this; }
        /**
         * @param source returns the configured storage, read per use; one that is not an
         *     {@link ObjectStore} counts as none (refs stay inline)
         * @return this
         */
        public Builder storage(Supplier<?> source) { this.storage = source != null ? source : () -> null; return this; }
        /** @param bytes the server's inbound fetch cap @return this */
        public Builder maxFetchBytes(long bytes) { this.maxFetchBytes = bytes; return this; }
        /** @param bytes the advertised request cap; {@code null}, {@code <= 0} or MAX_VALUE = unbounded @return this */
        public Builder maxRequestBytes(Long bytes) { this.maxRequestBytes = bytes; return this; }
        /** @param bytes {@value #STATE_REF_INLINE_BYTES_ENV}; must be positive @return this */
        public Builder inlineBytesCap(int bytes) { this.inlineBytesCap = bytes; return this; }
        /** @param rule the cursor rule @return this */
        public Builder cursorRule(CursorRule rule) { this.cursorRule = rule != null ? rule : CursorRule.WARN; return this; }
        /** @param objectCache the object cache (default: the process-wide one) @return this */
        public Builder cache(ObjectCache objectCache) { this.cache = objectCache; return this; }
        /** @param clock wall clock in seconds, injectable for tests @return this */
        public Builder clockSeconds(LongSupplier clock) { this.clockSeconds = clock; return this; }
        /** @return the runtime */
        public StateRefRuntime build() { return new StateRefRuntime(this); }
    }

    /**
     * Read {@value #STATE_REF_INLINE_BYTES_ENV} (default {@value #DEFAULT_STATE_REF_INLINE_BYTES}).
     *
     * @param env the environment
     * @return the inline cap
     * @throws IllegalArgumentException if set to anything but a positive integer
     */
    public static int inlineBytesFromEnv(Map<String, String> env) {
        String raw = env.get(STATE_REF_INLINE_BYTES_ENV);
        if (raw == null || raw.isBlank()) return DEFAULT_STATE_REF_INLINE_BYTES;
        long value;
        try {
            value = Long.parseLong(raw.strip());
        } catch (NumberFormatException e) {
            value = 0;
        }
        if (value <= 0 || value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(STATE_REF_INLINE_BYTES_ENV + " must be a positive integer, got '"
                    + raw + "'");
        }
        return (int) value;
    }

    /**
     * The sealed bytes a cursor may take on a continuation.
     *
     * @param maxRequestBytes the advertised request cap, {@code null} or {@code <= 0} for none
     * @param callTokenBytes sealed length of the stream's call token ({@code 0} when sizing the
     *     call token's own schemas)
     * @return the budget (negative under a tiny cap), or {@code null} when unbounded
     */
    public static Long turnBudget(Long maxRequestBytes, long callTokenBytes) {
        if (maxRequestBytes == null || maxRequestBytes <= 0 || maxRequestBytes == Long.MAX_VALUE) return null;
        return Math.floorDiv((maxRequestBytes - TURN_BUDGET_HEADROOM_BYTES) * 100, 134) - callTokenBytes;
    }

    /** @param callTokenBytes sealed call-token length @return {@link #turnBudget(Long, long)} for this server */
    public Long turnBudget(long callTokenBytes) {
        return turnBudget(maxRequestBytes, callTokenBytes);
    }

    /**
     * {@code state_ref_inline_bytes}: {@code min(cap, floor(turn_budget / 4))}, floored at 0.
     *
     * @param callTokenBytes sealed call-token length
     * @return the largest payload kept inline
     */
    public long inlineBytes(long callTokenBytes) {
        Long budget = turnBudget(callTokenBytes);
        if (budget == null) return inlineBytesCap;
        return Math.max(0, Math.min(inlineBytesCap, Math.floorDiv(budget, 4)));
    }

    /**
     * Apply the cursor rule to one sealed cursor.
     *
     * @param cursorBytes sealed (base64-decoded) cursor length
     * @param callTokenBytes sealed call-token length
     * @param protocol owning protocol, for the message
     * @param method method name, for the message
     * @return {@code null} when the cursor fits (or the rule is off), else the
     *     {@code cursor_large: ...} warning text under {@link CursorRule#WARN}
     * @throws CursorTooLargeError over budget under {@link CursorRule#ERROR}
     */
    public String checkCursor(long cursorBytes, long callTokenBytes, String protocol, String method) {
        if (cursorRuleDisabled) return null;
        Long budget = turnBudget(callTokenBytes);
        if (budget == null || cursorBytes <= budget) return null;
        String detail = "stream state for " + protocol + "." + method + " is " + cursorBytes
                + " bytes, over this server's turn budget of " + budget + " bytes (VGI-Max-Request-Bytes "
                + maxRequestBytes + "). Wrap data that does not change across turns in ctx.state_ref().";
        if (cursorRule == CursorRule.ERROR) {
            LOG.warn("cursor_too_large: {} (event=cursor_too_large protocol={} method={} cursor_bytes={} budget={})",
                    detail, protocol, method, cursorBytes, budget);
            throw new CursorTooLargeError("cursor_too_large: " + detail);
        }
        LOG.warn("cursor_large: {} (event=cursor_large protocol={} method={} cursor_bytes={} budget={})",
                detail, protocol, method, cursorBytes, budget);
        return "cursor_large: " + detail;
    }

    /** @return the configured object store, or {@code null} when there is none */
    public ObjectStore storage() {
        Object candidate = storage.get();
        return candidate instanceof ObjectStore os ? os : null;
    }

    /** @return whether over-budget cursors warn or error at all (off below the 16 KiB floor) */
    public boolean cursorRuleEnabled() {
        return !cursorRuleDisabled;
    }

    /** @return the largest decompressed value: 64 MiB or {@code max_fetch_bytes}, whichever is larger */
    public long decompressCap() {
        return Math.max(TokenCodec.MAX_TOKEN_PLAINTEXT_BYTES, maxFetchBytes);
    }

    /** @return the token key (a copy) */
    public byte[] tokenKey() { return tokenKey.clone(); }
    byte[] key() { return tokenKey; }
    /** @return the tag TTL in seconds ({@code 0} = none) */
    public long tokenTtlSeconds() { return tokenTtlSeconds; }
    /** @return the inbound fetch cap */
    public long maxFetchBytes() { return maxFetchBytes; }
    /** @return the request cap, or {@code null} when unbounded */
    public Long maxRequestBytes() { return maxRequestBytes; }
    /** @return {@value #STATE_REF_INLINE_BYTES_ENV} as configured */
    public int inlineBytesCap() { return inlineBytesCap; }
    /** @return the cursor rule */
    public CursorRule cursorRule() { return cursorRule; }
    /** @return the object cache */
    public ObjectCache cache() { return cache; }
    long nowSeconds() { return clockSeconds.getAsLong(); }

    /**
     * Make {@code runtime} the current thread's runtime (the HTTP server does this around
     * unary dispatch, so {@link farm.query.vgirpc.CallContext#resolveArgumentRef} works there).
     *
     * @param runtime the runtime, or {@code null}
     * @return restores the previous value when closed
     */
    public static AutoCloseable push(StateRefRuntime runtime) {
        StateRefRuntime previous = CURRENT.get();
        CURRENT.set(runtime);
        return () -> {
            if (previous == null) CURRENT.remove();
            else CURRENT.set(previous);
        };
    }

    /** @return the runtime installed by {@link #push}, or {@code null} off HTTP */
    public static StateRefRuntime current() {
        return CURRENT.get();
    }
}
