// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Bounded, thread-safe LRU of {@code callId} → {@link CallToken}.
 *
 * <p>A pure accelerator: a miss (cold process, evicted entry, request landing
 * on a different node) falls back to opening the call token the client
 * supplied, so statelessness is preserved and no request depends on a prior
 * request having warmed anything.</p>
 *
 * <p>The key pairs the {@code callId} recovered from <em>inside</em> the
 * cursor's ciphertext with the caller's principal. Both parts are
 * authenticated before the lookup happens, so a client can neither steer a
 * lookup toward another principal's entry nor present a {@code callId} the
 * server never minted.</p>
 */
final class CallStateCache {

    /** Entry ceiling when the operator states none. */
    static final int DEFAULT_MAX_ENTRIES = 4096;

    private final long ttlSeconds;
    /** {@code null} when the cache is disabled. */
    private final Map<String, CachedCall> entries;

    private record CachedCall(long expiresAt, CallToken call) {
    }

    CallStateCache(long ttlSeconds) {
        this(ttlSeconds, DEFAULT_MAX_ENTRIES);
    }

    /**
     * @param maxEntries entry ceiling; {@code <= 0} disables the cache
     *     entirely, so every continuation takes the miss path. That is not
     *     just a memory knob: it is what turns "the client forgot to echo its
     *     call token" from a bug that only shows up on a cold node into a
     *     deterministic failure, which is how the conformance suite pins the
     *     stateless-relay contract.
     */
    CallStateCache(long ttlSeconds, int maxEntries) {
        this.ttlSeconds = ttlSeconds > 0 ? ttlSeconds : 3600;
        final int cap = maxEntries;
        this.entries = cap <= 0 ? null : new LinkedHashMap<>(64, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, CachedCall> eldest) {
                return size() > cap;
            }
        };
    }

    private static String key(byte[] callId, String principal) {
        return HexFormat.of().formatHex(callId) + "\0" + (principal != null ? principal : "");
    }

    synchronized CallToken get(byte[] callId, String principal) {
        if (entries == null) {
            return null;
        }
        String k = key(callId, principal);
        CachedCall e = entries.get(k);
        if (e == null) {
            return null;
        }
        if (System.currentTimeMillis() / 1000 > e.expiresAt()) {
            entries.remove(k);
            return null;
        }
        return e.call();
    }

    synchronized void put(byte[] callId, String principal, CallToken call) {
        if (entries == null) {
            return;
        }
        entries.put(key(callId, principal),
                new CachedCall(System.currentTimeMillis() / 1000 + ttlSeconds, call));
    }
}
