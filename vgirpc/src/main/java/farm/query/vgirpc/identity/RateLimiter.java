// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.identity;

import java.util.HashMap;
import java.util.Map;

/**
 * Fixed-window request limiter, keyed by caller.
 *
 * <p>Present because introspection is a credential-to-identity oracle even when correctly
 * restricted: an allowlisted caller whose own credential leaks can still test guesses. Rate
 * limiting does not close that, it bounds it -- a ceiling on how fast an attacker converts
 * guesses into answers.
 *
 * <p>Fixed-window rather than a token bucket: a window admits at most twice the rate across a
 * boundary, which is a rounding error here, and the state is one integer per caller rather than
 * a float that has to be aged.
 *
 * <p>Every method is {@code synchronized}. A worker serves concurrent HTTP requests from a pool,
 * so an unsynchronised counter would let two threads read the same count and both admit -- and a
 * limiter that can be beaten by racing it is not a limiter.
 */
public final class RateLimiter {

    private final int perWindow;
    private final double windowSeconds;
    private final Map<String, Integer> counts = new HashMap<>();
    private double windowStart;

    /**
     * Admit {@code perWindow} requests per caller per one-second window.
     *
     * @param perWindow requests admitted per caller per window
     */
    public RateLimiter(int perWindow) {
        this(perWindow, 1.0);
    }

    /**
     * Admit {@code perWindow} requests per caller per window.
     *
     * @param perWindow requests admitted per caller per window
     * @param windowSeconds the window length, in seconds
     */
    public RateLimiter(int perWindow, double windowSeconds) {
        this.perWindow = perWindow;
        this.windowSeconds = windowSeconds;
    }

    /**
     * Whether {@code key} may make a request now.
     *
     * @param key the caller principal
     * @return {@code true} when the request is admitted
     */
    public boolean allow(String key) {
        return allow(key, System.nanoTime() / 1_000_000_000.0);
    }

    /**
     * Whether {@code key} may make a request at {@code now}.
     *
     * <p>The clock is a parameter so a test can pin window behaviour without sleeping; callers
     * in production use {@link #allow(String)} and get a monotonic reading.
     *
     * @param key the caller principal
     * @param now the current time, in seconds on any monotonic scale
     * @return {@code true} when the request is admitted
     */
    public synchronized boolean allow(String key, double now) {
        if (now - windowStart >= windowSeconds) {
            // Whole-map reset rather than per-key ageing: an attacker cycling
            // keys cannot grow the map beyond one window's worth.
            counts.clear();
            windowStart = now;
        }
        int count = counts.getOrDefault(key, 0);
        if (count >= perWindow) return false;
        counts.put(key, count + 1);
        return true;
    }

    /** How many callers the current window is tracking -- the bound a window reset enforces. */
    synchronized int trackedKeys() {
        return counts.size();
    }
}
