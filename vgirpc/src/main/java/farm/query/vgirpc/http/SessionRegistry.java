// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import farm.query.vgirpc.ServerDrainingError;

import java.security.SecureRandom;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * In-memory sticky-session registry. Stores opaque per-session state
 * objects keyed by a server-minted 12-byte session id, with TTL eviction
 * and a per-entry {@link ReentrantLock} so concurrent calls on the same
 * session run serially.
 *
 * <p>Hot paths (request open / lookup / close) are non-blocking on a
 * {@link ConcurrentHashMap}; the drain flag and shutdown are flipped via
 * an {@link AtomicBoolean}. A daemon reaper thread sweeps expired entries
 * once per second to bound memory growth without forcing every request
 * to scan.</p>
 */
public final class SessionRegistry {

    /** Per-session record. {@code state} may implement
     *  {@link AutoCloseable} to receive an explicit close on eviction.
     *  {@code closed} guards against double-close from concurrent expiry
     *  sweeps + explicit teardown. */
    public static final class Entry {
        public final byte[] sessionId;
        public final Object state;
        public final long expiresAtSeconds;
        public final String principalKey;
        public final ReentrantLock lock;
        private final AtomicBoolean closed = new AtomicBoolean(false);

        Entry(byte[] sessionId, Object state, long expiresAtSeconds,
              String principalKey, ReentrantLock lock) {
            this.sessionId = sessionId;
            this.state = state;
            this.expiresAtSeconds = expiresAtSeconds;
            this.principalKey = principalKey;
            this.lock = lock;
        }

        public byte[] sessionId() { return sessionId.clone(); }
        public Object state() { return state; }
        public long expiresAtSeconds() { return expiresAtSeconds; }
        public String principalKey() { return principalKey; }
        public ReentrantLock lock() { return lock; }

        /** Invoke {@link AutoCloseable#close()} exactly once across all
         *  callers (reaper, explicit close, concurrent expiry). */
        void closeStateOnce() {
            if (closed.compareAndSet(false, true) && state instanceof AutoCloseable ac) {
                try { ac.close(); } catch (Exception ignore) { /* swallow */ }
            }
        }
    }

    private final ConcurrentHashMap<String, Entry> entries = new ConcurrentHashMap<>();
    private final AtomicBoolean draining = new AtomicBoolean(false);
    private final long defaultTtlSeconds;
    private final SecureRandom rng = new SecureRandom();
    private volatile Thread reaper;

    public SessionRegistry(long defaultTtlSeconds) {
        if (defaultTtlSeconds <= 0) {
            throw new IllegalArgumentException("defaultTtlSeconds must be > 0");
        }
        this.defaultTtlSeconds = defaultTtlSeconds;
    }

    public long defaultTtlSeconds() { return defaultTtlSeconds; }

    public boolean isDraining() { return draining.get(); }
    public void setDraining(boolean v) { draining.set(v); }

    /** Lazily start the reaper thread on first request. Hot path: a volatile
     *  read returns immediately once the reaper is up; the synchronized
     *  block runs at most once per process. */
    public void ensureReaperStarted() {
        if (reaper != null) return;
        synchronized (this) {
            if (reaper != null) return;
            Thread t = new Thread(this::reaperLoop, "vgi-rpc-session-reaper");
            t.setDaemon(true);
            t.start();
            reaper = t;
        }
    }

    private void reaperLoop() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            drainExpired();
        }
    }

    /**
     * Mint a fresh session id and return the live registry entry. Throws
     * {@link ServerDrainingError} when the server has entered drain mode.
     */
    public Entry open(Object state, Long ttlSecondsOrNull, String principalKey) {
        if (draining.get()) {
            throw new ServerDrainingError("server is draining; not accepting new sticky sessions");
        }
        byte[] sessionId = new byte[SessionToken.SESSION_ID_LEN];
        rng.nextBytes(sessionId);
        long ttl = ttlSecondsOrNull != null && ttlSecondsOrNull > 0
                ? ttlSecondsOrNull : defaultTtlSeconds;
        long expires = (System.currentTimeMillis() / 1000) + ttl;
        Entry e = new Entry(sessionId, state, expires, principalKey, new ReentrantLock());
        entries.put(hex(sessionId), e);
        return e;
    }

    /**
     * Look up a session. Returns {@code null} on miss / principal
     * mismatch / expiry (evicting the expired entry as a side-effect,
     * invoking {@link AutoCloseable#close()} if available).
     */
    public Entry get(byte[] sessionId, String principalKey) {
        String key = hex(sessionId);
        Entry e = entries.get(key);
        if (e == null) return null;
        long now = System.currentTimeMillis() / 1000;
        if (now > e.expiresAtSeconds) {
            if (entries.remove(key, e)) e.closeStateOnce();
            return null;
        }
        if (!e.principalKey.equals(principalKey)) {
            return null;
        }
        return e;
    }

    /**
     * Remove a session entry honoring the caller's principal: cross-
     * principal evictions are silently refused (defense-in-depth on top
     * of the AAD binding in the token). Returns {@code true} when an
     * entry was actually removed.
     */
    public boolean close(byte[] sessionId, String principalKey) {
        String key = hex(sessionId);
        Entry e = entries.get(key);
        if (e == null) return false;
        if (!e.principalKey.equals(principalKey)) return false;
        if (!entries.remove(key, e)) return false;
        e.closeStateOnce();
        return true;
    }

    /** Drop all entries past their TTL. Returns count evicted. */
    public int drainExpired() {
        long now = System.currentTimeMillis() / 1000;
        int evicted = 0;
        for (Iterator<Map.Entry<String, Entry>> it = entries.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, Entry> me = it.next();
            if (now > me.getValue().expiresAtSeconds) {
                it.remove();
                me.getValue().closeStateOnce();
                evicted++;
            }
        }
        return evicted;
    }

    /** Evict everything; called from HttpServer shutdown. */
    public void shutdown() {
        for (Entry e : entries.values()) e.closeStateOnce();
        entries.clear();
        if (reaper != null) reaper.interrupt();
    }

    public static String hex(byte[] b) {
        StringBuilder sb = new StringBuilder(b.length * 2);
        for (byte x : b) sb.append(String.format("%02x", x));
        return sb.toString();
    }
}
