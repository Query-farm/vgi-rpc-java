// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.stateref;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.LongSupplier;

/**
 * The per-process object cache (STATE_REF_V1.md §11): verified object values keyed by
 * {@code object_id}, used by stored {@link StateRef}s and by argument references.
 *
 * <ol>
 *   <li><b>Keyed by {@code object_id}, never by a digest.</b> Grants mint objects for
 *       claimed hashes without seeing the bytes, so a digest-keyed cache would hand one
 *       principal's value to another who merely knows its hash.</li>
 *   <li><b>Single-flight:</b> concurrent resolves of one id share one fetch.</li>
 *   <li><b>Byte-bounded LRU</b> ({@value #OBJECT_CACHE_BYTES_ENV}, default
 *       {@code min(1 GiB, 40%} of the cgroup memory limit or of physical memory)). A value
 *       larger than the bound is returned but not retained, with a warning.</li>
 *   <li>Callers keep the {@code object_id}, never the value, so resident bytes stay
 *       bounded by this cache alone.</li>
 *   <li><b>{@code fetched_at}:</b> a hit on an entry fetched more than a day ago is a
 *       miss, so the cache never outlives the bucket lifecycle.</li>
 * </ol>
 *
 * <p>Thread-safe. Nothing here is visible on the wire.</p>
 */
public final class ObjectCache {

    private static final Logger LOG = LoggerFactory.getLogger(ObjectCache.class);

    /** Environment variable overriding the byte bound. */
    public static final String OBJECT_CACHE_BYTES_ENV = "VGI_RPC_OBJECT_CACHE_BYTES";
    /** An entry fetched longer ago than this many seconds is a miss (one day). */
    public static final long MAX_AGE_SECONDS = 86_400;
    private static final long ONE_GIB = 1L << 30;

    /**
     * A snapshot of the counters.
     *
     * @param residentBytes bytes of values currently retained
     * @param entries retained values
     * @param maxBytes the byte bound
     * @param hits lookups answered from the cache
     * @param misses lookups that fetched (including aged-out entries and single-flight joins)
     * @param evictions entries dropped to stay under the bound, or aged out
     * @param fetches fetches actually performed (single-flight joins excluded)
     */
    public record Stats(long residentBytes, long entries, long maxBytes, long hits, long misses,
                        long evictions, long fetches) {}

    /** The verified value for one id, and whether it came from the cache. */
    public record Lookup(byte[] value, boolean hit) {}

    /** Produces the verified value on a miss. */
    @FunctionalInterface
    public interface Fetch {
        /**
         * @return the verified, opened, decompressed bytes
         * @throws Exception any failure; every single-flight joiner sees it
         */
        byte[] fetch() throws Exception;
    }

    private record Entry(byte[] value, long fetchedAtMillis) {}

    private final long maxBytes;
    private final long maxAgeMillis;
    private final LongSupplier clockMillis;
    private final LinkedHashMap<String, Entry> entries = new LinkedHashMap<>(16, 0.75f, true);
    private final Map<String, CompletableFuture<byte[]>> flights = new HashMap<>();
    private long resident;
    private long hits;
    private long misses;
    private long evictions;
    private long fetches;

    /**
     * @param maxBytes the byte bound; {@code 0} retains nothing (every lookup fetches, still
     *     single-flight)
     */
    public ObjectCache(long maxBytes) {
        this(maxBytes, MAX_AGE_SECONDS, System::currentTimeMillis);
    }

    /**
     * @param maxBytes the byte bound
     * @param maxAgeSeconds entries fetched longer ago than this are misses
     * @param clockMillis wall clock in milliseconds, injectable for tests
     */
    public ObjectCache(long maxBytes, long maxAgeSeconds, LongSupplier clockMillis) {
        if (maxBytes < 0) throw new IllegalArgumentException("maxBytes must be >= 0");
        this.maxBytes = maxBytes;
        this.maxAgeMillis = maxAgeSeconds * 1000L;
        this.clockMillis = clockMillis;
    }

    /** @return the byte bound */
    public long maxBytes() {
        return maxBytes;
    }

    /** @return a snapshot of the counters */
    public synchronized Stats stats() {
        return new Stats(resident, entries.size(), maxBytes, hits, misses, evictions, fetches);
    }

    /** Drop every entry. */
    public synchronized void clear() {
        entries.clear();
        resident = 0;
    }

    /**
     * Move every entry's {@code fetched_at} back by {@code seconds} (test hook for the
     * one-day rule; the conformance method {@code object_cache_age}).
     *
     * @param seconds how far into the past
     * @return the number of entries shifted
     */
    public synchronized int ageEntries(long seconds) {
        for (Map.Entry<String, Entry> e : entries.entrySet()) {
            Entry v = e.getValue();
            e.setValue(new Entry(v.value(), v.fetchedAtMillis() - seconds * 1000L));
        }
        return entries.size();
    }

    /**
     * @param objectId the id
     * @return whether a lookup would hit (no counters change)
     */
    public synchronized boolean contains(String objectId) {
        Entry e = entries.get(objectId);
        return e != null && clockMillis.getAsLong() - e.fetchedAtMillis() <= maxAgeMillis;
    }

    /**
     * Return the value for {@code objectId}, fetching it at most once across threads.
     *
     * @param objectId the storage id (the key)
     * @param fetch called on a miss by exactly one caller; concurrent callers share its
     *     result or its exception
     * @return the value and whether it was a cache hit
     * @throws RuntimeException the fetch's exception (checked ones wrapped in
     *     {@link IllegalStateException})
     */
    public Lookup getOrFetch(String objectId, Fetch fetch) {
        CompletableFuture<byte[]> flight;
        boolean leader;
        synchronized (this) {
            Entry e = entries.get(objectId);
            if (e != null) {
                if (clockMillis.getAsLong() - e.fetchedAtMillis() <= maxAgeMillis) {
                    hits++;
                    LOG.debug("object_cache hit resident_bytes={}", resident);
                    return new Lookup(e.value(), true);
                }
                // Aged out: the object may already be gone; never serve it.
                entries.remove(objectId);
                resident -= e.value().length;
                evictions++;
            }
            misses++;
            flight = flights.get(objectId);
            leader = flight == null;
            if (leader) {
                flight = new CompletableFuture<>();
                flights.put(objectId, flight);
                fetches++;
            }
        }
        if (!leader) {
            try {
                return new Lookup(flight.get(), false);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted waiting for an object fetch");
            } catch (ExecutionException ee) {
                throw rethrow(ee.getCause());
            }
        }
        LOG.debug("object_cache miss resident_bytes={}", resident);
        byte[] value;
        try {
            value = fetch.fetch();
        } catch (Throwable t) {
            synchronized (this) {
                flights.remove(objectId);
            }
            flight.completeExceptionally(t);
            throw rethrow(t);
        }
        synchronized (this) {
            flights.remove(objectId);
            insertLocked(objectId, value);
        }
        flight.complete(value);
        return new Lookup(value, false);
    }

    private static RuntimeException rethrow(Throwable t) {
        if (t instanceof RuntimeException re) return re;
        if (t instanceof Error err) throw err;
        return new IllegalStateException(t.getClass().getSimpleName() + ": " + t.getMessage(), t);
    }

    private void insertLocked(String objectId, byte[] value) {
        long size = value.length;
        if (size > maxBytes) {
            LOG.warn("object_cache: a {}-byte value exceeds the cache bound ({} bytes, {}); resolved but not retained",
                    size, maxBytes, OBJECT_CACHE_BYTES_ENV);
            return;
        }
        Entry previous = entries.remove(objectId);
        if (previous != null) resident -= previous.value().length;
        entries.put(objectId, new Entry(value, clockMillis.getAsLong()));
        resident += size;
        Iterator<Map.Entry<String, Entry>> it = entries.entrySet().iterator();
        while (resident > maxBytes && it.hasNext()) {
            Map.Entry<String, Entry> oldest = it.next();
            resident -= oldest.getValue().value().length;
            it.remove();
            evictions++;
            LOG.debug("object_cache evict resident_bytes={}", resident);
        }
    }

    // -- process-wide default ------------------------------------------------

    private static ObjectCache defaultCache;

    /**
     * The process-wide cache, created on first use with its bound read from
     * {@value #OBJECT_CACHE_BYTES_ENV} (or the memory default).
     *
     * @return the shared cache
     * @throws IllegalArgumentException if the variable is not a non-negative integer
     */
    public static synchronized ObjectCache defaultCache() {
        if (defaultCache == null) defaultCache = new ObjectCache(defaultBytes(System.getenv()));
        return defaultCache;
    }

    /**
     * Install the process-wide cache explicitly (hosts and test workers that configure the
     * bound in code rather than through {@value #OBJECT_CACHE_BYTES_ENV}). Must run before the
     * first {@link #defaultCache()} call to take effect for servers built afterwards.
     *
     * @param cache the cache to share
     */
    public static synchronized void installDefault(ObjectCache cache) {
        defaultCache = java.util.Objects.requireNonNull(cache, "cache");
    }

    /**
     * The cache bound for {@code env}: the override, else {@code min(1 GiB, 40%} of the
     * cgroup memory limit, else of physical memory{@code )}.
     *
     * @param env the environment
     * @return the bound in bytes
     * @throws IllegalArgumentException if the variable is set but is not a non-negative integer
     */
    public static long defaultBytes(Map<String, String> env) {
        String raw = env.get(OBJECT_CACHE_BYTES_ENV);
        if (raw != null && !raw.isBlank()) {
            long value;
            try {
                value = Long.parseLong(raw.strip());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(OBJECT_CACHE_BYTES_ENV
                        + " must be a non-negative integer, got '" + raw + "'");
            }
            if (value < 0) {
                throw new IllegalArgumentException(OBJECT_CACHE_BYTES_ENV
                        + " must be a non-negative integer, got '" + raw + "'");
            }
            return value;
        }
        Long memory = cgroupMemoryLimit();
        if (memory == null) memory = physicalMemory();
        if (memory == null) return ONE_GIB;
        return Math.min(ONE_GIB, memory / 100 * 40);
    }

    private static Long cgroupMemoryLimit() {
        for (String p : new String[] {"/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"}) {
            String raw;
            try {
                raw = Files.readString(Path.of(p)).strip();
            } catch (IOException | RuntimeException e) {
                continue;
            }
            if (raw.isEmpty() || raw.equals("max")) return null;
            try {
                long v = Long.parseLong(raw);
                if (v <= 0 || v >= (1L << 60)) return null;
                return v;
            } catch (NumberFormatException e) {
                // try the next file
            }
        }
        return null;
    }

    private static Long physicalMemory() {
        try {
            if (ManagementFactory.getOperatingSystemMXBean()
                    instanceof com.sun.management.OperatingSystemMXBean os) {
                long total = os.getTotalMemorySize();
                return total > 0 ? total : null;
            }
        } catch (RuntimeException | LinkageError e) {
            // unavailable
        }
        return null;
    }
}
