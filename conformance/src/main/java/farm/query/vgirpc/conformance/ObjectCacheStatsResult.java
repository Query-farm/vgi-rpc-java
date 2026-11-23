// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.conformance;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

/**
 * The worker process's object-cache counters ({@code object_cache_stats}).
 *
 * @param resident_bytes bytes of values currently retained
 * @param entries retained values
 * @param max_bytes the byte bound
 * @param hits lookups answered from the cache
 * @param misses lookups that fetched
 * @param evictions entries dropped to stay under the bound, or aged out
 * @param fetches fetches actually performed
 */
public record ObjectCacheStatsResult(long resident_bytes, long entries, long max_bytes, long hits,
                                     long misses, long evictions, long fetches)
        implements ArrowSerializableRecord {}
