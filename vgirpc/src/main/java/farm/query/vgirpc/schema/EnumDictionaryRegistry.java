// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.schema;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/** Allocates stable Arrow dictionary IDs for each enum class. */
public final class EnumDictionaryRegistry {

    private static final Map<Class<? extends Enum<?>>, Long> IDS = new ConcurrentHashMap<>();
    private static final AtomicLong NEXT = new AtomicLong(1);

    private EnumDictionaryRegistry() {}

    /**
     * Return the stable Arrow dictionary id for an enum class, allocating a new
     * one on first request.
     *
     * @param cls the enum class
     * @return the dictionary id (stable for the lifetime of the JVM)
     */
    public static long idFor(Class<? extends Enum<?>> cls) {
        return IDS.computeIfAbsent(cls, c -> NEXT.getAndIncrement());
    }
}
