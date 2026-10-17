// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.marshal;

/**
 * Number-widening helpers shared by {@link ParameterBinder}, the record codec,
 * and client-side result decoding. Converts a boxed {@link Number} to the
 * declared primitive/boxed target type, matching Python's loose numeric
 * coercion on the wire.
 */
public final class Numbers {

    private Numbers() {}

    /**
     * Coerce {@code value} to {@code target}. Returns {@code value} unchanged
     * if {@code target} is not a recognised numeric type or {@code value} is
     * {@code null}. Callers are responsible for wrapping / null handling.
     */
    public static Object coerce(Class<?> target, Object value) {
        if (value == null) return null;
        if (!(value instanceof Number n)) return value;
        if (target == int.class    || target == Integer.class) return (int) n.longValue();
        if (target == long.class   || target == Long.class)    return n.longValue();
        if (target == double.class || target == Double.class)  return n.doubleValue();
        if (target == float.class  || target == Float.class)   return n.floatValue();
        if (target == short.class  || target == Short.class)   return (short) n.longValue();
        if (target == byte.class   || target == Byte.class)    return (byte) n.longValue();
        if (target == java.time.Instant.class) return microsToInstant(n.longValue());
        return value;
    }

    /**
     * Microseconds since the Unix epoch, as an {@link java.time.Instant} -- the decoding of a
     * {@code timestamp[us, tz=...]} column.
     *
     * @param micros microseconds since 1970-01-01T00:00:00Z
     * @return the instant
     */
    public static java.time.Instant microsToInstant(long micros) {
        return java.time.Instant.ofEpochSecond(
                Math.floorDiv(micros, 1_000_000L), Math.floorMod(micros, 1_000_000L) * 1_000L);
    }

    /**
     * An {@link java.time.Instant} as microseconds since the Unix epoch, truncating any
     * sub-microsecond part toward the past -- the encoding of a {@code timestamp[us, tz=...]}
     * column.
     *
     * @param instant the instant
     * @return microseconds since 1970-01-01T00:00:00Z
     * @throws ArithmeticException if the instant does not fit an int64 of microseconds
     */
    public static long instantToMicros(java.time.Instant instant) {
        return Math.addExact(Math.multiplyExact(instant.getEpochSecond(), 1_000_000L),
                instant.getNano() / 1_000L);
    }
}
