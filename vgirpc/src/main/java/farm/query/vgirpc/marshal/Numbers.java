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
        return value;
    }
}
