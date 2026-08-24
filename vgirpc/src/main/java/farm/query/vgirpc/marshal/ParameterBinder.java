// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.marshal;

import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.RpcError;
import farm.query.vgirpc.schema.ArrowSerializableRecord;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Shared server-side parameter binding: converts a decoded kwargs map into a
 * positional {@code Object[]} suitable for reflective invocation of a service
 * method. Handles {@link CallContext} injection, {@link Optional} wrapping,
 * enum resolution, {@link ArrowSerializableRecord} deserialization, and
 * widening of {@link Number} values to the declared primitive type.
 *
 * <p>Used by both the pipe server ({@code RpcServer}) and the HTTP streaming
 * handler so that the two dispatch paths stay byte-identical.</p>
 */
public final class ParameterBinder {

    private ParameterBinder() {}

    /**
     * Build the positional argument array for {@code method.invoke(impl, args)}.
     *
     * @param method     the service {@link Method} whose argument array is being built
     * @param kwargs     decoded request parameters, keyed by parameter name
     * @param ctx        the {@link CallContext} to inject where declared
     * @throws RpcError  if a non-{@link Optional}, non-primitive parameter is missing
     */
    public static Object[] bind(Method method, Map<String, Object> kwargs, CallContext ctx) {
        Parameter[] params = method.getParameters();
        Object[] args = new Object[params.length];
        for (int i = 0; i < params.length; i++) {
            Parameter p = params[i];
            if (CallContext.class.isAssignableFrom(p.getType())) {
                args[i] = ctx;
                continue;
            }
            Object val = kwargs.get(p.getName());
            if (val == null && !kwargs.containsKey(p.getName())
                    && p.getType() != Optional.class) {
                throw new RpcError("MissingArgument",
                        "Missing required parameter '" + p.getName() + "' for method "
                                + method.getName(), "");
            }
            args[i] = adapt(val, p);
        }
        return args;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    static Object adapt(Object val, Parameter p) {
        return adaptType(val, p.getParameterizedType());
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static Object adaptType(Object val, Type type) {
        if (type instanceof ParameterizedType pt) {
            Class<?> raw = (Class<?>) pt.getRawType();
            if (raw == Optional.class) {
                return Optional.ofNullable(adaptType(val, pt.getActualTypeArguments()[0]));
            }
            if (List.class.isAssignableFrom(raw) && val instanceof List<?> list) {
                List<Object> out = new ArrayList<>(list.size());
                Type elem = pt.getActualTypeArguments()[0];
                for (Object item : list) out.add(adaptType(item, elem));
                return out;
            }
            if (Map.class.isAssignableFrom(raw) && val instanceof Map<?, ?> map) {
                Map<Object, Object> out = new LinkedHashMap<>(map.size());
                Type key = pt.getActualTypeArguments()[0];
                Type value = pt.getActualTypeArguments()[1];
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    out.put(adaptType(entry.getKey(), key), adaptType(entry.getValue(), value));
                }
                return out;
            }
            type = raw;
        }
        if (!(type instanceof Class<?> target)) return val;
        if (val == null) return null;
        if (target.isEnum() && val instanceof String s) {
            return Enum.valueOf((Class<Enum>) target.asSubclass(Enum.class), s);
        }
        if (ArrowSerializableRecord.class.isAssignableFrom(target) && val instanceof byte[] bytes) {
            return RecordCodec.deserializeFromBytes(bytes, (Class<? extends ArrowSerializableRecord>) target);
        }
        if (ArrowSerializableRecord.class.isAssignableFrom(target) && val instanceof Map<?, ?> map) {
            return RecordCodec.fromRowMap(
                    (Class<? extends ArrowSerializableRecord>) target, (Map<String, Object>) map);
        }
        return Numbers.coerce(target, val);
    }
}
