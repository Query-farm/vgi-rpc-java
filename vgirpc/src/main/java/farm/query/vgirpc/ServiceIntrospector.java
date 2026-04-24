// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

import farm.query.vgirpc.schema.SchemaDerivation;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Extract {@link RpcMethodInfo} for every method on a service interface. */
public final class ServiceIntrospector {

    private static final Map<Class<?>, Map<String, RpcMethodInfo>> CACHE = new ConcurrentHashMap<>();

    private ServiceIntrospector() {}

    public static Map<String, RpcMethodInfo> describe(Class<?> serviceInterface) {
        return CACHE.computeIfAbsent(serviceInterface, ServiceIntrospector::buildMethods);
    }

    private static Map<String, RpcMethodInfo> buildMethods(Class<?> iface) {
        if (!iface.isInterface()) {
            throw new IllegalArgumentException("service must be an interface: " + iface);
        }
        Map<String, RpcMethodInfo> out = new LinkedHashMap<>();
        for (Method m : iface.getMethods()) {
            if (m.isSynthetic() || m.isBridge() || Modifier.isStatic(m.getModifiers())) continue;
            if (m.getDeclaringClass() == Object.class) continue;
            if (m.getName().startsWith("_")) continue;
            out.put(m.getName(), build(iface, m));
        }
        return Collections.unmodifiableMap(out);
    }

    private static RpcMethodInfo build(Class<?> iface, Method m) {
        // params schema (skip a CallContext param — that's framework-injected, not on the wire)
        List<Field> fields = new ArrayList<>();
        Map<String, Type> paramTypes = new LinkedHashMap<>();
        boolean wantsCtx = false;
        for (Parameter p : m.getParameters()) {
            if (CallContext.class.isAssignableFrom(p.getType())) {
                wantsCtx = true;
                continue;
            }
            String name = p.getName();
            fields.add(SchemaDerivation.buildFieldForParameter(p));
            paramTypes.put(name, p.getParameterizedType());
        }
        Schema paramsSchema = new Schema(fields);

        Type returnType = m.getGenericReturnType();
        MethodType type;
        Schema resultSchema;
        boolean hasReturn;
        StreamKind streamKind = StreamKind.UNKNOWN;
        Class<?> headerType = extractHeaderType(m);
        if (isStreamReturn(returnType)) {
            type = MethodType.STREAM;
            resultSchema = RpcStream.EMPTY_SCHEMA;
            hasReturn = false;
            streamKind = inferStreamKind(returnType);
        } else if (returnType == void.class || returnType == Void.class) {
            type = MethodType.UNARY;
            resultSchema = RpcStream.EMPTY_SCHEMA;
            hasReturn = false;
        } else {
            type = MethodType.UNARY;
            resultSchema = new Schema(List.of(SchemaDerivation.buildResultField(m)));
            hasReturn = true;
        }
        return new RpcMethodInfo(m.getName(), m, paramsSchema, resultSchema, returnType,
                type, hasReturn, /*doc*/ null, paramTypes,
                wantsCtx, streamKind, headerType);
    }

    private static boolean isStreamReturn(Type t) {
        if (t == RpcStream.class) return true;
        if (t instanceof ParameterizedType pt) return pt.getRawType() == RpcStream.class;
        return false;
    }

    /**
     * Look for a {@code @StreamHeader(Type.class)} annotation on the method to declare
     * the header record type (Java's wildcard RpcStream generic can't carry it). Returns
     * {@code null} for streams without a header.
     */
    private static Class<?> extractHeaderType(java.lang.reflect.Method m) {
        farm.query.vgirpc.schema.StreamHeader ann = m.getAnnotation(farm.query.vgirpc.schema.StreamHeader.class);
        return ann != null ? ann.value() : null;
    }

    private static StreamKind inferStreamKind(Type t) {
        if (t instanceof ParameterizedType pt) {
            Type stateArg = pt.getActualTypeArguments()[0];
            Class<?> stateRaw;
            if (stateArg instanceof Class<?> c) stateRaw = c;
            else if (stateArg instanceof ParameterizedType inner) stateRaw = (Class<?>) inner.getRawType();
            else return StreamKind.UNKNOWN;
            if (ExchangeState.class.isAssignableFrom(stateRaw)) return StreamKind.EXCHANGE;
            if (ProducerState.class.isAssignableFrom(stateRaw)) return StreamKind.PRODUCER;
        }
        return StreamKind.UNKNOWN;
    }
}
