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
import java.lang.reflect.WildcardType;
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

    /**
     * Introspect a service interface into a method table (results are cached
     * per interface).
     *
     * @param serviceInterface the service interface to describe
     * @return method name → {@link RpcMethodInfo}, in declaration order
     * @throws IllegalArgumentException if {@code serviceInterface} is not an interface
     */
    public static Map<String, RpcMethodInfo> describe(Class<?> serviceInterface) {
        return CACHE.computeIfAbsent(serviceInterface, ServiceIntrospector::buildMethods);
    }

    /**
     * The wire name of the protocol a service interface defines -- its routing key.
     *
     * <p>The interface's simple name, matching the reference, which uses the Protocol class name
     * when it declares no explicit {@code protocol_name}. Read from one place because the server
     * advertises it and the client stamps it on every request: derived separately on each side,
     * they would drift, and a drifted routing key fails as a 404 rather than as a type error.
     *
     * @param serviceInterface the service interface
     * @return the protocol name carried in {@code vgi_rpc.protocol} and in the HTTP path
     */
    public static String protocolName(Class<?> serviceInterface) {
        return serviceInterface.getSimpleName();
    }

    /**
     * The application protocol version a service interface declares via
     * {@link farm.query.vgirpc.schema.ProtocolVersion}.
     *
     * @param serviceInterface the service interface
     * @return the declared version, or {@code ""} when the interface declares none
     */
    public static String protocolVersion(Class<?> serviceInterface) {
        farm.query.vgirpc.schema.ProtocolVersion v =
                serviceInterface.getAnnotation(farm.query.vgirpc.schema.ProtocolVersion.class);
        return v == null || v.value() == null ? "" : v.value();
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
        long nextDictId = 0;
        for (Parameter p : m.getParameters()) {
            if (CallContext.class.isAssignableFrom(p.getType())) {
                wantsCtx = true;
                continue;
            }
            String name = p.getName();
            // Dictionary ids only have to be unique within this schema, and the
            // dictionary batch beside the params batch is keyed by them — so
            // number them by position among the dict-encoded parameters.
            Field pf = SchemaDerivation.buildFieldForParameter(p);
            if (SchemaDerivation.wantsParameterDictionary(p)) {
                pf = SchemaDerivation.withParameterDictionary(pf, p, nextDictId++);
            }
            fields.add(pf);
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

    /**
     * Classify a stream from its declared state type.
     *
     * <p>{@code stream_kind} is the only field in a description that says whether a stream
     * accepts input -- the description carries parameter, result and header schemas, but a
     * stream's <em>input</em> schema arrives at init time. So returning UNKNOWN when the answer
     * is in fact declared makes reflection unable to answer the question it exists for.
     *
     * <p>Handles the wildcard form, {@code RpcStream<? extends ProducerState>}, which is what
     * every method in this repo's conformance service declares: Java's generics give a
     * {@link WildcardType} there rather than a Class, and reading only the non-wildcard forms
     * discarded a declaration that was right in front of it.
     */
    private static StreamKind inferStreamKind(Type t) {
        if (t instanceof ParameterizedType pt) {
            Class<?> stateRaw = rawStateType(pt.getActualTypeArguments()[0]);
            if (stateRaw == null) return StreamKind.UNKNOWN;
            if (ExchangeState.class.isAssignableFrom(stateRaw)) return StreamKind.EXCHANGE;
            if (ProducerState.class.isAssignableFrom(stateRaw)) return StreamKind.PRODUCER;
        }
        return StreamKind.UNKNOWN;
    }

    /** The erased class behind a state type argument, or null when there is not one. */
    private static Class<?> rawStateType(Type stateArg) {
        if (stateArg instanceof Class<?> c) return c;
        if (stateArg instanceof ParameterizedType inner) return (Class<?>) inner.getRawType();
        if (stateArg instanceof WildcardType wildcard) {
            // `? extends ProducerState` -- the upper bound is the declaration.
            // A bare `?` bounds at Object, which classifies as neither, so it
            // still falls through to UNKNOWN.
            Type[] upper = wildcard.getUpperBounds();
            if (upper.length == 1) return rawStateType(upper[0]);
        }
        return null;
    }
}
