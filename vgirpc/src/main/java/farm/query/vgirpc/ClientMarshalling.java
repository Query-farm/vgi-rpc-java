// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

import farm.query.vgirpc.marshal.Marshalling;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.wire.Allocators;
import farm.query.vgirpc.wire.IpcStreamWriter;
import farm.query.vgirpc.wire.Wire;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * The call-shaping half of a client: turning a Java method invocation into a
 * request IPC stream, and a response batch back into a typed return value.
 *
 * <p>This exists as its own type because there is more than one kind of client.
 * {@link RpcConnection} drives a byte-stream {@link farm.query.vgirpc.transport.RpcTransport};
 * {@link farm.query.vgirpc.http.HttpRpcConnection} drives request/response
 * POSTs. They differ entirely in <em>how</em> bytes move and not at all in
 * <em>what</em> the bytes are — the request framing, the {@code Optional}
 * unwrap/re-wrap, the enum and {@link ArrowSerializableRecord} conversions, and
 * the {@code @StreamHeader} resolution are one protocol, and a second copy of
 * them would be a second place for the wire shape to drift.</p>
 *
 * <p>Framework-internal: public only because the transports live in different
 * packages. Nothing here is part of the surface a service author calls.</p>
 */
public final class ClientMarshalling {

    private ClientMarshalling() {}

    /**
     * Write a complete request IPC stream (schema, one params batch carrying
     * {@code vgi_rpc.method} + the request version, then EOS) to {@code out}.
     *
     * <p>A method with no parameters still sends a <em>one-row</em> batch of the
     * empty schema, not a zero-row one: zero rows is the wire's "no data"
     * signal, and the server's request reader decodes an empty params map from
     * the row count rather than from the field list.</p>
     *
     * @param out destination for the framed request; not closed by this method
     * @param info the introspected method being called
     * @param method the reflected interface method, used to name the arguments
     * @param args the invocation arguments, positionally matching {@code method}
     * @throws IOException if {@code out} fails
     */
    public static void writeRequest(OutputStream out, RpcMethodInfo info, Method method, Object[] args)
            throws IOException {
        Map<String, Object> wireKwargs = convertForWire(bindArgs(method, args), info);
        try (IpcStreamWriter w = new IpcStreamWriter(out)) {
            w.writeSchema(info.paramsSchema());
            if (info.paramsSchema().getFields().isEmpty()) {
                try (VectorSchemaRoot zero = VectorSchemaRoot.create(info.paramsSchema(), Allocators.root())) {
                    zero.allocateNew();
                    zero.setRowCount(1);
                    w.writeBatch(zero, Wire.requestMetadata(info.name()));
                }
            } else {
                try (VectorSchemaRoot root =
                             Marshalling.encodeRow(info.paramsSchema(), wireKwargs, Allocators.root())) {
                    w.writeBatch(root, Wire.requestMetadata(info.name()));
                }
            }
        }
    }

    /**
     * Bind invocation arguments to their declared parameter names.
     *
     * <p>{@link CallContext} parameters are framework-injected on the server and
     * never travel, so they are skipped. An {@link Optional} argument is
     * unwrapped to its value or {@code null} — the wire has one representation
     * for absence, and {@link #decodeResult} performs the symmetric re-wrap.</p>
     *
     * @param method the reflected interface method
     * @param args the invocation arguments, or {@code null} for a no-arg call
     * @return parameter name to value, in declaration order
     */
    public static Map<String, Object> bindArgs(Method method, Object[] args) {
        Map<String, Object> out = new LinkedHashMap<>();
        Parameter[] params = method.getParameters();
        for (int i = 0; i < params.length; i++) {
            if (CallContext.class.isAssignableFrom(params[i].getType())) continue;
            Object v = args != null && i < args.length ? args[i] : null;
            if (v instanceof Optional<?> o) v = o.orElse(null);
            out.put(params[i].getName(), v);
        }
        return out;
    }

    /**
     * Project bound arguments onto the params schema's fields, converting the
     * Java-only shapes the schema declares as wire types: an enum becomes its
     * {@code name()}, and an {@link ArrowSerializableRecord} declared as a
     * binary field becomes its serialized bytes.
     *
     * @param kwargs the bound arguments from {@link #bindArgs}
     * @param info the introspected method whose params schema drives the projection
     * @return values keyed by wire field name, in schema field order
     */
    public static Map<String, Object> convertForWire(Map<String, Object> kwargs, RpcMethodInfo info) {
        Map<String, Object> out = new LinkedHashMap<>();
        for (Field f : info.paramsSchema().getFields()) {
            Object v = kwargs.get(f.getName());
            if (v instanceof Enum<?> e) {
                v = e.name();
            } else if (v instanceof ArrowSerializableRecord r
                    && f.getType() instanceof org.apache.arrow.vector.types.pojo.ArrowType.Binary) {
                v = RecordCodec.serializeToBytes(r);
            }
            out.put(f.getName(), v);
        }
        return out;
    }

    /**
     * Decode a unary response's data batch into the method's declared return type.
     *
     * @param info the introspected method being returned from
     * @param root the response envelope batch (a single {@code result} column)
     * @return the decoded value, {@code null} for a void method, or
     *     {@link Optional#empty()} when the method returns {@code Optional} and
     *     the server sent nothing
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public static Object decodeResult(RpcMethodInfo info, VectorSchemaRoot root) {
        if (!info.hasReturn()) return null;
        Class<?> returnRaw = rawClass(info.resultType());
        // A method declared to return Optional<T> must never hand back a raw
        // null — an absent value is Optional.empty(). (bindArgs unwraps
        // Optional args to null on the way out; this is the symmetric
        // re-wrap on the way back.)
        if (root.getRowCount() == 0) {
            return returnRaw == Optional.class ? Optional.empty() : null;
        }
        Map<String, Object> row = Marshalling.decodeRow(root);
        Object value = row.get("result");
        if (value == null) {
            return returnRaw == Optional.class ? Optional.empty() : null;
        }
        if (returnRaw == Optional.class) return Optional.ofNullable(value);
        if (returnRaw != null && returnRaw.isEnum() && value instanceof String s) {
            return Enum.valueOf((Class<Enum>) returnRaw.asSubclass(Enum.class), s);
        }
        if (returnRaw != null && ArrowSerializableRecord.class.isAssignableFrom(returnRaw)
                && value instanceof byte[] bytes) {
            return RecordCodec.deserializeFromBytes(bytes, (Class<? extends ArrowSerializableRecord>) returnRaw);
        }
        if (value instanceof Number) return farm.query.vgirpc.marshal.Numbers.coerce(returnRaw, value);
        return value;
    }

    /**
     * The stream's header record type, or {@code null} when it declares none.
     *
     * <p>{@code @StreamHeader} is the way a service declares this —
     * {@code RpcStream<S extends StreamState>} takes a single type parameter,
     * so the header type cannot ride the generic (see
     * {@code ServiceIntrospector.extractHeaderType}). The introspector has
     * already resolved the annotation into {@link RpcMethodInfo#headerType()};
     * consult that first.</p>
     *
     * <p>Reading a second type argument is kept as a fallback for any
     * hand-built {@link RpcMethodInfo}, but no method declared against
     * {@code RpcStream} can satisfy it. Preferring it was a latent bug: a
     * server writes the header stream whenever the annotation is present, so
     * a client that skipped it read the header batch as the stream's first
     * data batch.</p>
     *
     * @param info the introspected streaming method
     * @return the header record class, or {@code null}
     */
    public static Class<?> resolveHeaderType(RpcMethodInfo info) {
        if (info.headerType() != null
                && ArrowSerializableRecord.class.isAssignableFrom(info.headerType())) {
            return info.headerType();
        }
        if (info.resultType() instanceof ParameterizedType pt
                && pt.getActualTypeArguments().length >= 2) {
            Type h = pt.getActualTypeArguments()[1];
            if (h instanceof Class<?> c && ArrowSerializableRecord.class.isAssignableFrom(c)) return c;
        }
        return null;
    }

    private static Class<?> rawClass(Type t) {
        if (t instanceof Class<?> c) return c;
        if (t instanceof ParameterizedType pt) return (Class<?>) pt.getRawType();
        return null;
    }
}
