// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

import farm.query.vgirpc.log.Message;
import farm.query.vgirpc.marshal.Marshalling;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.transport.RpcTransport;
import farm.query.vgirpc.wire.Allocators;
import farm.query.vgirpc.wire.IpcStreamReader;
import farm.query.vgirpc.wire.IpcStreamWriter;
import farm.query.vgirpc.wire.Wire;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.Proxy;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * A client-side RPC connection. Use {@link #proxy(Class)} to obtain a typed
 * dynamic-proxy implementation of a service interface that marshals each call
 * over the underlying {@link RpcTransport}.
 */
public final class RpcConnection implements AutoCloseable {

    private final RpcTransport transport;
    private final Consumer<Message> onLog;
    private final farm.query.vgirpc.external.LocationResolver locationResolver;

    /**
     * Create a connection over the given transport with no log sink and no
     * external-location resolution.
     *
     * @param transport the underlying transport (owned: closed by {@link #close()})
     */
    public RpcConnection(RpcTransport transport) { this(transport, m -> {}); }

    /**
     * Create a connection that forwards server-emitted log batches to {@code onLog}.
     *
     * @param transport the underlying transport (owned: closed by {@link #close()})
     * @param onLog sink for {@link Message} log batches received during calls; may be {@code null}
     */
    public RpcConnection(RpcTransport transport, Consumer<Message> onLog) {
        this(transport, onLog, null);
    }

    /**
     * Create a connection with log forwarding and transparent external-location
     * resolution. When {@code externalConfig} is supplied, pointer batches in
     * unary responses are fetched and decoded in place via a
     * {@link farm.query.vgirpc.external.LocationResolver}.
     *
     * @param transport the underlying transport (owned: closed by {@link #close()})
     * @param onLog sink for {@link Message} log batches; may be {@code null}
     * @param externalConfig external-storage configuration, or {@code null} to disable resolution
     */
    public RpcConnection(RpcTransport transport, Consumer<Message> onLog,
                         farm.query.vgirpc.external.ExternalLocationConfig externalConfig) {
        this.transport = transport;
        this.onLog = onLog != null ? onLog : (m -> {});
        this.locationResolver = externalConfig != null
                ? new farm.query.vgirpc.external.LocationResolver(externalConfig)
                : null;
    }

    /**
     * Create a typed dynamic proxy that implements {@code serviceInterface}. Each
     * method call is introspected via {@link ServiceIntrospector}, marshalled to
     * an Arrow params batch, and dispatched over the transport — unary calls
     * return the decoded result, streaming methods return a {@link ClientStreamSession}.
     *
     * @param serviceInterface the RPC service interface to implement
     * @param <T> the service type
     * @return a proxy instance bound to this connection
     */
    @SuppressWarnings("unchecked")
    public <T> T proxy(Class<T> serviceInterface) {
        Map<String, RpcMethodInfo> methods = ServiceIntrospector.describe(serviceInterface);
        return (T) Proxy.newProxyInstance(
                serviceInterface.getClassLoader(),
                new Class<?>[]{serviceInterface},
                new ClientHandler(methods));
    }

    /** Close the underlying transport. */
    @Override
    public void close() { transport.close(); }

    private final class ClientHandler implements InvocationHandler {

        private final Map<String, RpcMethodInfo> methods;

        ClientHandler(Map<String, RpcMethodInfo> methods) { this.methods = methods; }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (method.getDeclaringClass() == Object.class) {
                return method.invoke(this, args);
            }
            RpcMethodInfo info = methods.get(method.getName());
            if (info == null) throw new RpcError("AttributeError", "Unknown method: " + method.getName(), "");
            if (info.methodType() == MethodType.STREAM) {
                return doStream(info, method, args);
            }
            return doUnary(info, method, args);
        }

        private Object doStream(RpcMethodInfo info, Method m, Object[] args) throws Exception {
            Map<String, Object> kwargs = bindArgs(m, args);
            // Send request
            try (IpcStreamWriter w = new IpcStreamWriter(transport.writer())) {
                w.writeSchema(info.paramsSchema());
                Map<String, Object> wireKwargs = convertForWire(kwargs, info);
                if (info.paramsSchema().getFields().isEmpty()) {
                    try (VectorSchemaRoot zero = VectorSchemaRoot.create(info.paramsSchema(), Allocators.root())) {
                        zero.allocateNew();
                        zero.setRowCount(1);
                        w.writeBatch(zero, Wire.requestMetadata(info.name()));
                    }
                } else {
                    try (VectorSchemaRoot root = Marshalling.encodeRow(info.paramsSchema(), wireKwargs, Allocators.root())) {
                        w.writeBatch(root, Wire.requestMetadata(info.name()));
                    }
                }
            }
            transport.writer().flush();

            // Read header IPC stream if declared
            ArrowSerializableRecord header = null;
            Class<?> headerType = resolveHeaderType(info);
            if (headerType != null) {
                header = readHeaderStream(headerType);
            }

            // Client initially knows nothing about the stream's schemas; the first batch
            // received from the server carries them, and exchange inputs carry their own schema.
            return new ClientStreamSession<>(transport, RpcStream.EMPTY_SCHEMA, RpcStream.EMPTY_SCHEMA, header, onLog);
        }

        @SuppressWarnings("unchecked")
        private ArrowSerializableRecord readHeaderStream(Class<?> headerType) throws Exception {
            try (IpcStreamReader r = new IpcStreamReader(transport.reader(), Allocators.root())) {
                while (true) {
                    Map<String, String> md = r.readNextBatch();
                    if (md == null) throw new RpcError("ProtocolError", "header stream empty", "");
                    Wire.BatchKind kind = Wire.classify(r.root().getRowCount(), md);
                    if (kind == Wire.BatchKind.LOG) { onLog.accept(Wire.messageFromMetadata(md)); continue; }
                    if (kind == Wire.BatchKind.ERROR) throw Wire.errorFromMetadata(md);
                    Map<String, Object> row = Marshalling.decodeRow(r.root(), r.dictionaryProvider(), r.wireSchema());
                    ArrowSerializableRecord header = RecordCodec.fromRowMap(
                            (Class<? extends ArrowSerializableRecord>) headerType, row);
                    // Consume the header stream's trailing EOS so the main
                    // output stream that follows starts at a clean boundary.
                    drainQuietly(r);
                    return header;
                }
            }
        }

        private Class<?> resolveHeaderType(RpcMethodInfo info) {
            if (info.resultType() instanceof java.lang.reflect.ParameterizedType pt
                    && pt.getActualTypeArguments().length >= 2) {
                java.lang.reflect.Type h = pt.getActualTypeArguments()[1];
                if (h instanceof Class<?> c && ArrowSerializableRecord.class.isAssignableFrom(c)) return c;
            }
            return null;
        }

        private Object doUnary(RpcMethodInfo info, Method m, Object[] args) throws Exception {
            Map<String, Object> kwargs = bindArgs(m, args);

            // Send request
            try (IpcStreamWriter w = new IpcStreamWriter(transport.writer())) {
                w.writeSchema(info.paramsSchema());
                Map<String, Object> wireKwargs = convertForWire(kwargs, info);
                if (info.paramsSchema().getFields().isEmpty()) {
                    try (VectorSchemaRoot zero = VectorSchemaRoot.create(info.paramsSchema(), Allocators.root())) {
                        zero.allocateNew();
                        zero.setRowCount(1);
                        w.writeBatch(zero, Wire.requestMetadata(info.name()));
                    }
                } else {
                    try (VectorSchemaRoot root = Marshalling.encodeRow(info.paramsSchema(), wireKwargs, Allocators.root())) {
                        w.writeBatch(root, Wire.requestMetadata(info.name()));
                    }
                }
            }
            transport.writer().flush();

            // Read response
            try (IpcStreamReader r = new IpcStreamReader(transport.reader(), Allocators.root())) {
                while (true) {
                    Map<String, String> md = r.readNextBatch();
                    if (md == null) {
                        throw new RpcError("ProtocolError", "stream ended without response", "");
                    }
                    VectorSchemaRoot root = r.root();
                    Wire.BatchKind kind = Wire.classify(root.getRowCount(), md);
                    if (kind == Wire.BatchKind.LOG) {
                        onLog.accept(Wire.messageFromMetadata(md));
                        continue;
                    }
                    if (kind == Wire.BatchKind.ERROR) {
                        throw Wire.errorFromMetadata(md);
                    }
                    // Transparent resolution of external-location pointer batches.
                    if (locationResolver != null
                            && farm.query.vgirpc.external.LocationResolver.isPointer(root.getRowCount(), md)) {
                        farm.query.vgirpc.external.LocationResolver.Resolved resolved;
                        try {
                            resolved = locationResolver.resolve(md);
                        } catch (Exception fe) {
                            throw new RpcError("ExternalLocationError",
                                    "failed to resolve " + md.get(farm.query.vgirpc.wire.Metadata.LOCATION)
                                            + ": " + fe.getMessage(), "");
                        }
                        try {
                            Object result = decodeResult(info, resolved.root());
                            drainQuietly(r);
                            return result;
                        } finally {
                            resolved.root().close();
                        }
                    }
                    Object result = decodeResult(info, root);
                    drainQuietly(r);
                    return result;
                }
            }
        }

        /**
         * Consume the response stream's trailing EOS marker so a reused
         * (persistent) transport — subprocess / pipe / Unix socket, where
         * {@code transport.reader()} hands back the same stream every call —
         * presents a clean stream to the next call. Without this, the next
         * call's {@link IpcStreamReader} reads the stale EOS first and fails
         * with "Unexpected end of input. Missing schema".
         *
         * <p>Best-effort: a drain failure must never fail an
         * otherwise-successful call (mirrors {@code RpcServer}'s server-side
         * {@code reader.drain()} handling).
         */
        private static void drainQuietly(IpcStreamReader r) {
            try {
                r.drain();
            } catch (IOException ignore) {
                // Transport already drained / closed (e.g. HTTP, where each
                // request is its own connection) — nothing to clean up.
            }
        }

        private Map<String, Object> bindArgs(Method m, Object[] args) {
            Map<String, Object> out = new LinkedHashMap<>();
            Parameter[] params = m.getParameters();
            for (int i = 0; i < params.length; i++) {
                // Skip framework-injected CallContext parameters — they aren't on the wire.
                if (CallContext.class.isAssignableFrom(params[i].getType())) continue;
                Object v = args != null && i < args.length ? args[i] : null;
                if (v instanceof Optional<?> o) v = o.orElse(null);
                out.put(params[i].getName(), v);
            }
            return out;
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        private Map<String, Object> convertForWire(Map<String, Object> kwargs, RpcMethodInfo info) {
            Map<String, Object> out = new LinkedHashMap<>();
            for (Field f : info.paramsSchema().getFields()) {
                Object v = kwargs.get(f.getName());
                if (v instanceof Enum<?> e) v = e.name();
                else if (v instanceof ArrowSerializableRecord r
                        && f.getType() instanceof org.apache.arrow.vector.types.pojo.ArrowType.Binary) {
                    v = RecordCodec.serializeToBytes(r);
                }
                out.put(f.getName(), v);
            }
            return out;
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        private Object decodeResult(RpcMethodInfo info, VectorSchemaRoot root) {
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

        private Class<?> rawClass(java.lang.reflect.Type t) {
            if (t instanceof Class<?> c) return c;
            if (t instanceof java.lang.reflect.ParameterizedType pt) return (Class<?>) pt.getRawType();
            return null;
        }
    }
}
