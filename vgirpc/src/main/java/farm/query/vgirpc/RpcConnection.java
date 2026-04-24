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

    public RpcConnection(RpcTransport transport) { this(transport, m -> {}); }

    public RpcConnection(RpcTransport transport, Consumer<Message> onLog) {
        this.transport = transport;
        this.onLog = onLog != null ? onLog : (m -> {});
    }

    @SuppressWarnings("unchecked")
    public <T> T proxy(Class<T> serviceInterface) {
        Map<String, RpcMethodInfo> methods = ServiceIntrospector.describe(serviceInterface);
        return (T) Proxy.newProxyInstance(
                serviceInterface.getClassLoader(),
                new Class<?>[]{serviceInterface},
                new ClientHandler(methods));
    }

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
                throw new UnsupportedOperationException("client-side streaming not yet implemented");
            }
            return doUnary(info, method, args);
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
                    return decodeResult(info, root);
                }
            }
        }

        private Map<String, Object> bindArgs(Method m, Object[] args) {
            Map<String, Object> out = new LinkedHashMap<>();
            Parameter[] params = m.getParameters();
            for (int i = 0; i < params.length; i++) {
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
            if (root.getRowCount() == 0) return null;
            Map<String, Object> row = Marshalling.decodeRow(root);
            Object value = row.get("result");
            if (value == null) return null;
            Class<?> returnRaw = rawClass(info.resultType());
            if (returnRaw == Optional.class) return Optional.ofNullable(value);
            if (returnRaw != null && returnRaw.isEnum() && value instanceof String s) {
                return Enum.valueOf((Class<Enum>) returnRaw.asSubclass(Enum.class), s);
            }
            if (returnRaw != null && ArrowSerializableRecord.class.isAssignableFrom(returnRaw)
                    && value instanceof byte[] bytes) {
                return RecordCodec.deserializeFromBytes(bytes, (Class<? extends ArrowSerializableRecord>) returnRaw);
            }
            if (returnRaw == int.class || returnRaw == Integer.class) return (int) ((Number) value).longValue();
            if (returnRaw == long.class || returnRaw == Long.class) return ((Number) value).longValue();
            if (returnRaw == float.class || returnRaw == Float.class) return ((Number) value).floatValue();
            if (returnRaw == double.class || returnRaw == Double.class) return ((Number) value).doubleValue();
            return value;
        }

        private Class<?> rawClass(java.lang.reflect.Type t) {
            if (t instanceof Class<?> c) return c;
            if (t instanceof java.lang.reflect.ParameterizedType pt) return (Class<?>) pt.getRawType();
            return null;
        }
    }
}
