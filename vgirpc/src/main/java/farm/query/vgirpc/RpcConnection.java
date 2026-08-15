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
import farm.query.vgirpc.wire.Wire;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Map;
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
     * resolution. When {@code externalConfig} is supplied, pointer batches are
     * fetched and decoded in place via a
     * {@link farm.query.vgirpc.external.LocationResolver} — in unary responses
     * and on streaming output alike.
     *
     * <p>Without it, an externalised batch cannot be materialised at all;
     * rather than surface a zero-row batch as if the server had sent no data,
     * both paths fail loudly (see
     * {@code ClientStreamSession.resolvePointerBatch}).</p>
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
            // Send request
            ClientMarshalling.writeRequest(transport.writer(), info, m, args);
            transport.writer().flush();

            // Read header IPC stream if declared
            ArrowSerializableRecord header = null;
            Class<?> headerType = ClientMarshalling.resolveHeaderType(info);
            if (headerType != null) {
                header = readHeaderStream(headerType);
            }

            // Client initially knows nothing about the stream's schemas; the first batch
            // received from the server carries them, and exchange inputs carry their own schema.
            // The resolver goes with it: a producer stream is where externalisation actually
            // bites (large scan results), so resolving only unary responses would leave the
            // main data path handing back empty batches for every externalised one.
            return new ClientStreamSession<>(transport, RpcStream.EMPTY_SCHEMA, RpcStream.EMPTY_SCHEMA,
                    header, onLog, locationResolver);
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

        private Object doUnary(RpcMethodInfo info, Method m, Object[] args) throws Exception {
            // Send request
            ClientMarshalling.writeRequest(transport.writer(), info, m, args);
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
                            Object result = ClientMarshalling.decodeResult(info, resolved.root());
                            drainQuietly(r);
                            return result;
                        } finally {
                            resolved.root().close();
                        }
                    }
                    Object result = ClientMarshalling.decodeResult(info, root);
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

    }
}
