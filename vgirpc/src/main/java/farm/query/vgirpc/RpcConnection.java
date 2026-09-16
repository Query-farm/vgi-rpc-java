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

import farm.query.vgirpc.external.LocationResolver;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.NoSuchElementException;
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
        // The interface's declared @ProtocolVersion rides every request: a
        // versioned peer checks the key at its dispatch boundary and rejects a
        // request that carries none, so omitting it is not leniency — it makes
        // the client unable to call that peer at all.
        String version = ServiceIntrospector.protocolVersion(serviceInterface);
        // The routing key rides every request too. Dispatch resolves the pair
        // (protocol, method): on this transport the metadata field is the only carrier there
        // is, and a peer that requires it -- as it must, since an intermediary dropping the
        // field would otherwise land silently on whichever protocol was registered first --
        // cannot be called at all by a client that omits it.
        String protocol = ServiceIntrospector.protocolName(serviceInterface);
        return (T) Proxy.newProxyInstance(
                serviceInterface.getClassLoader(),
                new Class<?>[]{serviceInterface},
                new ClientHandler(methods, protocol, version));
    }

    // ------------------------------------------------------------------
    // Untyped surface
    // ------------------------------------------------------------------

    /**
     * Call a unary method named by string, relaying a caller-supplied batch.
     *
     * <p>The untyped counterpart of a call made through {@link #proxy(Class)}:
     * the same request framing, the same routing key, the same read loop — log
     * batches to the {@code onLog} sink, an {@code EXCEPTION} batch raised as
     * {@link RpcError}, an externalized pointer resolved (or refused when this
     * connection has no {@code ExternalLocationConfig}) — differing only in
     * that nothing is decoded into a Java value at either end.</p>
     *
     * @param protocol the routing key to address; {@code null} or blank omits it,
     *     which a peer that requires one will refuse
     * @param protocolVersion the application protocol version to stamp, or {@code null}
     * @param method the RPC method name
     * @param request the request batch and the Arrow custom metadata to send with it
     * @return the reply batch framed as a one-batch Arrow IPC stream
     * @throws RpcError on a transport failure or a peer-reported error
     */
    public byte[] callUnaryRaw(String protocol, String protocolVersion, String method,
                               AnnotatedBatch request) {
        try {
            ClientMarshalling.writeRawRequest(transport.writer(), method, request.root(),
                    request.dictionaryProvider(), request.customMetadata(), protocol, protocolVersion);
            transport.writer().flush();
            return readUnaryResponse(Wire::writeOneBatch);
        } catch (RpcError e) {
            throw e;
        } catch (IOException e) {
            throw new RpcError("TransportError", method + ": " + e.getMessage(), "");
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RpcError("TransportError", method + ": " + e, "");
        }
    }

    /**
     * Open a streaming call named by string, relaying a caller-supplied batch.
     *
     * <p>{@code hasHeader} is not inferrable from the wire — the worker writes a
     * header stream ahead of the body exactly when the method declares one, and
     * a client that guesses wrong reads the header batch as the stream's first
     * data batch (or blocks waiting for a header nobody will send). It is a
     * property of the method's declaration, so the caller must supply it.</p>
     *
     * @param protocol the routing key to address
     * @param protocolVersion the application protocol version to stamp, or {@code null}
     * @param method the RPC method name
     * @param request the request batch and the Arrow custom metadata to send with it
     * @param hasHeader whether the method declares a {@code @StreamHeader}
     * @return the open stream
     * @throws RpcError on a transport failure or a peer-reported error while opening
     */
    public RawStream openStreamRaw(String protocol, String protocolVersion, String method,
                                   AnnotatedBatch request, boolean hasHeader) {
        byte[] header;
        try {
            ClientMarshalling.writeRawRequest(transport.writer(), method, request.root(),
                    request.dictionaryProvider(), request.customMetadata(), protocol, protocolVersion);
            transport.writer().flush();
            header = hasHeader ? readRawHeaderStream() : null;
        } catch (RpcError e) {
            throw e;
        } catch (IOException e) {
            throw new RpcError("TransportError", method + ": " + e.getMessage(), "");
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RpcError("TransportError", method + ": " + e, "");
        }
        ClientStreamSession<StreamState> session = new ClientStreamSession<>(
                transport, RpcStream.EMPTY_SCHEMA, RpcStream.EMPTY_SCHEMA, null, onLog, locationResolver);
        return new RawByteStream(session, header);
    }

    /** Read the header IPC stream without decoding it, returning its single batch reframed. */
    private byte[] readRawHeaderStream() throws IOException {
        try (IpcStreamReader r = new IpcStreamReader(transport.reader(), Allocators.root())) {
            while (true) {
                Map<String, String> md = r.readNextBatch();
                if (md == null) throw new RpcError("ProtocolError", "header stream empty", "");
                Wire.BatchKind kind = Wire.classify(r.root().getRowCount(), md);
                if (kind == Wire.BatchKind.LOG) { onLog.accept(Wire.messageFromMetadata(md)); continue; }
                if (kind == Wire.BatchKind.ERROR) {
                    RpcError error = Wire.errorFromMetadata(md);
                    drainQuietly(r);
                    throw error;
                }
                byte[] framed = Wire.writeOneBatch(r.root(), md, r.dictionaryProvider());
                // Consume the header stream's trailing EOS so the body stream
                // that follows starts at a clean boundary.
                drainQuietly(r);
                return framed;
            }
        }
    }

    /** Projects one unary response data batch onto a caller-chosen representation. */
    private interface UnaryProjection<T> {
        T apply(VectorSchemaRoot root, Map<String, String> customMetadata,
                org.apache.arrow.vector.dictionary.DictionaryProvider dictionaries) throws IOException;
    }

    /**
     * Read one unary response, applying the log / error / external-pointer
     * semantics every unary call shares, and project its data batch.
     *
     * <p>Shared by the typed and untyped paths so the two cannot drift in what
     * they do with a log line, an error batch or a pointer — the parts that are
     * protocol rather than presentation.</p>
     */
    private <T> T readUnaryResponse(UnaryProjection<T> projection) throws IOException {
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
                    // Drain before raising. An error batch is followed by the
                    // response stream's end-of-stream marker just like a result
                    // batch is, and on a persistent transport -- subprocess,
                    // pipe, Unix or TCP socket, where `transport.reader()` hands
                    // back the same stream every call -- leaving it unread
                    // poisons the *connection*, not just this call: the next
                    // call's reader sees the stale EOS first and fails with
                    // "Unexpected end of input. Missing schema".
                    //
                    // The failure therefore surfaces on some later, unrelated
                    // call, which is why it survived: a test that provokes one
                    // error and stops looks perfectly healthy.
                    RpcError error = Wire.errorFromMetadata(md);
                    drainQuietly(r);
                    throw error;
                }
                // Transparent resolution of external-location pointer batches.
                if (LocationResolver.isPointer(root.getRowCount(), md)) {
                    String safeUrl = LocationResolver.redactUrl(
                            md.get(farm.query.vgirpc.wire.Metadata.LOCATION));
                    if (locationResolver == null) {
                        throw new RpcError("ExternalLocationError",
                                "response contained an externalized batch ("
                                        + farm.query.vgirpc.wire.Metadata.LOCATION + "=" + safeUrl
                                        + ") but this connection has no ExternalLocationConfig", "");
                    }
                    LocationResolver.Resolved resolved;
                    try {
                        resolved = locationResolver.resolve(md);
                    } catch (Exception fe) {
                        throw new RpcError("ExternalLocationError",
                                "failed to resolve " + safeUrl + " ("
                                        + fe.getClass().getSimpleName() + ")", "");
                    }
                    try {
                        T out = projection.apply(resolved.root(), resolved.customMetadata(), resolved.dictionaries());
                        drainQuietly(r);
                        return out;
                    } finally {
                        resolved.close();
                    }
                }
                T out = projection.apply(root, md, r.dictionaryProvider());
                drainQuietly(r);
                return out;
            }
        }
    }

    /**
     * Consume a response stream's trailing EOS marker so a reused (persistent)
     * transport presents a clean stream to the next call. Best-effort: a drain
     * failure must never fail an otherwise-successful call.
     */
    private static void drainQuietly(IpcStreamReader r) {
        try {
            r.drain();
        } catch (IOException ignore) {
            // Transport already drained / closed — nothing to clean up.
        }
    }

    /** {@link RawStream} over a byte-stream {@link ClientStreamSession}. */
    private static final class RawByteStream implements RawStream {

        private final ClientStreamSession<StreamState> session;
        private final byte[] header;

        RawByteStream(ClientStreamSession<StreamState> session, byte[] header) {
            this.session = session;
            this.header = header;
        }

        @Override public byte[] header() { return header; }

        @Override
        public byte[] tick(Map<String, String> customMetadata) {
            try {
                return frame(session.tick(customMetadata));
            } catch (NoSuchElementException e) {
                return null;
            }
        }

        @Override
        public byte[] exchange(AnnotatedBatch input) {
            try {
                return frame(session.exchange(input));
            } catch (NoSuchElementException e) {
                return null;
            }
        }

        /**
         * Always {@code null}: a byte-stream transport carries no resumable
         * stream state, because the stream is the connection.
         */
        @Override public String stateToken() { return null; }

        @Override public void cancel() { session.cancel(); }

        @Override public void close() { session.close(); }

        private static byte[] frame(AnnotatedBatch batch) {
            try {
                return Wire.writeOneBatch(batch.root(), batch.customMetadata(), batch.dictionaryProvider());
            } catch (IOException e) {
                throw new RpcError("TransportError", "could not reframe stream batch: " + e.getMessage(), "");
            }
        }
    }

    /** Close the underlying transport. */
    @Override
    public void close() { transport.close(); }

    private final class ClientHandler implements InvocationHandler {

        private final Map<String, RpcMethodInfo> methods;
        private final String protocol;
        private final String protocolVersion;

        ClientHandler(Map<String, RpcMethodInfo> methods, String protocol, String protocolVersion) {
            this.methods = methods;
            this.protocol = protocol;
            this.protocolVersion = protocolVersion;
        }

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
            ClientMarshalling.writeRequest(transport.writer(), info, m, args, protocol, protocolVersion);
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
                    if (kind == Wire.BatchKind.ERROR) {
                        RpcError error = Wire.errorFromMetadata(md);
                        drainQuietly(r);
                        throw error;
                    }
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
            ClientMarshalling.writeRequest(transport.writer(), info, m, args, protocol, protocolVersion);
            transport.writer().flush();

            // Read response. The log / error / external-pointer semantics live in
            // readUnaryResponse, shared with the untyped surface, so the two paths
            // cannot drift in the parts that are protocol rather than presentation.
            return readUnaryResponse((root, md, dicts) -> ClientMarshalling.decodeResult(info, root));
        }

    }
}
