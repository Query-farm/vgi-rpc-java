// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

import farm.query.vgirpc.external.ExternalLocationConfig;
import farm.query.vgirpc.external.Externalizer;
import farm.query.vgirpc.external.LocationResolver;
import farm.query.vgirpc.http.SessionScope;
import farm.query.vgirpc.log.Message;
import farm.query.vgirpc.marshal.Marshalling;
import farm.query.vgirpc.marshal.ParameterBinder;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.schema.SchemaDerivation;
import farm.query.vgirpc.shm.ShmResolver;
import farm.query.vgirpc.shm.Shm;
import farm.query.vgirpc.shm.ShmSession;
import farm.query.vgirpc.transport.RpcTransport;
import farm.query.vgirpc.wire.Allocators;
import farm.query.vgirpc.wire.IpcStreamReader;
import farm.query.vgirpc.wire.IpcStreamWriter;
import farm.query.vgirpc.wire.Metadata;
import farm.query.vgirpc.wire.Wire;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;

/** Dispatches RPC requests to a service implementation over an {@link RpcTransport}. */
public final class RpcServer {

    private static final Logger LOG = LoggerFactory.getLogger(RpcServer.class);

    private final Class<?> serviceInterface;
    private final Object impl;
    private final String serverId;
    private final Map<String, RpcMethodInfo> methods;
    private final boolean describeEnabled;
    private LocationResolver locationResolver;
    private ExternalLocationConfig externalConfig;
    private DispatchHook dispatchHook;
    private String protocolVersion = "";
    private String protocolHash = "";

    /**
     * Create a server with a random 12-char server id and {@code __describe__} enabled.
     *
     * @param serviceInterface the service interface introspected for method schemas
     * @param impl the implementation instance calls are dispatched to
     */
    public RpcServer(Class<?> serviceInterface, Object impl) {
        this(serviceInterface, impl, UUID.randomUUID().toString().replace("-", "").substring(0, 12), true);
    }

    /**
     * Create a server with an explicit server id.
     *
     * @param serviceInterface the service interface introspected for method schemas
     * @param impl the implementation instance calls are dispatched to
     * @param serverId stable identifier echoed in response metadata
     * @param enableDescribe whether to answer the built-in {@code __describe__} introspection call
     */
    public RpcServer(Class<?> serviceInterface, Object impl, String serverId, boolean enableDescribe) {
        this.serviceInterface = serviceInterface;
        this.impl = impl;
        this.serverId = serverId;
        this.methods = new LinkedHashMap<>(ServiceIntrospector.describe(serviceInterface));
        this.describeEnabled = enableDescribe;
    }

    /** Attach an external-location resolver so streaming inputs with pointer batches are fetched transparently. */
    public void setLocationResolver(LocationResolver r) { this.locationResolver = r; }

    /** Configure outgoing-batch externalisation. Uploaded via {@link farm.query.vgirpc.external.ExternalStorage}. */
    public void setExternalConfig(ExternalLocationConfig cfg) { this.externalConfig = cfg; }

    /** Read-only accessor used by the HTTP transport to decide whether the
     *  server should advertise {@code VGI-Externalization-Enabled: true}. */
    public ExternalLocationConfig externalConfig() { return externalConfig; }

    /** Install an observability hook fired around each RPC dispatch. */
    public void setDispatchHook(DispatchHook hook) { this.dispatchHook = hook; }

    /** Operator-supplied free-form protocol-contract version label (optional). */
    public void setProtocolVersion(String v) { this.protocolVersion = v == null ? "" : v; }
    /** The operator-supplied protocol-contract version label, or {@code ""} if unset. */
    public String protocolVersion() { return protocolVersion; }

    /** SHA-256 hex digest of the canonical __describe__ payload. */
    public String protocolHash() {
        if (protocolHash.isEmpty()) {
            Introspect.Built b = Introspect.build(protocolName(), methods, serverId, protocolVersion);
            try {
                protocolHash = b.customMetadata().getOrDefault(Metadata.PROTOCOL_HASH_KEY, "");
            } finally {
                b.root().close();
            }
        }
        return protocolHash;
    }

    /** The stable server identifier echoed in response metadata. */
    public String serverId() { return serverId; }
    /** The protocol name advertised in {@code __describe__}; the service interface's simple name. */
    public String protocolName() { return serviceInterface.getSimpleName(); }
    /** Introspected method table keyed by method name (unmodifiable). */
    public Map<String, RpcMethodInfo> methods() { return Collections.unmodifiableMap(methods); }

    /** Underlying service implementation (exposed for the HTTP streaming handler). */
    public Object implementation() { return impl; }

    /**
     * Loop serving requests until the transport closes. A single shared-memory
     * session is held for the lifetime of the connection. Recoverable per-call
     * errors are reported back to the client as error streams without tearing
     * down the loop.
     *
     * @param transport the transport whose request/response streams are served
     */
    public void serve(RpcTransport transport) {
        // One shared-memory session per connection: lazily attaches when the
        // client advertises a segment, and is munmap'd/closed when the loop
        // exits (never unlinked — the client owns the segment).
        try (ShmSession shm = new ShmSession()) {
            while (true) {
                try {
                    serveOne(transport, shm);
                } catch (EndOfStream e) {
                    return;
                } catch (Throwable t) {
                    // Don't take the whole loop down on a single bad request — try to surface
                    // the error to the client and continue serving subsequent calls.
                    LOG.warn("recoverable serve error: {}", t.toString(), t);
                    t.printStackTrace(System.err);
                    try {
                        Wire.writeErrorStream(transport.writer(), RpcStream.EMPTY_SCHEMA, t, serverId);
                        transport.writer().flush();
                    } catch (Exception ignore) {
                        // If we can't even write back, the transport is dead — exit.
                        return;
                    }
                }
            }
        }
    }

    private static final class EndOfStream extends RuntimeException {}

    /**
     * Serialize a parameters {@link VectorSchemaRoot} (with its custom metadata) into a
     * self-contained Arrow IPC stream — schema message followed by one record batch
     * message — for inclusion in access-log {@code request_data}.
     *
     * <p>Best-effort: returns {@code null} on any failure so observability never fails
     * dispatch.
     */
    private static byte[] serializeRequestBatch(VectorSchemaRoot root, Map<String, String> meta) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (IpcStreamWriter w = new IpcStreamWriter(baos)) {
                w.writeBatch(root, meta);
                w.writeEos();
            }
            return baos.toByteArray();
        } catch (Throwable t) {
            return null;
        }
    }

    /**
     * Handle exactly one RPC call (no shared-memory session — used by the HTTP transport).
     *
     * @param transport the transport whose next request stream is read and answered
     */
    public void serveOne(RpcTransport transport) {
        serveOne(transport, null);
    }

    /** Handle exactly one RPC call, attaching/using the connection's shm session if present. */
    private void serveOne(RpcTransport transport, ShmSession shmSession) {
        try (IpcStreamReader reader = new IpcStreamReader(transport.reader(), Allocators.root())) {
            Map<String, String> meta;
            try {
                meta = reader.readNextBatch();
            } catch (IOException ioe) {
                throw new EndOfStream();
            }
            if (meta == null) {
                throw new EndOfStream();
            }
            if (shmSession != null) shmSession.attachIfAdvertised(meta);
            Shm shm = shmSession != null ? shmSession.segment() : null;
            VectorSchemaRoot paramsRoot = reader.root();
            // If the outer batch is a shm or external-location pointer, resolve the
            // inner batch and decode kwargs from it. Dispatch metadata still comes
            // from the (merged) outer batch metadata.
            VectorSchemaRoot resolvedParams = null;
            try {
                if (shm != null && ShmResolver.isPointer(paramsRoot.getRowCount(), meta)) {
                    try {
                        shm.inShmBatches++;
                        shm.inShmBytes += Long.parseLong(meta.get(Metadata.SHM_LENGTH));
                        ShmResolver.Resolved res = ShmResolver.resolve(shm, paramsRoot, meta);
                        resolvedParams = res.root();
                        paramsRoot = resolvedParams;
                        meta = res.customMetadata();
                    } catch (Exception shmExc) {
                        Wire.writeErrorStream(transport.writer(), RpcStream.EMPTY_SCHEMA, shmExc, serverId);
                        transport.writer().flush();
                        return;
                    }
                } else if (locationResolver != null && LocationResolver.isPointer(paramsRoot.getRowCount(), meta)) {
                    try {
                        LocationResolver.Resolved res = locationResolver.resolve(meta);
                        resolvedParams = res.root();
                        paramsRoot = resolvedParams;
                    } catch (Exception fetchExc) {
                        Wire.writeErrorStream(transport.writer(), RpcStream.EMPTY_SCHEMA, fetchExc, serverId);
                        transport.writer().flush();
                        return;
                    }
                } else if (ShmResolver.isPointer(paramsRoot.getRowCount(), meta)) {
                    // An inbound shm pointer arrived but no segment is attached. This
                    // can only happen if the client used shm without honoring the
                    // __transport_options__ handshake (e.g. a worker that reported
                    // shm-unavailable, or a capable worker whose attach failed). Fail
                    // loudly rather than silently decoding the 0-row pointer as empty.
                    Wire.writeErrorStream(transport.writer(), RpcStream.EMPTY_SCHEMA,
                            new IOException("received shm pointer batch but no segment is attached "
                                    + "(transport negotiation mismatch)"), serverId);
                    transport.writer().flush();
                    return;
                }
                // Snapshot the kwargs immediately, before draining mutates the reader's root.
                // Decode errors must be reported as an error response so the (possibly shared)
                // transport stays framed correctly for the next call.
                Map<String, Object> kwargsSnapshot;
                try {
                    kwargsSnapshot = paramsRoot.getRowCount() == 0
                            ? new LinkedHashMap<>()
                            : (resolvedParams != null
                                ? Marshalling.decodeRow(paramsRoot, null, paramsRoot.getSchema())
                                : Marshalling.decodeRow(paramsRoot, reader.dictionaryProvider(), reader.wireSchema()));
                } catch (Exception decodeExc) {
                    try { reader.drain(); } catch (IOException ignore) {}
                    Wire.writeErrorStream(transport.writer(), RpcStream.EMPTY_SCHEMA, decodeExc, serverId);
                    transport.writer().flush();
                    return;
                }
                // Drain remaining batches in this request stream so the next call sees a fresh stream
                try { reader.drain(); } catch (IOException ignore) {}
                String method;
                try {
                    Wire.validateRequestVersion(meta);
                    method = Wire.requireMethodName(meta);
                } catch (RuntimeException pe) {
                    Wire.writeErrorStream(transport.writer(), RpcStream.EMPTY_SCHEMA, pe, serverId);
                    transport.writer().flush();
                    return;
                }
                if (describeEnabled && Introspect.METHOD_NAME.equals(method)) {
                    serveDescribe(transport);
                    return;
                }
                if (TransportOptions.METHOD_NAME.equals(method)) {
                    serveTransportOptions(transport);
                    return;
                }
                RpcMethodInfo info = methods.get(method);
                if (info == null) {
                    Wire.writeErrorStream(transport.writer(), RpcStream.EMPTY_SCHEMA,
                            new IllegalArgumentException("Unknown method: '" + method + "'. Available: " + methods.keySet()),
                            serverId);
                    transport.writer().flush();
                    return;
                }
                Map<String, Object> kwargs = kwargsSnapshot;

                // Build dispatch info + invoke hook
                DispatchInfo dispatchInfo = null;
                CallStatistics callStats = null;
                Object hookToken = null;
                Throwable handlerErr = null;
                if (dispatchHook != null) {
                    dispatchInfo = new DispatchInfo();
                    dispatchInfo.method = method;
                    dispatchInfo.methodType = info.methodType() == MethodType.UNARY ? "unary" : "stream";
                    dispatchInfo.serverId = serverId;
                    dispatchInfo.protocol = protocolName();
                    dispatchInfo.protocolHash = protocolHash();
                    dispatchInfo.protocolVersion = protocolVersion;
                    AuthScope.Scope scope = AuthScope.current();
                    dispatchInfo.principal = scope.auth() != null && scope.auth().principal() != null ? scope.auth().principal() : "";
                    dispatchInfo.authDomain = scope.auth() != null && scope.auth().domain() != null ? scope.auth().domain() : "";
                    dispatchInfo.authenticated = scope.auth() != null && scope.auth().authenticated();
                    dispatchInfo.transportMetadata = scope.transportMetadata();
                    dispatchInfo.requestData = serializeRequestBatch(paramsRoot, meta);
                    if ("stream".equals(dispatchInfo.methodType)) {
                        dispatchInfo.streamId = AccessLogHook.randomStreamId();
                    }
                    callStats = new CallStatistics();
                    try {
                        hookToken = dispatchHook.onDispatchStart(dispatchInfo);
                    } catch (Throwable t) {
                        LOG.warn("dispatch hook start error: {}", t.toString());
                    }
                }

                try {
                    if (info.methodType() == MethodType.UNARY) {
                        serveUnary(transport, info, kwargs, shm);
                    } else {
                        serveStream(transport, info, kwargs, shm);
                    }
                } catch (Throwable t) {
                    handlerErr = t;
                    throw t;
                } finally {
                    if (dispatchHook != null && dispatchInfo != null) {
                        // Snapshot sticky-session state at end-of-dispatch so the access
                        // log reflects open/resume/close that happened during the call.
                        SessionScope scope = SessionScope.current();
                        if (scope != null) {
                            dispatchInfo.sessionId = scope.sessionIdHex();
                            dispatchInfo.sessionAction = scope.action();
                        }
                        try {
                            dispatchHook.onDispatchEnd(hookToken, dispatchInfo, callStats, handlerErr);
                        } catch (Throwable t) {
                            LOG.warn("dispatch hook end error: {}", t.toString());
                        }
                    }
                }
                transport.writer().flush();
            } finally {
                if (resolvedParams != null) resolvedParams.close();
            }
        } catch (EndOfStream e) {
            throw e;
        } catch (Exception e) {
            LOG.warn("serve error: {}", e.toString());
            e.printStackTrace(System.err);
        }
    }

    private void serveUnary(RpcTransport transport, RpcMethodInfo info, Map<String, Object> kwargs,
                            Shm shm) throws Exception {
        Schema schema = info.resultSchema();
        ClientLogSink sink = new ClientLogSink(serverId);
        AuthScope.Scope scope = AuthScope.current();
        try (IpcStreamWriter w = new IpcStreamWriter(transport.writer())) {
            w.writeSchema(schema);
            CallContext ctx = new CallContext(scope.auth(), sink, scope.transportMetadata(),
                    serverId, info.name(), protocolName(), "");
            sink.bind(w, schema);
            try {
                Object[] callArgs = ParameterBinder.bind(info.reflectMethod(), kwargs, ctx);
                Object result = info.reflectMethod().invoke(impl, callArgs);
                writeResult(w, info, result, shm);
            } catch (Throwable t) {
                Throwable inner = unwrap(t);
                Wire.writeZeroBatch(w, schema, Wire.errorMetadata(inner, serverId));
            }
        }
    }

    private void writeResult(IpcStreamWriter w, RpcMethodInfo info, Object result, Shm shm) throws Exception {
        Schema schema = info.resultSchema();
        if (schema.getFields().isEmpty()) {
            Wire.writeZeroBatch(w, schema, null);
            return;
        }
        Field resultField = schema.getFields().get(0);
        Map<String, Object> row = new LinkedHashMap<>();
        row.put(resultField.getName(), convertResult(result, info.resultType(), resultField));
        try (VectorSchemaRoot root = Marshalling.encodeRow(schema, row, Allocators.root())) {
            // Prefer the shared-memory side-channel; fall back to external-location,
            // then inline. The client resolves unary-response shm pointers on the
            // FunctionConnection path (ResolveUnaryShm); connections that never
            // advertise a segment (catalog, bind) get shm == null → inline.
            ShmResolver.ShmPointer sp = ShmResolver.maybeWriteToShm(shm, root, null);
            if (sp != null) {
                try (VectorSchemaRoot pr = sp.root()) {
                    w.writeBatch(pr, sp.customMetadata());
                }
                return;
            }
            Externalizer.Pointer ptr = Externalizer.maybeExternalize(root, null, externalConfig);
            if (ptr != null) {
                try (VectorSchemaRoot pr = ptr.root()) {
                    w.writeBatch(pr, ptr.customMetadata());
                }
            } else {
                w.writeBatch(root, null);
            }
        }
    }

    private Object convertResult(Object value, Type resultType, Field field) {
        if (value instanceof Optional<?> opt) value = opt.orElse(null);
        if (value == null) return null;
        // Records-as-binary: serialize whole record into bytes for the binary column
        if (value instanceof ArrowSerializableRecord r && field.getType() instanceof ArrowType.Binary) {
            return RecordCodec.serializeToBytes(r);
        }
        return value;
    }

    private void serveStream(RpcTransport transport, RpcMethodInfo info, Map<String, Object> kwargs,
                             Shm shm) throws Exception {
        ClientLogSink sink = new ClientLogSink(serverId);
        AuthScope.Scope scope = AuthScope.current();
        CallContext ctx = new CallContext(scope.auth(), sink, scope.transportMetadata(),
                serverId, info.name(), protocolName(), "");

        RpcStream<?> stream = runStreamInit(info, kwargs, ctx, transport);
        if (stream == null) return;  // init failed; error already reported + input drained

        if (stream.header() != null) {
            writeHeaderStream(transport.writer(), stream.header(), sink);
            transport.writer().flush();
        }

        Schema outputSchema = stream.outputSchema();
        Schema inputSchema = stream.inputSchema();
        StreamState state = stream.state();
        boolean isProducer = stream.isProducer();

        try (IpcStreamReader inputReader = new IpcStreamReader(transport.reader(), Allocators.root())) {
            IpcStreamWriter outputWriter = new IpcStreamWriter(transport.writer());
            try {
                outputWriter.writeSchema(outputSchema);
                transport.writer().flush();
                sink.bind(outputWriter, outputSchema);

                runTickLoop(inputReader, outputWriter, transport, state, ctx,
                        outputSchema, inputSchema, isProducer, shm);
            } finally {
                closeStreamCleanly(outputWriter, transport, inputReader);
            }
        }
    }

    /**
     * Invoke the service method to obtain the {@link RpcStream}. On failure
     * write an error stream + drain the client's input and return {@code null};
     * callers should early-exit in that case.
     */
    private RpcStream<?> runStreamInit(RpcMethodInfo info, Map<String, Object> kwargs, CallContext ctx,
                                        RpcTransport transport) throws IOException {
        try {
            Object[] args = ParameterBinder.bind(info.reflectMethod(), kwargs, ctx);
            return (RpcStream<?>) info.reflectMethod().invoke(impl, args);
        } catch (Throwable t) {
            Throwable initException = unwrap(t);
            Wire.writeErrorStream(transport.writer(), RpcStream.EMPTY_SCHEMA, initException, serverId);
            transport.writer().flush();
            // Drain the client's input IPC stream so subsequent requests aren't misparsed.
            try (IpcStreamReader inputReader = new IpcStreamReader(transport.reader(), Allocators.root())) {
                try { inputReader.drain(); } catch (IOException ignore) { /* best-effort */ }
            }
            return null;
        }
    }

    /** Main tick loop: read input batch → resolve/cast → state.process → flush output, until EOS or finish. */
    private void runTickLoop(IpcStreamReader inputReader, IpcStreamWriter outputWriter, RpcTransport transport,
                             StreamState state, CallContext ctx, Schema outputSchema, Schema inputSchema,
                             boolean isProducer, Shm shm) throws IOException {
        while (true) {
            Map<String, String> meta;
            // Time spent blocked here is the worker idle waiting for the client to
            // send the next input — under lockstep that equals the client's own
            // (encode/consume) work + handoff latency.
            long tIdle0 = System.nanoTime();
            try {
                meta = inputReader.readNextBatch();
            } catch (IOException e) {
                break;
            }
            if (shm != null) shm.idleNs += System.nanoTime() - tIdle0;
            if (meta == null) break;
            if (meta.containsKey(Metadata.CANCEL)) {
                try { state.onCancel(ctx); } catch (Exception ignore) { /* best-effort */ }
                break;
            }
            long tBusy0 = System.nanoTime();
            boolean cont = processOneTick(inputReader, outputWriter, transport, state, ctx,
                    outputSchema, inputSchema, isProducer, meta, shm);
            if (shm != null) shm.busyNs += System.nanoTime() - tBusy0;
            if (!cont) break;
        }
    }

    /**
     * Handle one tick: resolve pointer / cast schema / invoke state.process / flush.
     * Returns {@code true} to continue the loop, {@code false} on terminal state (error or finish).
     */
    private boolean processOneTick(IpcStreamReader inputReader, IpcStreamWriter outputWriter, RpcTransport transport,
                                    StreamState state, CallContext ctx, Schema outputSchema, Schema inputSchema,
                                    boolean isProducer, Map<String, String> meta, Shm shm) throws IOException {
        VectorSchemaRoot inputRoot = inputReader.root();
        VectorSchemaRoot resolvedRoot = null;
        Map<String, String> effectiveMeta = meta;

        boolean inboundViaShm = shm != null && ShmResolver.isPointer(inputRoot.getRowCount(), meta);
        if (inboundViaShm) {
            try {
                shm.inShmBatches++;
                shm.inShmBytes += Long.parseLong(meta.get(Metadata.SHM_LENGTH));
                long tr0 = System.nanoTime();
                ShmResolver.Resolved res = ShmResolver.resolve(shm, inputRoot, meta);
                shm.resolveNs += System.nanoTime() - tr0;
                resolvedRoot = res.root();
                inputRoot = resolvedRoot;
                effectiveMeta = res.customMetadata();
            } catch (Exception shmExc) {
                Wire.writeZeroBatch(outputWriter, outputSchema, Wire.errorMetadata(shmExc, serverId));
                transport.writer().flush();
                return false;
            }
        } else if (locationResolver != null && LocationResolver.isPointer(inputRoot.getRowCount(), meta)) {
            try {
                LocationResolver.Resolved res = locationResolver.resolve(meta);
                resolvedRoot = res.root();
                inputRoot = resolvedRoot;
                effectiveMeta = res.customMetadata();
            } catch (Exception fetchExc) {
                Wire.writeZeroBatch(outputWriter, outputSchema, Wire.errorMetadata(fetchExc, serverId));
                transport.writer().flush();
                return false;
            }
        } else if (shm != null && resolvedRoot == null && inputRoot.getRowCount() > 0) {
            // A non-empty data batch arrived inline even though shm is active —
            // the client chose not to (or couldn't) put it in the segment.
            shm.inInlineDataBatches++;
        }

        VectorSchemaRoot castRoot = null;
        if (!isProducer && !inputRoot.getSchema().equals(inputSchema)) {
            try {
                castRoot = Marshalling.castRoot(inputRoot, inputSchema, Allocators.root());
                inputRoot = castRoot;
            } catch (Exception castExc) {
                Wire.writeZeroBatch(outputWriter, outputSchema, Wire.errorMetadata(
                        new ClassCastException(castExc.getMessage()), serverId));
                transport.writer().flush();
                return false;
            }
        }

        OutputCollector out = new OutputCollector(outputSchema, serverId, isProducer);
        long tp0 = System.nanoTime();
        try {
            state.process(new AnnotatedBatch(inputRoot, effectiveMeta), out, ctx);
        } catch (Throwable t) {
            Wire.writeZeroBatch(outputWriter, outputSchema, Wire.errorMetadata(unwrap(t), serverId));
            transport.writer().flush();
            return false;
        }
        if (shm != null) shm.processNs += System.nanoTime() - tp0;
        long te0 = System.nanoTime();
        flushCollector(outputWriter, out, inputReader.dictionaryProvider(), shm);
        transport.writer().flush();
        if (shm != null) shm.emitNs += System.nanoTime() - te0;
        if (castRoot != null) castRoot.close();
        if (resolvedRoot != null) resolvedRoot.close();
        return !out.finished();
    }

    /**
     * Close the output FIRST (sends EOS to client), then drain the client's
     * remaining input (until its EOS) so the transport is clean for the next
     * request. Closing output first breaks the client out of its reader;
     * otherwise draining deadlocks.
     */
    private static void closeStreamCleanly(IpcStreamWriter outputWriter, RpcTransport transport,
                                            IpcStreamReader inputReader) {
        try { outputWriter.close(); } catch (Exception ignore) { /* already-closed transport is fine */ }
        try { transport.writer().flush(); } catch (Exception ignore) { /* already-closed transport is fine */ }
        try { inputReader.drain(); } catch (IOException ignore) { /* client already gone */ }
    }

    /**
     * Threads the inbound stream's {@link
     * org.apache.arrow.vector.dictionary.DictionaryProvider} through to the
     * writer, so dict-encoded columns (DuckDB ENUMs) round-trip through
     * passthrough handlers like {@code echo} with their dictionaries
     * intact. Without this the consumer sees raw index columns and renders
     * them as unbound nulls. {@code shm}, when present, offloads non-dict data
     * batches to the shared-memory segment as zero-row pointer batches.
     */
    private void flushCollector(IpcStreamWriter writer, OutputCollector out,
                                  org.apache.arrow.vector.dictionary.DictionaryProvider dictProvider,
                                  Shm shm)
            throws IOException {
        for (OutputCollector.Entry e : out.entries()) {
            // Per-entry provider (set when a producer emits dict-encoded
            // batches) wins over the stream-level dictProvider (set by the
            // TIO/echo path so input dicts round-trip). Either is sufficient
            // for IpcStreamWriter to emit the dict batches alongside the
            // record batch.
            org.apache.arrow.vector.dictionary.DictionaryProvider effective =
                    e.dictionaryProvider() != null ? e.dictionaryProvider() : dictProvider;
            try {
                if (e.isData() && shm != null) {
                    ShmResolver.ShmPointer sp =
                            ShmResolver.maybeWriteToShm(shm, e.root(), e.customMetadata());
                    if (sp != null) {
                        shm.outShmBatches++;
                        shm.outShmBytes += Long.parseLong(sp.customMetadata().get(Metadata.SHM_LENGTH));
                        try (VectorSchemaRoot pr = sp.root()) {
                            writer.writeBatch(pr, sp.customMetadata(), effective);
                        }
                        continue;   // finally still closes e.root()
                    }
                    // Eligible for shm but fell back to inline (segment full / serialize
                    // failed) — the signal that intended-shm output leaked to the pipe.
                    if (ShmResolver.shmEligible(e.root())) {
                        shm.outInlineEligibleBatches++;
                    }
                }
                if (e.isData() && externalConfig != null && externalConfig.storage() != null) {
                    try {
                        Externalizer.Pointer ptr = Externalizer.maybeExternalize(
                                e.root(), e.customMetadata(), externalConfig);
                        if (ptr != null) {
                            try (VectorSchemaRoot pr = ptr.root()) {
                                writer.writeBatch(pr, ptr.customMetadata(), effective);
                            }
                            continue;
                        }
                    } catch (Exception up) {
                        // Upload failed — fall through and write the batch inline rather than
                        // failing the stream. The client will still receive valid data.
                    }
                }
                writer.writeBatch(e.root(), e.customMetadata(), effective);
            } finally {
                e.root().close();
                // Per-entry providers own their dictionary vectors (the
                // producer that built them transferred ownership via emit).
                // The stream-level dictProvider is *not* closed here — its
                // lifecycle is owned by the input reader on the TIO path.
                if (e.dictionaryProvider() != null) {
                    for (long id : e.dictionaryProvider().getDictionaryIds()) {
                        org.apache.arrow.vector.dictionary.Dictionary d =
                                e.dictionaryProvider().lookup(id);
                        if (d != null && d.getVector() != null) {
                            try { d.getVector().close(); } catch (Exception ignore) {}
                        }
                    }
                }
            }
        }
    }

    private void serveDescribe(RpcTransport transport) throws IOException {
        Introspect.Built built = Introspect.build(protocolName(), methods, serverId, protocolVersion);
        try (IpcStreamWriter w = new IpcStreamWriter(transport.writer());
             VectorSchemaRoot root = built.root()) {
            w.writeBatch(root, built.customMetadata());
        } finally {
            transport.writer().flush();
        }
    }

    private void serveTransportOptions(RpcTransport transport) throws IOException {
        // Worker capabilities ride as metadata; the response batch is empty.
        Map<String, String> md = new LinkedHashMap<>(TransportOptions.workerCapabilities());
        md.put(Metadata.REQUEST_VERSION_KEY, Metadata.REQUEST_VERSION);
        if (serverId != null) md.put(Metadata.SERVER_ID, serverId);
        try (IpcStreamWriter w = new IpcStreamWriter(transport.writer());
             VectorSchemaRoot root = VectorSchemaRoot.create(RpcStream.EMPTY_SCHEMA, Allocators.root())) {
            root.setRowCount(0);
            w.writeBatch(root, md);
        } finally {
            transport.writer().flush();
        }
    }

    private void writeHeaderStream(OutputStream os, ArrowSerializableRecord header,
                                   ClientLogSink sink) throws IOException {
        Schema schema = SchemaDerivation.schemaForRecord(header.getClass());
        Map<String, Object> row = RecordCodec.toRowMap(header);
        IpcStreamWriter w = new IpcStreamWriter(os);
        try {
            w.writeSchema(schema);
            // Flush buffered init-time log batches into the header IPC stream
            // (matches the Python reference: log batches precede the header batch).
            if (sink != null) sink.bind(w, schema);
            try (VectorSchemaRoot root = Marshalling.encodeRow(schema, row, Allocators.root())) {
                w.writeBatch(root, null);
            }
        } finally {
            w.close();
            if (sink != null) sink.detach();
        }
    }

    private static Throwable unwrap(Throwable t) {
        if (t instanceof InvocationTargetException && t.getCause() != null) {
            return t.getCause();
        }
        return t;
    }

    /** Buffers log messages until the IPC writer is bound, then writes them inline. */
    static final class ClientLogSink implements Consumer<Message> {
        private final String serverId;
        private final List<Message> buffer = new ArrayList<>();
        private IpcStreamWriter writer;
        private Schema schema;

        ClientLogSink(String serverId) { this.serverId = serverId; }

        void bind(IpcStreamWriter writer, Schema schema) throws IOException {
            this.writer = writer;
            this.schema = schema;
            for (Message msg : buffer) writeNow(msg);
            buffer.clear();
        }

        /** Unbind so further logs are buffered again (used between header + main streams). */
        void detach() {
            this.writer = null;
            this.schema = null;
        }

        @Override
        public void accept(Message msg) {
            if (writer != null) {
                try { writeNow(msg); } catch (IOException e) { throw new RuntimeException(e); }
            } else buffer.add(msg);
        }

        private void writeNow(Message msg) throws IOException {
            Map<String, String> md = msg.addToMetadata(null);
            if (serverId != null) md.put(Metadata.SERVER_ID, serverId);
            Wire.writeZeroBatch(writer, schema, md);
        }
    }
}
