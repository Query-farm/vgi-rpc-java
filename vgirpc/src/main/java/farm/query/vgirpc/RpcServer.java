// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

import farm.query.vgirpc.external.ExternalLocationConfig;
import farm.query.vgirpc.external.Externalizer;
import farm.query.vgirpc.external.LocationResolver;
import farm.query.vgirpc.log.Message;
import farm.query.vgirpc.marshal.Marshalling;
import farm.query.vgirpc.marshal.ParameterBinder;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.schema.SchemaDerivation;
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

    public RpcServer(Class<?> serviceInterface, Object impl) {
        this(serviceInterface, impl, UUID.randomUUID().toString().replace("-", "").substring(0, 12), true);
    }

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

    public String serverId() { return serverId; }
    public String protocolName() { return serviceInterface.getSimpleName(); }
    public Map<String, RpcMethodInfo> methods() { return Collections.unmodifiableMap(methods); }

    /** Underlying service implementation (exposed for the HTTP streaming handler). */
    public Object implementation() { return impl; }

    /** Loop serving requests until the transport closes. */
    public void serve(RpcTransport transport) {
        while (true) {
            try {
                serveOne(transport);
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

    private static final class EndOfStream extends RuntimeException {}

    /** Handle exactly one RPC call. */
    public void serveOne(RpcTransport transport) {
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
            VectorSchemaRoot paramsRoot = reader.root();
            // Snapshot the kwargs immediately, before draining mutates the reader's root.
            Map<String, Object> kwargsSnapshot;
            try {
                kwargsSnapshot = paramsRoot.getRowCount() == 0
                        ? new LinkedHashMap<>()
                        : Marshalling.decodeRow(paramsRoot, reader.dictionaryProvider(), reader.wireSchema());
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
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
            RpcMethodInfo info = methods.get(method);
            if (info == null) {
                Wire.writeErrorStream(transport.writer(), RpcStream.EMPTY_SCHEMA,
                        new IllegalArgumentException("Unknown method: '" + method + "'. Available: " + methods.keySet()),
                        serverId);
                transport.writer().flush();
                return;
            }
            Map<String, Object> kwargs = kwargsSnapshot;
            if (info.methodType() == MethodType.UNARY) {
                serveUnary(transport, info, kwargs);
            } else {
                serveStream(transport, info, kwargs);
            }
            transport.writer().flush();
        } catch (EndOfStream e) {
            throw e;
        } catch (Exception e) {
            LOG.warn("serve error: {}", e.toString());
            e.printStackTrace(System.err);
        }
    }

    private void serveUnary(RpcTransport transport, RpcMethodInfo info, Map<String, Object> kwargs) throws Exception {
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
                writeResult(w, info, result);
            } catch (Throwable t) {
                Throwable inner = unwrap(t);
                Wire.writeZeroBatch(w, schema, Wire.errorMetadata(inner, serverId));
            }
        }
    }

    private void writeResult(IpcStreamWriter w, RpcMethodInfo info, Object result) throws Exception {
        Schema schema = info.resultSchema();
        if (schema.getFields().isEmpty()) {
            Wire.writeZeroBatch(w, schema, null);
            return;
        }
        Field resultField = schema.getFields().get(0);
        Map<String, Object> row = new LinkedHashMap<>();
        row.put(resultField.getName(), convertResult(result, info.resultType(), resultField));
        try (VectorSchemaRoot root = Marshalling.encodeRow(schema, row, Allocators.root())) {
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

    private void serveStream(RpcTransport transport, RpcMethodInfo info, Map<String, Object> kwargs) throws Exception {
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
                        outputSchema, inputSchema, isProducer);
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
                             boolean isProducer) throws IOException {
        while (true) {
            Map<String, String> meta;
            try {
                meta = inputReader.readNextBatch();
            } catch (IOException e) {
                break;
            }
            if (meta == null) break;
            if (meta.containsKey(Metadata.CANCEL)) {
                try { state.onCancel(ctx); } catch (Exception ignore) { /* best-effort */ }
                break;
            }
            if (!processOneTick(inputReader, outputWriter, transport, state, ctx,
                    outputSchema, inputSchema, isProducer, meta)) {
                break;
            }
        }
    }

    /**
     * Handle one tick: resolve pointer / cast schema / invoke state.process / flush.
     * Returns {@code true} to continue the loop, {@code false} on terminal state (error or finish).
     */
    private boolean processOneTick(IpcStreamReader inputReader, IpcStreamWriter outputWriter, RpcTransport transport,
                                    StreamState state, CallContext ctx, Schema outputSchema, Schema inputSchema,
                                    boolean isProducer, Map<String, String> meta) throws IOException {
        VectorSchemaRoot inputRoot = inputReader.root();
        VectorSchemaRoot resolvedRoot = null;
        Map<String, String> effectiveMeta = meta;

        if (locationResolver != null && LocationResolver.isPointer(inputRoot.getRowCount(), meta)) {
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
        try {
            state.process(new AnnotatedBatch(inputRoot, effectiveMeta), out, ctx);
        } catch (Throwable t) {
            Wire.writeZeroBatch(outputWriter, outputSchema, Wire.errorMetadata(unwrap(t), serverId));
            transport.writer().flush();
            return false;
        }
        flushCollector(outputWriter, out);
        transport.writer().flush();
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

    private void flushCollector(IpcStreamWriter writer, OutputCollector out) throws IOException {
        for (OutputCollector.Entry e : out.entries()) {
            try {
                if (e.isData() && externalConfig != null && externalConfig.storage() != null) {
                    try {
                        Externalizer.Pointer ptr = Externalizer.maybeExternalize(
                                e.root(), e.customMetadata(), externalConfig);
                        if (ptr != null) {
                            try (VectorSchemaRoot pr = ptr.root()) {
                                writer.writeBatch(pr, ptr.customMetadata());
                            }
                            continue;
                        }
                    } catch (Exception up) {
                        // Upload failed — fall through and write the batch inline rather than
                        // failing the stream. The client will still receive valid data.
                    }
                }
                writer.writeBatch(e.root(), e.customMetadata());
            } finally {
                e.root().close();
            }
        }
    }

    private void serveDescribe(RpcTransport transport) throws IOException {
        Introspect.Built built = Introspect.build(protocolName(), methods, serverId);
        try (IpcStreamWriter w = new IpcStreamWriter(transport.writer());
             VectorSchemaRoot root = built.root()) {
            w.writeBatch(root, built.customMetadata());
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
