// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.AuthContext;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.ExchangeState;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.ProducerState;
import farm.query.vgirpc.RpcMethodInfo;
import farm.query.vgirpc.RpcServer;
import farm.query.vgirpc.Stream;
import farm.query.vgirpc.StreamState;
import farm.query.vgirpc.marshal.Marshalling;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.schema.SchemaDerivation;
import farm.query.vgirpc.wire.Allocators;
import farm.query.vgirpc.wire.IpcStreamReader;
import farm.query.vgirpc.wire.IpcStreamWriter;
import farm.query.vgirpc.wire.Metadata;
import farm.query.vgirpc.wire.Wire;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.nio.channels.Channels;
import java.security.SecureRandom;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Stateless HTTP streaming dispatch: each init / exchange request is a standalone
 * HTTP call that round-trips a signed state token in Arrow custom metadata.
 *
 * <p>Producer streams emit all batches within the init response (no token);
 * exchange streams emit a zero-row batch carrying a continuation token.</p>
 */
public final class HttpStreamHandler {

    private final RpcServer rpc;
    private final byte[] signingKey;
    private final long tokenTtlSeconds;
    /** method name → concrete {@link StreamState} class, learned from the first init call. */
    private final Map<String, Class<? extends StreamState>> stateTypes = new ConcurrentHashMap<>();

    public HttpStreamHandler(RpcServer rpc) { this(rpc, null, 0); }

    /**
     * @param signingKey HMAC-SHA256 signing key; when {@code null} a random
     *     per-process key is generated (tokens won't survive restarts).
     * @param tokenTtlSeconds maximum token age in seconds; {@code 0} disables
     *     TTL enforcement.
     */
    public HttpStreamHandler(RpcServer rpc, byte[] signingKey, long tokenTtlSeconds) {
        this.rpc = rpc;
        if (signingKey != null) {
            this.signingKey = signingKey.clone();
        } else {
            this.signingKey = new byte[32];
            new SecureRandom().nextBytes(this.signingKey);
        }
        this.tokenTtlSeconds = tokenTtlSeconds;
    }

    /** Handle {@code POST /{method}/init}. Returns response IPC bytes. */
    public byte[] handleInit(String method, byte[] requestBody) throws Exception {
        RpcMethodInfo info = rpc.methods().get(method);
        if (info == null) return errorStream(new IllegalArgumentException("Unknown method: " + method));

        // Parse request
        Map<String, Object> kwargs;
        try (IpcStreamReader r = new IpcStreamReader(new ByteArrayInputStream(requestBody), Allocators.root())) {
            Map<String, String> meta = r.readNextBatch();
            if (meta == null) return errorStream(new RuntimeException("empty request"));
            Wire.validateRequestVersion(meta);
            String urlMethod = Wire.requireMethodName(meta);
            if (!method.equals(urlMethod)) {
                return errorStream(new ClassCastException(
                        "Method name mismatch: URL has '" + method + "' but metadata has '" + urlMethod + "'"));
            }
            VectorSchemaRoot root = r.root();
            kwargs = root.getRowCount() == 0
                    ? new LinkedHashMap<>()
                    : Marshalling.decodeRow(root, r.dictionaryProvider(), r.wireSchema());
        }

        OutputCollectorSink sink = new OutputCollectorSink();
        farm.query.vgirpc.AuthScope.Scope scope = farm.query.vgirpc.AuthScope.current();
        CallContext ctx = new CallContext(scope.auth, sink, scope.transportMetadata,
                rpc.serverId(), method, rpc.protocolName(), "");

        Stream<?> streamResult;
        try {
            Object[] args = buildArgs(info, kwargs, ctx);
            streamResult = (Stream<?>) info.reflectMethod().invoke(rpc.implementation(), args);
        } catch (InvocationTargetException ie) {
            return errorStream(ie.getCause() != null ? ie.getCause() : ie);
        } catch (Throwable t) {
            return errorStream(t);
        }
        // Record the concrete state class for this method so /exchange can rehydrate.
        stateTypes.put(method, streamResult.state().getClass());

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        if (streamResult.header() != null) {
            writeHeaderIpcStream(out, streamResult.header(), sink);
        }

        boolean isProducer = streamResult.isProducer();
        if (isProducer) {
            writeProducerRun(out, streamResult, ctx, sink);
        } else {
            writeExchangeInitToken(out, streamResult, sink);
        }
        return out.toByteArray();
    }

    /** Handle {@code POST /{method}/exchange}. */
    public byte[] handleExchange(String method, byte[] requestBody) throws Exception {
        RpcMethodInfo info = rpc.methods().get(method);
        if (info == null) return errorStream(new IllegalArgumentException("Unknown method: " + method));

        VectorSchemaRoot inputRoot;
        Map<String, String> meta;
        try (IpcStreamReader r = new IpcStreamReader(new ByteArrayInputStream(requestBody), Allocators.root())) {
            meta = r.readNextBatch();
            if (meta == null) return errorStream(new RuntimeException("empty exchange request"));
            inputRoot = r.root();
            // Copy the input because the reader is going out of scope
            inputRoot = copyRoot(inputRoot);
        }

        String tokenB64 = meta.get(Metadata.STREAM_STATE);
        if (tokenB64 == null) {
            inputRoot.close();
            return errorStream(new RuntimeException("Missing state token in exchange request"));
        }
        StateToken token;
        try {
            token = StateToken.unpack(tokenB64.getBytes(), signingKey, tokenTtlSeconds);
        } catch (Exception e) {
            inputRoot.close();
            return errorStream(e);
        }
        Schema outputSchema = deserializeSchema(token.outputSchema);
        Schema inputSchema = deserializeSchema(token.inputSchema);
        boolean isProducer = inputSchema.getFields().isEmpty();

        Class<? extends StreamState> stateCls = stateTypes.get(method);
        if (stateCls == null) {
            inputRoot.close();
            return errorStream(new IllegalStateException("No state class cached for '" + method + "'"));
        }
        StreamState state = StateSerializer.deserialize(token.state, stateCls);

        OutputCollectorSink sink = new OutputCollectorSink();
        farm.query.vgirpc.AuthScope.Scope scope = farm.query.vgirpc.AuthScope.current();
        CallContext ctx = new CallContext(scope.auth, sink, scope.transportMetadata,
                rpc.serverId(), method, rpc.protocolName(), "");

        // Handle cancel: invoke on_cancel, return empty stream (schema + EOS).
        if (meta.containsKey(Metadata.CANCEL)) {
            try { state.onCancel(ctx); } catch (Exception ignore) {}
            inputRoot.close();
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (IpcStreamWriter w = new IpcStreamWriter(out)) {
                w.writeSchema(outputSchema);
            }
            return out.toByteArray();
        }

        // Cast input schema for compatible widenings (int32 → float64 etc.)
        VectorSchemaRoot actualInput = inputRoot;
        VectorSchemaRoot castInput = null;
        try {
            if (!isProducer && !inputRoot.getSchema().equals(inputSchema)) {
                castInput = Marshalling.castRoot(inputRoot, inputSchema, Allocators.root());
                actualInput = castInput;
            }
        } catch (Exception castExc) {
            inputRoot.close();
            return errorStream(new ClassCastException(castExc.getMessage()));
        }

        OutputCollector collector = new OutputCollector(outputSchema, rpc.serverId(), isProducer);
        try {
            state.process(new AnnotatedBatch(actualInput, meta), collector, ctx);
            if (!collector.finished()) collector.validate();
        } catch (Throwable t) {
            if (castInput != null) castInput.close();
            inputRoot.close();
            return errorStream(t);
        }

        boolean finished = collector.finished();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (IpcStreamWriter w = new IpcStreamWriter(out)) {
            w.writeSchema(outputSchema);

            String newTokenStr = null;
            if (!finished) {
                byte[] newStateBytes = StateSerializer.serialize(state);
                StateToken newToken = new StateToken(newStateBytes, token.outputSchema, token.inputSchema,
                        token.streamId, System.currentTimeMillis() / 1000);
                newTokenStr = new String(newToken.pack(signingKey));
            }

            // Producer continuation: data batch + separate zero-row state-token batch (matches init format).
            // Exchange: data batch carries the new state-token in its metadata.
            OutputCollector.Entry data = null;
            for (OutputCollector.Entry e : collector.entries()) {
                if (e.isData) { data = e; continue; }
                w.writeBatch(e.root, e.customMetadata);
                e.root.close();
            }
            if (!isProducer && data != null && newTokenStr != null) {
                Map<String, String> md = new LinkedHashMap<>();
                if (data.customMetadata != null) md.putAll(data.customMetadata);
                md.put(Metadata.STREAM_STATE, newTokenStr);
                w.writeBatch(data.root, md);
                data.root.close();
            } else {
                if (data != null) { w.writeBatch(data.root, data.customMetadata); data.root.close(); }
                if (newTokenStr != null) {
                    Map<String, String> md = new LinkedHashMap<>();
                    md.put(Metadata.STREAM_STATE, newTokenStr);
                    try (VectorSchemaRoot zero = VectorSchemaRoot.create(outputSchema, Allocators.root())) {
                        zero.allocateNew(); zero.setRowCount(0);
                        w.writeBatch(zero, md);
                    }
                }
            }
        }
        if (castInput != null) castInput.close();
        inputRoot.close();
        return out.toByteArray();
    }

    // --- Helpers ----------------------------------------------------------

    private void writeProducerRun(ByteArrayOutputStream out, Stream<?> streamResult,
                                   CallContext ctx, OutputCollectorSink sink) throws IOException {
        Schema outputSchema = streamResult.outputSchema();
        Schema inputSchema = streamResult.inputSchema();
        StreamState state = streamResult.state();
        try (IpcStreamWriter w = new IpcStreamWriter(out)) {
            w.writeSchema(outputSchema);
            sink.bind(w, outputSchema);

            OutputCollector coll = new OutputCollector(outputSchema, rpc.serverId(), true);
            boolean error = false;
            try {
                state.process(new AnnotatedBatch(
                        VectorSchemaRoot.create(Stream.EMPTY_SCHEMA, Allocators.root()), Map.of()),
                        coll, ctx);
            } catch (Throwable t) {
                error = true;
                try (VectorSchemaRoot zero = VectorSchemaRoot.create(outputSchema, Allocators.root())) {
                    zero.allocateNew(); zero.setRowCount(0);
                    w.writeBatch(zero, Wire.errorMetadata(t, rpc.serverId()));
                }
            }

            if (!error) {
                // Emit all buffered entries (logs + at most one data batch) in order.
                for (OutputCollector.Entry e : coll.entries()) {
                    w.writeBatch(e.root, e.customMetadata);
                    e.root.close();
                }
                // If the producer isn't finished, append a zero-row state-token batch so the
                // client knows to call /exchange to continue. Finished streams just EOS.
                if (!coll.finished()) {
                    byte[] stateBytes = StateSerializer.serialize(state);
                    byte[] outputSchemaBytes = serializeSchema(outputSchema);
                    byte[] inputSchemaBytes = serializeSchema(inputSchema);
                    StateToken token = new StateToken(stateBytes, outputSchemaBytes, inputSchemaBytes,
                            UUID.randomUUID().toString().replace("-", ""), System.currentTimeMillis() / 1000);
                    Map<String, String> md = new LinkedHashMap<>();
                    md.put(Metadata.STREAM_STATE, new String(token.pack(signingKey)));
                    try (VectorSchemaRoot zero = VectorSchemaRoot.create(outputSchema, Allocators.root())) {
                        zero.allocateNew(); zero.setRowCount(0);
                        w.writeBatch(zero, md);
                    }
                }
            }
        }
    }

    private void writeExchangeInitToken(ByteArrayOutputStream out, Stream<?> streamResult,
                                         OutputCollectorSink sink) throws IOException {
        Schema outputSchema = streamResult.outputSchema();
        Schema inputSchema = streamResult.inputSchema();
        byte[] stateBytes = StateSerializer.serialize(streamResult.state());
        byte[] outputSchemaBytes = serializeSchema(outputSchema);
        byte[] inputSchemaBytes = serializeSchema(inputSchema);
        StateToken token = new StateToken(stateBytes, outputSchemaBytes, inputSchemaBytes,
                UUID.randomUUID().toString().replace("-", ""), System.currentTimeMillis() / 1000);
        byte[] tokenB64 = token.pack(signingKey);
        try (IpcStreamWriter w = new IpcStreamWriter(out)) {
            w.writeSchema(outputSchema);
            sink.bind(w, outputSchema);
            Map<String, String> md = new LinkedHashMap<>();
            md.put(Metadata.STREAM_STATE, new String(tokenB64));
            try (VectorSchemaRoot zero = VectorSchemaRoot.create(outputSchema, Allocators.root())) {
                zero.allocateNew();
                zero.setRowCount(0);
                w.writeBatch(zero, md);
            }
        }
    }

    private void writeHeaderIpcStream(ByteArrayOutputStream out,
                                       ArrowSerializableRecord header,
                                       OutputCollectorSink sink) throws IOException {
        Schema schema = SchemaDerivation.schemaForRecord(header.getClass());
        Map<String, Object> row = RecordCodec.toRowMap(header);
        IpcStreamWriter w = new IpcStreamWriter(out);
        try {
            w.writeSchema(schema);
            sink.bind(w, schema);
            try (VectorSchemaRoot root = Marshalling.encodeRow(schema, row, Allocators.root())) {
                w.writeBatch(root, null);
            }
        } finally {
            w.close();
            sink.detach();
        }
    }

    private byte[] errorStream(Throwable t) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Wire.writeErrorStream(out, Stream.EMPTY_SCHEMA, t, rpc.serverId());
        return out.toByteArray();
    }

    private static byte[] serializeSchema(Schema schema) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            WriteChannel ch = new WriteChannel(Channels.newChannel(bos));
            MessageSerializer.serialize(ch, schema);
            return bos.toByteArray();
        } catch (Exception e) { throw new RuntimeException(e); }
    }

    private static Schema deserializeSchema(byte[] b) {
        try {
            org.apache.arrow.vector.ipc.ReadChannel rc = new org.apache.arrow.vector.ipc.ReadChannel(
                    Channels.newChannel(new ByteArrayInputStream(b)));
            return MessageSerializer.deserializeSchema(rc);
        } catch (Exception e) { throw new RuntimeException(e); }
    }

    private static VectorSchemaRoot copyRoot(VectorSchemaRoot src) {
        VectorSchemaRoot dst = VectorSchemaRoot.create(src.getSchema(), Allocators.root());
        dst.allocateNew();
        int rows = src.getRowCount();
        for (int c = 0; c < src.getSchema().getFields().size(); c++) {
            org.apache.arrow.vector.FieldVector sv = src.getVector(c);
            org.apache.arrow.vector.FieldVector dv = dst.getVector(c);
            for (int r = 0; r < rows; r++) dv.copyFromSafe(r, r, sv);
        }
        dst.setRowCount(rows);
        return dst;
    }

    private Object[] buildArgs(RpcMethodInfo info, Map<String, Object> kwargs, CallContext ctx) {
        Method m = info.reflectMethod();
        Parameter[] params = m.getParameters();
        Object[] args = new Object[params.length];
        for (int i = 0; i < params.length; i++) {
            Parameter p = params[i];
            if (CallContext.class.isAssignableFrom(p.getType())) { args[i] = ctx; continue; }
            Object val = kwargs.get(p.getName());
            args[i] = adapt(val, p);
        }
        return args;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private Object adapt(Object val, Parameter p) {
        Class<?> t = p.getType();
        if (t == java.util.Optional.class) return java.util.Optional.ofNullable(val);
        if (val == null) return null;
        if (t.isEnum() && val instanceof String s) {
            return Enum.valueOf((Class<Enum>) t.asSubclass(Enum.class), s);
        }
        if (ArrowSerializableRecord.class.isAssignableFrom(t) && val instanceof byte[] bytes) {
            return RecordCodec.deserializeFromBytes(bytes, (Class<? extends ArrowSerializableRecord>) t);
        }
        if (t == int.class || t == Integer.class) return (int) ((Number) val).longValue();
        if (t == long.class || t == Long.class) return ((Number) val).longValue();
        if (t == double.class || t == Double.class) return ((Number) val).doubleValue();
        if (t == float.class || t == Float.class) return ((Number) val).floatValue();
        return val;
    }

    /** Collects log Messages during init so they can be flushed into the response stream. */
    private final class OutputCollectorSink implements java.util.function.Consumer<farm.query.vgirpc.log.Message> {
        private final java.util.List<farm.query.vgirpc.log.Message> buffer = new java.util.ArrayList<>();
        private IpcStreamWriter writer;
        private Schema schema;

        void bind(IpcStreamWriter w, Schema s) throws IOException {
            this.writer = w; this.schema = s;
            for (var msg : buffer) writeNow(msg);
            buffer.clear();
        }
        void detach() { this.writer = null; this.schema = null; }

        @Override
        public void accept(farm.query.vgirpc.log.Message msg) {
            if (writer != null) {
                try { writeNow(msg); } catch (IOException e) { throw new RuntimeException(e); }
            } else buffer.add(msg);
        }

        private void writeNow(farm.query.vgirpc.log.Message msg) throws IOException {
            Map<String, String> md = msg.addToMetadata(null);
            md.put(Metadata.SERVER_ID, rpc.serverId());
            try (VectorSchemaRoot zero = VectorSchemaRoot.create(schema, Allocators.root())) {
                zero.allocateNew(); zero.setRowCount(0);
                writer.writeBatch(zero, md);
            }
        }
    }
}
