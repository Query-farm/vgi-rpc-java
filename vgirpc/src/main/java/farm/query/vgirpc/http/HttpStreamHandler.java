// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.AuthScope;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.RpcMethodInfo;
import farm.query.vgirpc.RpcServer;
import farm.query.vgirpc.RpcStream;
import farm.query.vgirpc.StreamState;
import farm.query.vgirpc.log.Message;
import farm.query.vgirpc.marshal.Marshalling;
import farm.query.vgirpc.marshal.ParameterBinder;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.schema.SchemaDerivation;
import farm.query.vgirpc.wire.Allocators;
import farm.query.vgirpc.wire.IpcStreamReader;
import farm.query.vgirpc.wire.IpcStreamWriter;
import farm.query.vgirpc.wire.Metadata;
import farm.query.vgirpc.wire.Wire;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

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
    private final long maxResponseBytes;
    /** method name → concrete {@link StreamState} class, learned from the first init call. */
    private final Map<String, Class<? extends StreamState>> stateTypes = new ConcurrentHashMap<>();

    public HttpStreamHandler(RpcServer rpc) { this(rpc, null, 0, Long.MAX_VALUE); }

    /**
     * @param signingKey       HMAC-SHA256 signing key; when {@code null} a random
     *     per-process key is generated (tokens won't survive restarts).
     * @param tokenTtlSeconds  maximum token age in seconds; {@code 0} disables
     *     TTL enforcement.
     * @param maxResponseBytes per-call response cap. Exceeding it raises
     *     {@link PayloadTooLargeException} which the caller maps to HTTP 413;
     *     producers of large batches must use the external-location protocol.
     */
    public HttpStreamHandler(RpcServer rpc, byte[] signingKey, long tokenTtlSeconds, long maxResponseBytes) {
        if (tokenTtlSeconds < 0) {
            throw new IllegalArgumentException("tokenTtlSeconds must be >= 0, got " + tokenTtlSeconds);
        }
        if (maxResponseBytes <= 0) {
            throw new IllegalArgumentException("maxResponseBytes must be > 0, got " + maxResponseBytes);
        }
        this.rpc = rpc;
        if (signingKey != null) {
            this.signingKey = signingKey.clone();
        } else {
            this.signingKey = new byte[32];
            new SecureRandom().nextBytes(this.signingKey);
        }
        this.tokenTtlSeconds = tokenTtlSeconds;
        this.maxResponseBytes = maxResponseBytes;
    }

    /** Handle {@code POST /{method}/init}. Returns response IPC bytes. */
    public byte[] handleInit(String method, byte[] requestBody) throws Exception {
        RpcMethodInfo info = rpc.methods().get(method);
        if (info == null) return errorStream(new IllegalArgumentException("Unknown method: " + method));

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
        CallContext ctx = buildCallContext(method, sink);

        RpcStream<?> streamResult;
        try {
            Object[] args = ParameterBinder.bind(info.reflectMethod(), kwargs, ctx);
            streamResult = (RpcStream<?>) info.reflectMethod().invoke(rpc.implementation(), args);
        } catch (InvocationTargetException ie) {
            return errorStream(ie.getCause() != null ? ie.getCause() : ie);
        } catch (Throwable t) {
            return errorStream(t);
        }
        // Record the concrete state class for this method so /exchange can rehydrate.
        stateTypes.put(method, streamResult.state().getClass());

        BoundedByteArrayOutputStream out = new BoundedByteArrayOutputStream(maxResponseBytes);
        if (streamResult.header() != null) {
            writeHeaderIpcStream(out, streamResult.header(), sink);
        }
        if (streamResult.isProducer()) {
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

        ExchangeRequest req;
        try {
            req = parseExchangeRequest(requestBody);
        } catch (IOException | RuntimeException e) {
            return errorStream(e);
        }

        try (VectorSchemaRoot ownedInput = req.inputRoot()) {
            String tokenB64 = req.meta().get(Metadata.STREAM_STATE);
            if (tokenB64 == null) {
                return errorStream(new RuntimeException("Missing state token in exchange request"));
            }
            String principal = currentPrincipal();
            StateToken token;
            try {
                token = StateToken.unpack(tokenB64.getBytes(StandardCharsets.US_ASCII),
                        signingKey, tokenTtlSeconds, principal);
            } catch (Exception e) {
                return errorStream(e);
            }

            Class<? extends StreamState> stateCls = stateTypes.get(method);
            if (stateCls == null) {
                return errorStream(new IllegalStateException("No state class cached for '" + method + "'"));
            }
            Schema outputSchema = deserializeSchema(token.outputSchema());
            Schema inputSchema = deserializeSchema(token.inputSchema());
            boolean isProducer = inputSchema.getFields().isEmpty();
            StreamState state = StateSerializer.deserialize(token.state(), stateCls);

            CallContext ctx = buildCallContext(method, new OutputCollectorSink());

            if (req.meta().containsKey(Metadata.CANCEL)) {
                return handleCancel(outputSchema, state, ctx);
            }

            VectorSchemaRoot castInput = null;
            if (!isProducer && !ownedInput.getSchema().equals(inputSchema)) {
                try {
                    castInput = Marshalling.castRoot(ownedInput, inputSchema, Allocators.root());
                } catch (Exception castExc) {
                    return errorStream(new ClassCastException(castExc.getMessage()));
                }
            }

            try (VectorSchemaRoot maybeCast = castInput) {
                VectorSchemaRoot actualInput = maybeCast != null ? maybeCast : ownedInput;
                OutputCollector collector = new OutputCollector(outputSchema, rpc.serverId(), isProducer);
                try {
                    state.process(new AnnotatedBatch(actualInput, req.meta()), collector, ctx);
                    if (!collector.finished()) collector.validate();
                } catch (Throwable t) {
                    return errorStream(t);
                }
                return writeExchangeResponse(collector, state, token, outputSchema, isProducer, principal);
            }
        }
    }

    // --- handleExchange sub-steps -----------------------------------------

    /** Parsed exchange-request body: metadata (including the state token) plus the (owned) input batch. */
    private record ExchangeRequest(Map<String, String> meta, VectorSchemaRoot inputRoot) {}

    private static ExchangeRequest parseExchangeRequest(byte[] body) throws IOException {
        try (IpcStreamReader r = new IpcStreamReader(new ByteArrayInputStream(body), Allocators.root())) {
            Map<String, String> meta = r.readNextBatch();
            if (meta == null) throw new RuntimeException("empty exchange request");
            // Copy the input because the reader is going out of scope.
            VectorSchemaRoot copied = copyRoot(r.root());
            return new ExchangeRequest(meta, copied);
        }
    }

    private byte[] handleCancel(Schema outputSchema, StreamState state, CallContext ctx) throws IOException {
        // on_cancel is best-effort; the client has already decided it's done.
        try { state.onCancel(ctx); } catch (Exception ignore) { /* reported via onCancel contract */ }
        BoundedByteArrayOutputStream out = new BoundedByteArrayOutputStream(maxResponseBytes);
        try (IpcStreamWriter w = new IpcStreamWriter(out)) {
            w.writeSchema(outputSchema);
        }
        return out.toByteArray();
    }

    private byte[] writeExchangeResponse(OutputCollector collector, StreamState state, StateToken priorToken,
                                         Schema outputSchema, boolean isProducer, String principal) throws IOException {
        boolean finished = collector.finished();
        String newTokenStr = finished ? null : serializeContinuationToken(state, priorToken, principal);

        BoundedByteArrayOutputStream out = new BoundedByteArrayOutputStream(maxResponseBytes);
        try (IpcStreamWriter w = new IpcStreamWriter(out)) {
            w.writeSchema(outputSchema);

            // Logs and other non-data entries flow through first; the data entry is held for token-attachment.
            OutputCollector.Entry data = null;
            for (OutputCollector.Entry e : collector.entries()) {
                if (e.isData()) { data = e; continue; }
                w.writeBatch(e.root(), e.customMetadata());
                e.root().close();
            }

            if (!isProducer && data != null && newTokenStr != null) {
                // Exchange continuation: piggy-back the new token on the data batch's metadata.
                Map<String, String> md = new LinkedHashMap<>();
                if (data.customMetadata() != null) md.putAll(data.customMetadata());
                md.put(Metadata.STREAM_STATE, newTokenStr);
                w.writeBatch(data.root(), md);
                data.root().close();
            } else {
                // Producer continuation (or no data): emit the data batch as-is, token as a trailing zero-row batch.
                if (data != null) {
                    w.writeBatch(data.root(), data.customMetadata());
                    data.root().close();
                }
                if (newTokenStr != null) {
                    Wire.writeZeroBatch(w, outputSchema, Map.of(Metadata.STREAM_STATE, newTokenStr));
                }
            }
        }
        return out.toByteArray();
    }

    private String serializeContinuationToken(StreamState state, StateToken priorToken, String principal) {
        byte[] newStateBytes = StateSerializer.serialize(state);
        StateToken newToken = new StateToken(newStateBytes, priorToken.outputSchema(), priorToken.inputSchema(),
                priorToken.streamId(), System.currentTimeMillis() / 1000);
        return new String(newToken.pack(signingKey, principal), StandardCharsets.US_ASCII);
    }

    private CallContext buildCallContext(String method, Consumer<Message> sink) {
        AuthScope.Scope scope = AuthScope.current();
        return new CallContext(scope.auth(), sink, scope.transportMetadata(),
                rpc.serverId(), method, rpc.protocolName(), "");
    }

    /** Principal for state-token key derivation; empty string for anonymous. */
    private static String currentPrincipal() {
        String p = AuthScope.current().auth().principal();
        return p != null ? p : "";
    }

    // --- Init helpers -----------------------------------------------------

    private void writeProducerRun(ByteArrayOutputStream out, RpcStream<?> streamResult,
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
                        VectorSchemaRoot.create(RpcStream.EMPTY_SCHEMA, Allocators.root()), Map.of()),
                        coll, ctx);
            } catch (Throwable t) {
                error = true;
                Wire.writeZeroBatch(w, outputSchema, Wire.errorMetadata(t, rpc.serverId()));
            }

            if (!error) {
                // Emit all buffered entries (logs + at most one data batch) in order.
                for (OutputCollector.Entry e : coll.entries()) {
                    w.writeBatch(e.root(), e.customMetadata());
                    e.root().close();
                }
                // If the producer isn't finished, append a zero-row state-token batch so the
                // client knows to call /exchange to continue. Finished streams just EOS.
                if (!coll.finished()) {
                    StateToken token = new StateToken(
                            StateSerializer.serialize(state),
                            serializeSchema(outputSchema),
                            serializeSchema(inputSchema),
                            newStreamId(), System.currentTimeMillis() / 1000);
                    Map<String, String> md = Map.of(Metadata.STREAM_STATE,
                            new String(token.pack(signingKey, currentPrincipal()), StandardCharsets.US_ASCII));
                    Wire.writeZeroBatch(w, outputSchema, md);
                }
            }
        }
    }

    private void writeExchangeInitToken(ByteArrayOutputStream out, RpcStream<?> streamResult,
                                         OutputCollectorSink sink) throws IOException {
        Schema outputSchema = streamResult.outputSchema();
        Schema inputSchema = streamResult.inputSchema();
        StateToken token = new StateToken(
                StateSerializer.serialize(streamResult.state()),
                serializeSchema(outputSchema),
                serializeSchema(inputSchema),
                newStreamId(), System.currentTimeMillis() / 1000);
        try (IpcStreamWriter w = new IpcStreamWriter(out)) {
            w.writeSchema(outputSchema);
            sink.bind(w, outputSchema);
            Map<String, String> md = Map.of(Metadata.STREAM_STATE,
                    new String(token.pack(signingKey, currentPrincipal()), StandardCharsets.US_ASCII));
            Wire.writeZeroBatch(w, outputSchema, md);
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
        // Error streams are zero-row metadata, well under the cap; use unbounded so we never lose
        // the error message itself if a different code path tripped maxResponseBytes earlier.
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Wire.writeErrorStream(out, RpcStream.EMPTY_SCHEMA, t, rpc.serverId());
        return out.toByteArray();
    }

    private static String newStreamId() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    private static byte[] serializeSchema(Schema schema) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            WriteChannel ch = new WriteChannel(Channels.newChannel(bos));
            MessageSerializer.serialize(ch, schema);
            return bos.toByteArray();
        } catch (IOException e) { throw new IllegalStateException("schema serialize failed", e); }
    }

    private static Schema deserializeSchema(byte[] b) {
        try {
            ReadChannel rc = new ReadChannel(Channels.newChannel(new ByteArrayInputStream(b)));
            return MessageSerializer.deserializeSchema(rc);
        } catch (IOException e) { throw new IllegalStateException("schema deserialize failed", e); }
    }

    private static VectorSchemaRoot copyRoot(VectorSchemaRoot src) {
        VectorSchemaRoot dst = VectorSchemaRoot.create(src.getSchema(), Allocators.root());
        dst.allocateNew();
        int rows = src.getRowCount();
        for (int c = 0; c < src.getSchema().getFields().size(); c++) {
            FieldVector sv = src.getVector(c);
            FieldVector dv = dst.getVector(c);
            for (int r = 0; r < rows; r++) dv.copyFromSafe(r, r, sv);
        }
        dst.setRowCount(rows);
        return dst;
    }

    /** Collects log Messages during init so they can be flushed into the response stream. */
    private final class OutputCollectorSink implements Consumer<Message> {
        private final List<Message> buffer = new ArrayList<>();
        private IpcStreamWriter writer;
        private Schema schema;

        void bind(IpcStreamWriter w, Schema s) throws IOException {
            this.writer = w; this.schema = s;
            for (Message msg : buffer) writeNow(msg);
            buffer.clear();
        }
        void detach() { this.writer = null; this.schema = null; }

        @Override
        public void accept(Message msg) {
            if (writer != null) {
                try { writeNow(msg); } catch (IOException e) { throw new RuntimeException(e); }
            } else {
                buffer.add(msg);
            }
        }

        private void writeNow(Message msg) throws IOException {
            Map<String, String> md = msg.addToMetadata(null);
            md.put(Metadata.SERVER_ID, rpc.serverId());
            Wire.writeZeroBatch(writer, schema, md);
        }
    }
}
