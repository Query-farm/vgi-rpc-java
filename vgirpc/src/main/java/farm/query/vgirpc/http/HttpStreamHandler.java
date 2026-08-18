// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.AuthContext;
import farm.query.vgirpc.AuthScope;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.CallOutcome;
import farm.query.vgirpc.DispatchHook;
import farm.query.vgirpc.DispatchInfo;
import farm.query.vgirpc.MethodType;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.RpcMethodInfo;
import farm.query.vgirpc.RpcServer;
import farm.query.vgirpc.RpcStream;
import farm.query.vgirpc.StreamState;
import farm.query.vgirpc.external.ExternalLocationConfig;
import farm.query.vgirpc.external.ExternalizedResponseCapExceededException;
import farm.query.vgirpc.external.Externalizer;
import farm.query.vgirpc.external.LocationResolver;
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
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.TransferPair;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Stateless HTTP streaming dispatch: each init / exchange request is a standalone
 * HTTP call that round-trips a signed state token in Arrow custom metadata.
 *
 * <p>Producer streams emit one {@code process()} turn per response, followed by a
 * zero-row continuation-token batch while unfinished; exchange streams piggy-back
 * the refreshed token on each data batch.</p>
 */
public final class HttpStreamHandler {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(HttpStreamHandler.class);

    private final RpcServer rpc;
    private final byte[] tokenKey;
    private final long tokenTtlSeconds;
    /**
     * Accelerates the fixed half of a stream's state. Purely an accelerator:
     * a miss reopens the call token the client echoed, so correctness never
     * depends on a hit. See {@link CallStateCache}.
     */
    private final CallStateCache callStates;
    private final long maxResponseBytes;
    /**
     * method name → concrete {@link StreamState} class. Seeded at construction by
     * introspecting the implementation's generic return types (the Java mirror of
     * Python's {@code _resolve_state_types}), so a continuation-only {@code /exchange}
     * — e.g. a relay resuming from a held token — works on a process that never
     * served the stream's {@code /init}. Implementations that keep a wildcard
     * return ({@code RpcStream<? extends ProducerState>}) are learned from their
     * first init call instead, and cannot be resumed on a fresh process.
     */
    private final Map<String, Class<? extends StreamState>> stateTypes = new ConcurrentHashMap<>();

    /**
     * Create a handler with a random per-process token key, no token TTL, and an
     * unbounded response size.
     *
     * @param rpc the dispatcher whose streaming methods are served
     */
    public HttpStreamHandler(RpcServer rpc) { this(rpc, null, 0, Long.MAX_VALUE); }

    /**
     * @param tokenKey       AEAD master key (32 bytes) for stream state
     *     tokens; when {@code null} a random per-process key is generated
     *     (tokens won't survive restarts or load-balance across workers).
     * @param tokenTtlSeconds  maximum token age in seconds; {@code 0} disables
     *     TTL enforcement.
     * @param maxResponseBytes per-call response cap. Exceeding it raises
     *     {@link PayloadTooLargeException} which the caller maps to HTTP 413;
     *     producers of large batches must use the external-location protocol.
     */
    public HttpStreamHandler(RpcServer rpc, byte[] tokenKey, long tokenTtlSeconds, long maxResponseBytes) {
        this(rpc, tokenKey, tokenTtlSeconds, maxResponseBytes, CallStateCache.DEFAULT_MAX_ENTRIES);
    }

    /**
     * @param tokenKey       AEAD master key (32 bytes) for stream state
     *     tokens; when {@code null} a random per-process key is generated.
     * @param tokenTtlSeconds  maximum token age in seconds; {@code 0} disables
     *     TTL enforcement.
     * @param maxResponseBytes per-call response cap.
     * @param callStateCacheMaxEntries entry ceiling for the call-state cache;
     *     {@code 0} disables it, forcing every continuation to re-open the
     *     call token the client echoed.
     */
    public HttpStreamHandler(RpcServer rpc, byte[] tokenKey, long tokenTtlSeconds, long maxResponseBytes,
                              int callStateCacheMaxEntries) {
        if (tokenTtlSeconds < 0) {
            throw new IllegalArgumentException("tokenTtlSeconds must be >= 0, got " + tokenTtlSeconds);
        }
        if (maxResponseBytes <= 0) {
            throw new IllegalArgumentException("maxResponseBytes must be > 0, got " + maxResponseBytes);
        }
        this.rpc = rpc;
        if (tokenKey != null) {
            this.tokenKey = tokenKey.clone();
        } else {
            this.tokenKey = new byte[32];
            new SecureRandom().nextBytes(this.tokenKey);
        }
        this.tokenTtlSeconds = tokenTtlSeconds;
        this.callStates = new CallStateCache(tokenTtlSeconds, callStateCacheMaxEntries);
        this.maxResponseBytes = maxResponseBytes;
        seedStateTypes();
    }

    /**
     * Resolve each stream method's concrete state class from the implementation's
     * declared generic return type ({@code RpcStream<Counter> produce_n(long)}).
     * Wildcards, type variables, and abstract state types are skipped — those
     * methods fall back to init-time learning.
     */
    private void seedStateTypes() {
        Class<?> implClass = rpc.implementation().getClass();
        for (RpcMethodInfo info : rpc.methods().values()) {
            if (info.methodType() != MethodType.STREAM) continue;
            Method implMethod;
            try {
                implMethod = implClass.getMethod(
                        info.reflectMethod().getName(), info.reflectMethod().getParameterTypes());
            } catch (NoSuchMethodException e) {
                continue;
            }
            Class<? extends StreamState> cls = concreteStateArg(implMethod.getGenericReturnType());
            if (cls != null) stateTypes.put(info.name(), cls);
        }
    }

    /** The concrete {@link StreamState} type argument of an {@code RpcStream<X>} return, or null. */
    private static Class<? extends StreamState> concreteStateArg(Type returnType) {
        if (!(returnType instanceof ParameterizedType pt)) return null;
        if (!(pt.getRawType() instanceof Class<?> raw) || !RpcStream.class.isAssignableFrom(raw)) return null;
        Type[] args = pt.getActualTypeArguments();
        if (args.length != 1 || !(args[0] instanceof Class<?> c)) return null;
        if (!StreamState.class.isAssignableFrom(c) || Modifier.isAbstract(c.getModifiers())) return null;
        return c.asSubclass(StreamState.class);
    }

    // --- Access-log telemetry ---------------------------------------------

    /**
     * One HTTP turn of a stream call, and the access-log record it produces.
     *
     * <p>A stream over HTTP is not one dispatch but a chain of them: an
     * {@code /init} and then an {@code /exchange} per continuation, each its own
     * request with its own body, status and byte counts. The spec follows the
     * wire — one record per turn, all sharing the {@code stream_id} minted at
     * init — rather than pretending the chain is a single call whose duration
     * spans the client's think time.
     *
     * <p>Opened once the turn is a genuine dispatch: a malformed body or an
     * unresolvable token is refused before a method runs and produces no
     * record, matching the reference.
     */
    private final class StreamTurn implements AutoCloseable {
        private final DispatchHook hook;
        private final DispatchInfo info;
        private final Object token;
        /** Set when the turn threw. Errors written into the body instead are
         *  picked up from {@link CallOutcome}, which the writer already records. */
        private Throwable thrown;

        StreamTurn(DispatchHook hook, DispatchInfo info, Object token) {
            this.hook = hook;
            this.info = info;
            this.token = token;
        }

        @Override
        public void close() {
            Throwable err = thrown != null ? thrown : CallOutcome.currentError();
            try {
                hook.onDispatchEnd(token, info, null, err);
            } catch (Throwable t) {
                LOG.warn("dispatch hook end error: {}", t.toString());
            }
        }
    }

    /**
     * Start a turn's telemetry, or return {@code null} when nothing is listening.
     *
     * @param method the stream method being dispatched
     * @param streamId the stream's lifecycle id, shared by every turn
     */
    private StreamTurn beginTurn(String method, String streamId) {
        DispatchHook hook = rpc.dispatchHook();
        if (hook == null) return null;
        DispatchInfo info = new DispatchInfo();
        info.method = method;
        info.methodType = "stream";
        info.streamId = streamId;
        info.serverId = rpc.serverId();
        info.protocol = rpc.protocolName();
        info.protocolHash = rpc.protocolHash();
        info.protocolVersion = rpc.protocolVersion();
        AuthScope.Scope scope = AuthScope.current();
        AuthContext auth = scope.auth();
        info.principal = auth != null && auth.principal() != null ? auth.principal() : "";
        info.authDomain = auth != null && auth.domain() != null ? auth.domain() : "";
        info.authenticated = auth != null && auth.authenticated();
        info.claims = auth != null ? auth.claims() : null;
        info.transportMetadata = scope.transportMetadata();
        Object token = null;
        try {
            token = hook.onDispatchStart(info);
        } catch (Throwable t) {
            LOG.warn("dispatch hook start error: {}", t.toString());
        }
        return new StreamTurn(hook, info, token);
    }

    /** Record a field on a turn that may not exist (no hook installed). */
    private static void onTurn(StreamTurn turn, Consumer<DispatchInfo> mutation) {
        if (turn != null) mutation.accept(turn.info);
    }

    /** Handle {@code POST /{method}/init}. Returns response IPC bytes. */
    public byte[] handleInit(String method, byte[] requestBody) throws Exception {
        RpcMethodInfo info = rpc.methods().get(method);
        if (info == null) return errorStream(new IllegalArgumentException("Unknown method: " + method));

        Map<String, Object> kwargs;
        // The init request's batch metadata carries per-call signals the producer's
        // first tick must see (e.g. vgi.cache.if_none_match for conditional
        // revalidation). The subprocess transport delivers them as the tick's input
        // batch metadata; http must do the same or a producer that reads them
        // silently behaves as if they were absent.
        Map<String, String> requestMeta;
        LocationResolver.Resolved resolved = null;
        try (IpcStreamReader r = new IpcStreamReader(new ByteArrayInputStream(requestBody), Allocators.root())) {
            Map<String, String> meta = r.readNextBatch();
            if (meta == null) return errorStream(new RuntimeException("empty request"));
            VectorSchemaRoot root = r.root();
            LocationResolver resolver = rpc.locationResolver();
            if (resolver != null && LocationResolver.isPointer(root.getRowCount(), meta)) {
                try {
                    resolved = resolver.resolve(meta);
                    root = resolved.root();
                    meta = resolved.customMetadata();
                } catch (Exception e) {
                    return errorStream(e);
                }
            }
            Wire.validateRequestVersion(meta);
            String urlMethod = Wire.requireMethodName(meta);
            if (!method.equals(urlMethod)) {
                return errorStream(new ClassCastException(
                        "Method name mismatch: URL has '" + method + "' but metadata has '" + urlMethod + "'"));
            }
            requestMeta = Map.copyOf(meta);
            kwargs = root.getRowCount() == 0
                    ? new LinkedHashMap<>()
                    : (resolved != null
                        ? Marshalling.decodeRow(root, null, root.getSchema())
                        : Marshalling.decodeRow(root, r.dictionaryProvider(), r.wireSchema()));
        } finally {
            if (resolved != null) resolved.root().close();
        }

        OutputCollectorSink sink = new OutputCollectorSink();
        CallContext ctx = buildCallContext(method, sink);

        // Minted here rather than inside mintInitTokens so the init record
        // carries it even when the producer finishes in one turn and no
        // continuation token is ever issued — and so every later turn's record
        // can be joined to this one.
        String streamId = newStreamId();
        try (StreamTurn turn = beginTurn(method, streamId)) {
            // The request body is already a self-contained Arrow IPC stream, so
            // it is logged verbatim: byte-faithful, metadata intact, and free.
            // Only the pipe transport, which reads from a shared stream with no
            // discrete body, has to re-frame the batch.
            onTurn(turn, i -> i.requestData = requestBody);
            try {
                return runInit(method, info, kwargs, requestMeta, ctx, sink, streamId, turn);
            } catch (Throwable t) {
                if (turn != null) turn.thrown = t;
                throw t;
            }
        }
    }

    /** The body of {@code /init}, wrapped by {@link #handleInit}'s telemetry. */
    private byte[] runInit(String method, RpcMethodInfo info, Map<String, Object> kwargs,
                            Map<String, String> requestMeta, CallContext ctx,
                            OutputCollectorSink sink, String streamId, StreamTurn turn) throws Exception {
        RpcStream<?> streamResult;
        try {
            Object[] args = ParameterBinder.bind(info.reflectMethod(), kwargs, ctx);
            streamResult = (RpcStream<?>) info.reflectMethod().invoke(rpc.implementation(), args);
        } catch (InvocationTargetException ie) {
            return errorStream(ie.getCause() != null ? ie.getCause() : ie, sink);
        } catch (Throwable t) {
            return errorStream(t, sink);
        }
        // Record the concrete state class for this method so /exchange can rehydrate.
        stateTypes.put(method, streamResult.state().getClass());

        // The /init response is soft-capped: producer streams may emit a single
        // batch larger than maxResponseBytes and follow it with a continuation
        // token so the client picks up the rest via /exchange.  Exchange-init
        // responses are tiny (just a token) and never overflow.
        //
        // The bound here is a runaway-producer guard, not a contract: 16x the
        // configured cap (or 256 MiB if no cap is set) is generous enough for
        // any reasonable single-emit producer but stops {@code rows=Long.MAX}
        // from OOM-ing the worker.
        long hardCeiling = maxResponseBytes < Long.MAX_VALUE / 16
                ? Math.max(maxResponseBytes * 16, 256L << 20)
                : Long.MAX_VALUE;
        BoundedByteArrayOutputStream out = new BoundedByteArrayOutputStream(hardCeiling);
        if (streamResult.header() != null) {
            writeHeaderIpcStream(out, streamResult.header(), sink);
        }
        if (streamResult.isProducer()) {
            writeProducerRun(out, streamResult, ctx, sink, requestMeta, streamId, turn);
        } else {
            writeExchangeInitToken(out, streamResult, sink, streamId, turn);
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

        DictionaryProvider inputDicts = req.inputDicts();
        LocationResolver.Resolved resolved = null;
        try {
        try (VectorSchemaRoot ownedInput = req.inputRoot()) {
            VectorSchemaRoot actualInput = ownedInput;
            Map<String, String> requestMeta = req.meta();
            LocationResolver resolver = rpc.locationResolver();
            if (resolver != null && LocationResolver.isPointer(ownedInput.getRowCount(), requestMeta)) {
                try {
                    resolved = resolver.resolve(requestMeta);
                    actualInput = resolved.root();
                    requestMeta = resolved.customMetadata();
                } catch (Exception e) {
                    return errorStream(e);
                }
            }
            ExchangeRequest effectiveRequest = new ExchangeRequest(requestMeta, actualInput, inputDicts);
            String tokenB64 = requestMeta.get(Metadata.STREAM_STATE);
            if (tokenB64 == null) {
                return errorStream(new RuntimeException("Missing state token in exchange request"));
            }
            String principal = currentPrincipal();
            // Open the cursor FIRST: its AEAD tag covers the call id and its
            // AAD covers the caller, so the id is authenticated before it is
            // used to resolve anything. See resolveCall for why that ordering
            // is the whole security argument for the cache.
            StateToken token;
            try {
                token = StateToken.unpack(tokenB64.getBytes(StandardCharsets.US_ASCII),
                        tokenKey, tokenTtlSeconds, principal);
            } catch (Exception e) {
                return errorStream(e);
            }
            CallToken call;
            try {
                call = resolveCall(token, req.meta().get(Metadata.CALL_STATE), principal);
            } catch (Exception e) {
                return errorStream(e);
            }

            // The turn is a real dispatch only once the cursor has opened and
            // named a call: everything above refuses the request before any
            // state is rehydrated, and produces no record — same boundary the
            // reference draws. The stream id rides in the call token, so every
            // continuation's record joins the init's without server state.
            try (StreamTurn turn = beginTurn(method, call.streamId())) {
                // The plaintext the client's opaque AEAD cursor decrypted to.
                // Logging the ciphertext would give a reader nothing they could
                // decode without the server's token key.
                onTurn(turn, i -> i.requestState = token.state());
                try {
                    return runExchange(method, effectiveRequest, actualInput, token, call, principal, inputDicts, turn);
                } catch (Throwable t) {
                    if (turn != null) turn.thrown = t;
                    throw t;
                }
            }
        }
        } finally {
            if (resolved != null) resolved.root().close();
            closeDictionaries(inputDicts);
        }
    }

    /** The body of {@code /exchange}, wrapped by {@link #handleExchange}'s telemetry. */
    private byte[] runExchange(String method, ExchangeRequest req, VectorSchemaRoot ownedInput,
                                StateToken token, CallToken call, String principal,
                                DictionaryProvider inputDicts, StreamTurn turn) throws Exception {
        Class<? extends StreamState> stateCls = stateTypes.get(method);
        if (stateCls == null) {
            return errorStream(new IllegalStateException(
                    "Cannot resolve state type for method '" + method + "'"));
        }
        Schema outputSchema = deserializeSchema(call.outputSchema());
        Schema inputSchema = deserializeSchema(call.inputSchema());
        boolean isProducer = inputSchema.getFields().isEmpty();
        StreamState state = StateSerializer.deserialize(token.state(), stateCls);

        // The sink has to be kept — and later bound to this turn's response
        // writer. A stream over HTTP has no single output stream to bind once
        // (that is what serveStream does on the pipe transports); each turn
        // writes its own response, so an unbound sink buffers the turn's log
        // messages into a list that dies with the request. Dropping every log
        // after the first turn is worst on exactly the long streams whose
        // progress logs are the only in-band diagnostic a caller has.
        OutputCollectorSink sink = new OutputCollectorSink();
        CallContext ctx = buildCallContext(method, sink);

        if (req.meta().containsKey(Metadata.CANCEL)) {
            onTurn(turn, i -> i.cancelled = true);
            return handleCancel(outputSchema, state, ctx, sink);
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
                // Carries whatever the turn logged before it failed. Those lines
                // are the reason a caller can tell *where* a long stream broke,
                // and the pipe transports deliver them (their sink is bound to
                // the output stream for the whole tick loop) — so an HTTP turn
                // that answered with a bare error batch was strictly less
                // diagnosable for the same worker code.
                return errorStream(t, sink);
            }
            return writeExchangeResponse(collector, state, token, outputSchema, isProducer, principal,
                    inputDicts, turn, sink);
        }
    }

    // --- handleExchange sub-steps -----------------------------------------

    /** Parsed exchange-request body: metadata (including the state token) plus the (owned) input batch. */
    private record ExchangeRequest(Map<String, String> meta, VectorSchemaRoot inputRoot, DictionaryProvider inputDicts) {}

    private static ExchangeRequest parseExchangeRequest(byte[] body) throws IOException {
        try (IpcStreamReader r = new IpcStreamReader(new ByteArrayInputStream(body), Allocators.root())) {
            Map<String, String> meta = r.readNextBatch();
            if (meta == null) throw new RuntimeException("empty exchange request");
            // Copy the input because the reader is going out of scope.
            VectorSchemaRoot copied = copyRoot(r.root());
            DictionaryProvider inputDicts = copyDictionaries(r.dictionaryProvider());
            return new ExchangeRequest(meta, copied, inputDicts);
        }
    }

    private byte[] handleCancel(Schema outputSchema, StreamState state, CallContext ctx,
                                 OutputCollectorSink sink) throws IOException {
        // on_cancel is best-effort; the client has already decided it's done.
        try { state.onCancel(ctx); } catch (Exception ignore) { /* reported via onCancel contract */ }
        BoundedByteArrayOutputStream out = new BoundedByteArrayOutputStream(maxResponseBytes);
        try (IpcStreamWriter w = new IpcStreamWriter(out)) {
            w.writeSchema(outputSchema);
            // Anything onCancel logged still belongs to the caller — this
            // response is the last one it will ever read on this stream.
            sink.bind(w, outputSchema);
        } finally {
            sink.detach();
        }
        return out.toByteArray();
    }

    private byte[] writeExchangeResponse(OutputCollector collector, StreamState state, StateToken priorToken,
                                         Schema outputSchema, boolean isProducer, String principal,
                                         DictionaryProvider inputDicts, StreamTurn turn,
                                         OutputCollectorSink sink) throws IOException {
        boolean finished = collector.finished();
        // Absent on the terminal turn: there is no outbound state when the
        // stream closes, which is exactly what its absence in the record means.
        String newTokenStr = finished ? null : serializeContinuationToken(state, priorToken, principal, turn);

        BoundedByteArrayOutputStream out = new BoundedByteArrayOutputStream(maxResponseBytes);
        try (IpcStreamWriter w = new IpcStreamWriter(out)) {
            w.writeSchema(outputSchema);
            // Flushes whatever process() logged through the CallContext. It ran
            // before this writer existed, so the sink buffered it; binding here
            // drains that buffer ahead of the data batch, matching the order the
            // pipe transports put on the wire.
            sink.bind(w, outputSchema);

            // Logs and other non-data entries flow through first; the data entry is held for token-attachment.
            OutputCollector.Entry data = null;
            for (OutputCollector.Entry e : collector.entries()) {
                if (e.isData()) { data = e; continue; }
                w.writeBatch(e.root(), e.customMetadata(), dictOr(e.dictionaryProvider(), inputDicts));
                e.root().close();
            }

            if (!isProducer && data != null && newTokenStr != null) {
                // Exchange continuation: piggy-back the new token on the data batch's metadata.
                // Externalising keeps it — Externalizer merges the batch's metadata into the
                // pointer's, so the cursor rides the pointer batch the client actually reads.
                Map<String, String> md = new LinkedHashMap<>();
                if (data.customMetadata() != null) md.putAll(data.customMetadata());
                md.put(Metadata.STREAM_STATE, newTokenStr);
                writeStreamBatch(w, outputSchema, data.root(), md,
                        dictOr(data.dictionaryProvider(), inputDicts), true);
                data.root().close();
            } else {
                // Producer continuation (or no data): emit the data batch as-is, token as a trailing zero-row batch.
                if (data != null) {
                    writeStreamBatch(w, outputSchema, data.root(), data.customMetadata(),
                            dictOr(data.dictionaryProvider(), inputDicts), true);
                    data.root().close();
                }
                if (newTokenStr != null) {
                    Wire.writeZeroBatch(w, outputSchema, Map.of(Metadata.STREAM_STATE, newTokenStr));
                }
            }
        } finally {
            sink.detach();
        }
        return out.toByteArray();
    }

    private String serializeContinuationToken(StreamState state, StateToken priorToken, String principal,
                                               StreamTurn turn) {
        byte[] newStateBytes = StateSerializer.serialize(state);
        onTurn(turn, i -> i.responseState = newStateBytes);
        StateToken newToken = new StateToken(newStateBytes, priorToken.callId(),
                System.currentTimeMillis() / 1000);
        return new String(newToken.pack(tokenKey, principal), StandardCharsets.US_ASCII);
    }

    /**
     * Resolve a stream's fixed half for an already-authenticated cursor.
     *
     * <p>Order matters, and it is the whole security argument for the cache.
     * The cursor is opened first by the caller; its AEAD tag covers the call
     * id and its AAD covers the caller's identity. Only then is that
     * authenticated id used as a cache key. A client cannot name a call id
     * the server did not mint for it, so a cache hit can never hand back
     * another principal's call state — and on a hit the presented call token
     * is not consulted at all, which is exactly the work being avoided.</p>
     *
     * <p>On a miss (cold process, evicted entry, or a request load-balanced
     * to a node that never saw this stream's {@code /init}) the client's call
     * token is opened and verified, and its embedded call id must match the
     * one the cursor named.</p>
     */
    private CallToken resolveCall(StateToken cursor, String callTokenB64, String principal) {
        CallToken cached = callStates.get(cursor.callId(), principal);
        if (cached != null) {
            return cached;
        }
        if (callTokenB64 == null) {
            throw new IllegalArgumentException("Missing call token in exchange request");
        }
        CallToken call = CallToken.unpack(callTokenB64.getBytes(StandardCharsets.US_ASCII),
                tokenKey, tokenTtlSeconds, principal);
        if (!java.util.Arrays.equals(call.callId(), cursor.callId())) {
            // The cursor named a different call. Uniform message: reachable
            // only by pairing two tokens the same principal legitimately
            // holds, so it carries nothing worth distinguishing.
            throw new IllegalArgumentException("Malformed state token");
        }
        callStates.put(cursor.callId(), principal, call);
        return call;
    }

    /** Mint a stream's call id, call token, and first cursor at {@code /init}. */
    private Map<String, String> mintInitTokens(StreamState state, Schema outputSchema,
                                                Schema inputSchema, String principal,
                                                String streamId, StreamTurn turn) {
        byte[] callId = new byte[Tokens.CALL_ID_LEN];
        new java.security.SecureRandom().nextBytes(callId);
        long now = System.currentTimeMillis() / 1000;

        CallToken call = new CallToken(serializeSchema(outputSchema), serializeSchema(inputSchema),
                streamId, callId, now);
        // Warm the cache with what we already hold, so this stream's first
        // continuation does not have to open the token it was just handed.
        callStates.put(callId, principal, call);

        byte[] stateBytes = StateSerializer.serialize(state);
        onTurn(turn, i -> i.responseState = stateBytes);
        StateToken cursor = new StateToken(stateBytes, callId, now);
        return Map.of(
                Metadata.STREAM_STATE,
                new String(cursor.pack(tokenKey, principal), StandardCharsets.US_ASCII),
                Metadata.CALL_STATE,
                new String(call.pack(tokenKey, principal), StandardCharsets.US_ASCII));
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
                                   CallContext ctx, OutputCollectorSink sink,
                                   Map<String, String> requestMeta,
                                   String streamId, StreamTurn turn) throws IOException {
        Schema outputSchema = streamResult.outputSchema();
        Schema inputSchema = streamResult.inputSchema();
        StreamState state = streamResult.state();
        try (IpcStreamWriter w = new IpcStreamWriter(out)) {
            w.writeSchema(outputSchema);
            sink.bind(w, outputSchema);

            OutputCollector coll = new OutputCollector(outputSchema, rpc.serverId(), true);
            boolean error = false;
            try (VectorSchemaRoot tickInput =
                         VectorSchemaRoot.create(RpcStream.EMPTY_SCHEMA, Allocators.root())) {
                state.process(new AnnotatedBatch(tickInput, requestMeta), coll, ctx);
            } catch (Throwable t) {
                error = true;
                Wire.writeZeroBatch(w, outputSchema, Wire.errorMetadata(t, rpc.serverId()));
                // The exception goes *into* the response, behind the stream
                // header the client is already committed to reading, instead of
                // replacing it. That makes this the one error the transport must
                // not advertise with X-VGI-RPC-Error — a client that believes it
                // stops reading at the header stream's end-of-stream and never
                // sees this batch. Matches the reference, whose producer loop
                // leaves the response status at a plain 200 here.
                CallOutcome.suppressResponseFlag();
            }

            if (!error) {
                // Emit all buffered entries (logs + at most one data batch) in order.
                for (OutputCollector.Entry e : coll.entries()) {
                    writeStreamBatch(w, outputSchema, e.root(), e.customMetadata(),
                            e.dictionaryProvider(), e.isData());
                    e.root().close();
                }
                // If the producer isn't finished, append a zero-row state-token batch so the
                // client knows to call /exchange to continue. Finished streams just EOS.
                if (!coll.finished()) {
                    Map<String, String> md = mintInitTokens(state, outputSchema, inputSchema,
                            currentPrincipal(), streamId, turn);
                    Wire.writeZeroBatch(w, outputSchema, md);
                }
            }
        }
    }

    private void writeExchangeInitToken(ByteArrayOutputStream out, RpcStream<?> streamResult,
                                         OutputCollectorSink sink,
                                         String streamId, StreamTurn turn) throws IOException {
        Schema outputSchema = streamResult.outputSchema();
        Schema inputSchema = streamResult.inputSchema();
        Map<String, String> md = mintInitTokens(streamResult.state(), outputSchema, inputSchema,
                currentPrincipal(), streamId, turn);
        try (IpcStreamWriter w = new IpcStreamWriter(out)) {
            w.writeSchema(outputSchema);
            sink.bind(w, outputSchema);
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

    /**
     * An error stream that first flushes anything the failed call logged.
     *
     * <p>Same shape as {@link #errorStream(Throwable)} — log batches then the
     * EXCEPTION batch — which is exactly the order a client reads them in, so
     * the logs reach the log sink and the exception is still what gets raised.
     */
    private byte[] errorStream(Throwable t, OutputCollectorSink sink) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (IpcStreamWriter w = new IpcStreamWriter(out)) {
            w.writeSchema(RpcStream.EMPTY_SCHEMA);
            sink.bind(w, RpcStream.EMPTY_SCHEMA);
            Wire.writeZeroBatch(w, RpcStream.EMPTY_SCHEMA, Wire.errorMetadata(t, rpc.serverId()));
        } finally {
            sink.detach();
        }
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

    /**
     * Detach {@code src}'s columns into a new root that outlives the reader.
     * Uses {@link TransferPair} (a zero-copy buffer move) rather than row-wise
     * {@code copyFromSafe}: the latter throws {@code UnsupportedOperationException}
     * for TIMESTAMP_TZ and misindexes union children (see the same TransferPair
     * fix on the shm resolve path). After {@code transfer()} the reader's source
     * vectors are empty and close cleanly.
     */
    private static VectorSchemaRoot copyRoot(VectorSchemaRoot src) {
        int rows = src.getRowCount();
        List<FieldVector> moved = new ArrayList<>();
        for (FieldVector sv : src.getFieldVectors()) {
            org.apache.arrow.vector.util.TransferPair tp = sv.getTransferPair(Allocators.root());
            tp.transfer();
            moved.add((FieldVector) tp.getTo());
        }
        return new VectorSchemaRoot(src.getSchema().getFields(), moved, rows);
    }

    /** Transfer the dictionaries out of {@code src} into an owned provider that
     *  survives the source reader's close; null when there are none. */
    private static DictionaryProvider copyDictionaries(DictionaryProvider src) {
        if (src == null) return null;
        Set<Long> ids = src.getDictionaryIds();
        if (ids.isEmpty()) return null;
        DictionaryProvider.MapDictionaryProvider out = new DictionaryProvider.MapDictionaryProvider();
        for (Long id : ids) {
            Dictionary d = src.lookup(id);
            TransferPair tp = d.getVector().getTransferPair(Allocators.root());
            tp.transfer();
            out.put(new Dictionary((FieldVector) tp.getTo(), d.getEncoding()));
        }
        return out;
    }

    private static void closeDictionaries(DictionaryProvider provider) {
        if (provider instanceof DictionaryProvider.MapDictionaryProvider m) {
            for (Long id : m.getDictionaryIds()) {
                Dictionary d = m.lookup(id);
                if (d != null && d.getVector() != null) d.getVector().close();
            }
        }
    }

    private static DictionaryProvider dictOr(DictionaryProvider a, DictionaryProvider b) { return a != null ? a : b; }

    /**
     * Write one stream output batch, routing data batches through the
     * external-location channel when one is configured.
     *
     * <p>Streams used to skip this entirely: only unary results
     * ({@code RpcServer.writeResult}) and the pipe-family tick loop
     * ({@code RpcServer.flushEntries}) externalised, so an HTTP worker that
     * advertised {@code VGI-Externalization-Enabled} and a threshold applied
     * neither to the responses where the bytes actually are. Nothing noticed,
     * because inline delivery is observationally identical to a resolved
     * pointer — right up to the point an operator caps the external channel and
     * the cap has nothing to govern.
     *
     * <p>Dictionary-encoded batches stay inline. {@link Externalizer} serialises
     * the payload without a dictionary provider, so an externalised dictionary
     * batch would upload an undecodable stream; inline is the correct answer
     * until the uploader can carry dictionaries.
     *
     * <p>{@code outputSchema} is handed to the uploader so the standalone
     * payload declares the schema this stream declared. A collector root often
     * differs from it in field nullability only, which is invisible inline —
     * the batch rides a stream whose schema was declared up front — and reads
     * back as a schema mismatch the moment it becomes its own stream.
     */
    private void writeStreamBatch(IpcStreamWriter w, Schema outputSchema, VectorSchemaRoot root,
                                   Map<String, String> meta, DictionaryProvider dicts,
                                   boolean isData) throws IOException {
        ExternalLocationConfig cfg = rpc.externalConfig();
        if (isData && dicts == null && cfg != null && cfg.storage() != null) {
            Externalizer.Pointer ptr;
            try {
                ptr = Externalizer.maybeExternalize(root, meta, cfg, outputSchema);
            } catch (ExternalizedResponseCapExceededException cap) {
                // A refusal, not a failure — see Externalizer's docs.
                throw cap;
            } catch (Exception up) {
                // Upload failed — fall through and write inline rather than
                // failing the turn; the client still gets valid data.
                ptr = null;
            }
            if (ptr != null) {
                try (VectorSchemaRoot pointer = ptr.root()) {
                    w.writeBatch(pointer, ptr.customMetadata());
                }
                return;
            }
        }
        w.writeBatch(root, meta, dicts);
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
