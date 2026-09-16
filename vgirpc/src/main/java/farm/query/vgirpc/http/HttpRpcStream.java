// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.RpcError;
import farm.query.vgirpc.RpcStream;
import farm.query.vgirpc.StreamState;
import farm.query.vgirpc.external.LocationResolver;
import farm.query.vgirpc.log.Message;
import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.wire.Allocators;
import farm.query.vgirpc.wire.IpcStreamReader;
import farm.query.vgirpc.wire.IpcStreamWriter;
import farm.query.vgirpc.wire.Metadata;
import farm.query.vgirpc.wire.Wire;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Consumer;

/**
 * A streaming call in progress over the HTTP transport: the client-side half of
 * {@code POST /{method}/init} followed by one {@code POST /{method}/exchange}
 * per turn.
 *
 * <p>The continuation state is <em>not</em> a header and not a session. Each
 * response carries an opaque, server-signed cursor
 * ({@link Metadata#STREAM_STATE}) in a batch's Arrow {@code custom_metadata},
 * and the next request echoes it back — together with the call token
 * ({@link Metadata#CALL_STATE}) minted once at init, which pins the stream's
 * fixed half (its schemas and stream id). Echoing the call token on every
 * request is what makes the stream survive being load-balanced onto a node that
 * never served its {@code /init}: without it a request succeeds only while some
 * node's call-state cache happens to be warm, which is a bug that appears under
 * load and nowhere else.</p>
 *
 * <p>The two call shapes read the cursor from different places, because the
 * server puts it in different places. A producer response ends with a
 * <em>trailing zero-row</em> token batch after the data; an exchange response
 * piggy-backs the token on the data batch's own metadata. Both are handled by
 * {@link #nextDataBatch(boolean)}, which strips the token keys so a caller never sees
 * transport bookkeeping in {@link AnnotatedBatch#customMetadata()}.</p>
 *
 * <p>Not thread-safe — one stream belongs to one caller, matching
 * {@code ClientStreamSession}. Batch ownership matches it too: the returned
 * root belongs to the reader and is recycled on the next call, so copy anything
 * you need to keep.</p>
 *
 * @param <S> the server-side {@link StreamState} type declared by the service
 *     interface; a compile-time marker only — {@link #state()} always throws on
 *     the client
 */
public final class HttpRpcStream<S extends StreamState> extends RpcStream<S> {

    private final HttpRpcConnection connection;
    private final String protocol;
    private final String method;
    private final Consumer<Message> onLog;
    private final ArrowSerializableRecord header;
    private final Schema outputSchema;

    /**
     * Reader over the current response's body stream, or {@code null} when the
     * last one was consumed to its end-of-stream marker.
     */
    private IpcStreamReader currentReader;
    /**
     * The cursor for the <em>next</em> turn, or {@code null} when this response
     * did not offer one — which is precisely how the server says "finished".
     */
    private String stateToken;
    /** The call token minted at init and echoed on every later request. */
    private String callToken;
    /**
     * The last <em>externalised</em> batch, which was fetched from storage
     * rather than read from the response and so is owned here, not by this
     * stream's reader. Freed on the next read and on close.
     */
    private ResolvedBatch resolvedBatch;
    /** Whether this HTTP response has already yielded its single allowed data batch. */
    private boolean dataSeenInCurrentResponse;
    private boolean closed;

    /**
     * Wrap an {@code /init} response that has already had its header stream (if
     * any) consumed.
     *
     * @param connection the connection that issues the continuation requests
     * @param protocol the routing key the stream started on; every continuation must stay on it
     * @param method the RPC method name, used to build the {@code /exchange} URL
     * @param initBody the init response, positioned at the start of the stream body
     * @param header the decoded {@code @StreamHeader} record, or {@code null}
     * @throws IOException if the body is not a readable IPC stream
     */
    HttpRpcStream(HttpRpcConnection connection, String protocol, String method,
                  ByteArrayInputStream initBody, ArrowSerializableRecord header) throws IOException {
        this.connection = connection;
        this.protocol = protocol;
        this.method = method;
        this.onLog = connection.onLog();
        this.header = header;
        IpcStreamReader reader = new IpcStreamReader(initBody, Allocators.root());
        try {
            // Reading the schema message up front is what makes outputSchema()
            // answerable before the first tick, matching the server-built
            // streams' contract; it consumes no batches.
            this.outputSchema = reader.schema();
        } catch (IOException | RuntimeException e) {
            // Nobody holds this stream yet, so nothing else will ever close the
            // reader's buffers if construction does not complete.
            try { reader.close(); } catch (Exception ignore) { /* best-effort */ }
            throw e;
        }
        this.currentReader = reader;
    }

    /**
     * Schema of the batches the worker emits, learned from the init response.
     *
     * @return the output schema
     */
    @Override public Schema outputSchema() { return outputSchema; }

    /**
     * Always {@link RpcStream#EMPTY_SCHEMA} on a client stream.
     *
     * <p>The input schema is never sent to the client: on an exchange the caller
     * supplies it one batch at a time, and on a producer there is none. This
     * mirrors {@code ClientStreamSession}, and it means {@link #isProducer()} —
     * which is derived from the input schema — reports {@code true} for a client
     * stream of either shape and should not be consulted.</p>
     *
     * @return the empty schema
     */
    @Override public Schema inputSchema() { return RpcStream.EMPTY_SCHEMA; }

    /**
     * Never available on a client stream — the state lives in the worker.
     *
     * @return never returns
     * @throws UnsupportedOperationException always
     */
    @Override public S state() {
        throw new UnsupportedOperationException("state() not available on client session");
    }

    /**
     * The {@code @StreamHeader} record the worker sent before the body.
     *
     * @return the header, or {@code null} when the method declares none
     */
    @Override public ArrowSerializableRecord header() { return header; }

    /**
     * Advance a producer stream and return its next data batch.
     *
     * <p>Unlike the stream transports there is no tick to send until the current
     * response is exhausted: the init response already carries the worker's
     * first turn. Only once its batches are consumed does this issue the
     * continuation POST that produces the next one — so a producer that finished
     * within init costs exactly one HTTP request.</p>
     *
     * @return the next output batch; its root is owned by the reader and is
     *     recycled on the following call
     * @throws NoSuchElementException when the producer has finished
     * @throws RpcError on a transport failure or a worker-reported error
     */
    @Override
    public AnnotatedBatch tick() {
        return tick(null);
    }

    /**
     * Advance a producer stream, stamping per-tick metadata on the request.
     *
     * <p>The tick batch stays zero-row — it carries no data, only whatever the
     * caller wants the worker to see for this one turn (a trace context, a
     * deadline, a watermark of its own). The transport's cursor keys are
     * layered <em>over</em> the supplied map rather than under it: a caller
     * that (accidentally or otherwise) sets {@code vgi_rpc.state} would
     * otherwise redirect the stream to a cursor of its choosing, which is a
     * different stream's state on a server that never expected to be asked.</p>
     *
     * <p>A tick that is answered from the response already in hand sends
     * nothing, so its metadata reaches no one — exactly as on the byte-stream
     * transports, where the same is true of a buffered batch.</p>
     *
     * @param customMetadata metadata to stamp on the tick request, or {@code null}
     * @return the next output batch; its root is owned by the reader and is
     *     recycled on the following call
     * @throws NoSuchElementException when the producer has finished
     * @throws RpcError on a transport failure or a worker-reported error
     */
    public AnnotatedBatch tick(Map<String, String> customMetadata) {
        ensureOpen();
        try {
            while (true) {
                if (currentReader == null) {
                    if (stateToken == null) throw new NoSuchElementException();
                    installResponse(connection.post(url(), continuationBody(false, customMetadata),
                            what("tick")));
                }
                AnnotatedBatch batch = nextDataBatch(true);
                if (batch != null) return batch;
                closeCurrentReader();
            }
        } catch (NoSuchElementException e) {
            close();
            throw e;
        } catch (IOException e) {
            closed = true;
            throw new RpcError("TransportError", "stream tick failed: " + e.getMessage(), "");
        }
    }

    /**
     * Send an input batch and return the worker's answer for it.
     *
     * @param input the input batch for this turn; its custom metadata is
     *     forwarded, with the transport's own token keys layered on top
     * @return the output batch; its root is owned by the reader and is recycled
     *     on the following call
     * @throws NoSuchElementException if the worker ended the stream instead of
     *     answering this input batch
     * @throws RpcError on a transport failure or a worker-reported error
     */
    @Override
    public AnnotatedBatch exchange(AnnotatedBatch input) {
        ensureOpen();
        try {
            // An exchange /init response is nothing but the token batch; read it
            // now so the first turn has a cursor to echo. Idempotent: after the
            // first exchange this just walks the previous response to its EOS.
            drainToTokens();
            if (stateToken == null) throw new NoSuchElementException();
            installResponse(connection.post(url(), exchangeBody(input), what("exchange")));
            AnnotatedBatch batch = nextDataBatch(false);
            if (batch == null) throw new NoSuchElementException();
            return batch;
        } catch (NoSuchElementException e) {
            // Same close-on-end-of-stream contract as ClientStreamSession: a
            // spent stream must not accept another input batch, or the caller
            // keeps POSTing a cursor the worker has already retired.
            close();
            throw e;
        } catch (IOException e) {
            closed = true;
            throw new RpcError("TransportError", "stream exchange failed: " + e.getMessage(), "");
        }
    }

    /**
     * Release this stream's reader. Idempotent.
     *
     * <p>Nothing is sent: HTTP stream state lives in a token the client simply
     * stops presenting, and the worker's copy is reclaimed by the token's TTL.
     * Use {@link #cancel()} to tell the worker now — that is the call that runs
     * its {@code onCancel} hook.</p>
     */
    @Override
    public void close() {
        if (closed) return;
        closed = true;
        releaseResolvedBatch();
        closeCurrentReader();
    }

    /**
     * Abort the stream and tell the worker, then {@link #close()}.
     *
     * <p>Sends one {@code /exchange} carrying {@link Metadata#CANCEL} beside the
     * current cursor, which is what makes the worker run
     * {@code StreamState.onCancel} and drop the state instead of holding it
     * until the token expires. Best-effort and idempotent: the caller has
     * already decided it is done, so a failure to deliver the notice must not
     * become an exception it has to handle.</p>
     */
    @Override
    public void cancel() {
        if (closed) return;
        // The cursor may still be sitting unread in the response in hand: a
        // producer's token batch trails its data, so a stream cancelled before
        // its first tick — or immediately after one — has never looked far
        // enough to have a cursor. Walk the response out first, or cancel()
        // silently does nothing on exactly the streams most worth cancelling.
        if (stateToken == null) {
            try {
                discardCurrentResponse();
            } catch (Exception ignore) {
                // Nothing left to cancel with; fall through to a local close.
            }
        }
        if (stateToken != null) {
            try {
                connection.post(url(), continuationBody(true, null), what("cancel"));
            } catch (Exception ignore) {
                // Best-effort: the worker's state expires with its token anyway.
            }
        }
        close();
    }

    /**
     * The continuation cursor this stream would present to resume, as captured
     * so far from the response in hand.
     *
     * <p>Opaque and server-signed: it is echoed back verbatim and is not
     * parseable by a client. Its <em>timing</em> is the subtle part. A producer
     * response carries its cursor in a zero-row batch that <em>trails</em> the
     * data batch, so immediately after {@link #tick()} returns, the cursor for
     * the turn just taken has not been read yet and this still reports the
     * previous one (or {@code null} on the first turn). {@link #settleCursor()}
     * reads the response out to that trailing batch, which is what a caller
     * pairing each batch with its resume point must do. An exchange response
     * piggy-backs the cursor on the data batch itself, so there it is current
     * as soon as the batch is returned.</p>
     *
     * <p>{@code null} means the worker offered no continuation — which is
     * precisely how it says the stream is finished.</p>
     *
     * @return the cursor for the next turn, or {@code null}
     */
    public String stateToken() { return stateToken; }

    /**
     * Read the response in hand out to its end so the cursor that follows the
     * data batch is captured.
     *
     * <p>Package-private because it exists for {@link HttpRawStream}, whose
     * contract is to hand back each batch <em>with</em> the token that resumes
     * after it. The typed {@link #tick()} deliberately does not do this: it
     * returns a root the reader owns and recycles, so reading on before the
     * caller has looked at the batch would hand back vectors whose contents
     * have already moved on. The raw path is safe because it reframes the batch
     * to bytes first.</p>
     *
     * <p>Relaying any log batches that trail the data is a second reason to do
     * it: they belong to the turn that produced them, and a caller that never
     * reads past its data batch would only see them one turn late.</p>
     */
    void settleCursor() {
        if (currentReader == null) return;
        try {
            discardCurrentResponse();
        } catch (IOException e) {
            closed = true;
            throw new RpcError("TransportError", "stream tick failed: " + e.getMessage(), "");
        }
    }

    // ------------------------------------------------------------------

    private void ensureOpen() {
        if (closed) throw new RpcError("ProtocolError", "RpcStream has been closed or cancelled", "");
    }

    private String url() { return connection.urlFor(protocol, method, "/exchange"); }

    private String what(String phase) { return method + "/exchange (" + phase + ")"; }

    /**
     * Read on until the next real data batch, or {@code null} at this response's
     * end-of-stream.
     *
     * <p>Everything a stream response can interleave is resolved here: log
     * batches go to the sink, an EXCEPTION batch becomes an {@link RpcError}
     * (terminal — the stream is closed before it is thrown, so a caller that
     * catches it cannot keep POSTing a retired cursor), every batch's cursor is
     * captured, and an externalized pointer is resolved in place (or refused
     * loudly when this connection was built without an
     * {@code ExternalLocationConfig}).
     *
     * <p>{@code skipCursorMarkers} is the one thing that cannot be decided from
     * the batch alone, and getting it wrong loses rows. A <em>producer</em>
     * response ends with a zero-row batch carrying nothing but the cursor — a
     * continuation marker that must be skipped, or every drain would yield a
     * phantom empty batch. An <em>exchange</em> response piggy-backs the cursor
     * on its single data batch, and that batch is legitimately zero-row whenever
     * the caller sent a zero-row input. The two are byte-identical on the wire,
     * so the call shape decides: a tick skips cursor-only markers, an exchange
     * takes the first data batch as the answer it asked for. (The reference
     * client draws the same line, in {@code __iter__} versus {@code exchange}.)
     *
     * @param skipCursorMarkers {@code true} on the producer path
     */
    private AnnotatedBatch nextDataBatch(boolean skipCursorMarkers) throws IOException {
        // The previous batch's caller-facing root, if it came from an external
        // fetch, is ours to free: nothing downstream knows the difference
        // between a resolved root and the reader's recycled one, so nothing
        // downstream can be asked to close it.
        releaseResolvedBatch();
        while (true) {
            Map<String, String> md = currentReader.readNextBatch();
            if (md == null) return null;
            VectorSchemaRoot root = currentReader.root();
            Wire.BatchKind kind = Wire.classify(root.getRowCount(), md);
            if (kind == Wire.BatchKind.LOG) {
                onLog.accept(Wire.messageFromMetadata(md));
                continue;
            }
            if (kind == Wire.BatchKind.ERROR) {
                RpcError error = Wire.errorFromMetadata(md);
                close();
                throw error;
            }
            boolean carriesToken = md.containsKey(Metadata.STREAM_STATE);
            if (carriesToken) captureTokens(md);
            if (skipCursorMarkers && carriesToken && root.getRowCount() == 0) continue;
            if (dataSeenInCurrentResponse) {
                RpcError error = new RpcError("ProtocolError",
                        method + ": HTTP stream response contained more than one data batch", "");
                close();
                throw error;
            }
            dataSeenInCurrentResponse = true;
            // Transport bookkeeping never reaches the caller, but the location
            // keys must survive as far as the resolver — they are the batch.
            Map<String, String> visible = carriesToken ? stripTokens(md) : md;
            if (LocationResolver.isPointer(root.getRowCount(), md)) {
                return resolvePointerBatch(visible);
            }
            return new AnnotatedBatch(root, visible, currentReader.dictionaryProvider(), null);
        }
    }

    /**
     * Fetch and decode an external-location pointer batch, returning the real
     * batch in its place.
     *
     * <p>An unresolvable pointer is a hard failure, never an empty batch — the
     * same contract {@code ClientStreamSession.resolvePointerBatch} states for
     * the byte-stream transports, and for the same reason: the worker has said,
     * explicitly, that rows exist somewhere this client cannot reach, and
     * handing the caller the pointer's zero-row body would be silent row loss
     * on exactly the path externalization exists for. Both "no
     * {@code ExternalLocationConfig}" and "the fetch failed" raise
     * {@link RpcError} typed {@code ExternalLocationError} so one catch covers
     * every transport and both call shapes.</p>
     *
     * <p>The metadata carried downstream is the resolver's merged view: the
     * pointer's own metadata minus the {@code vgi_rpc.location*} keys, overlaid
     * with the metadata the externalised batch was written with — the metadata
     * the batch would have had if it had travelled inline, which is the whole
     * point of the resolution being transparent.</p>
     */
    private AnnotatedBatch resolvePointerBatch(Map<String, String> pointerMeta) {
        ResolvedBatch resolved = connection.resolvePointer(pointerMeta);
        // Held for release on the next read / close — see releaseResolvedBatch().
        resolvedBatch = resolved;
        Map<String, String> md = resolved.customMetadata();
        // The cursor travels with the batch it belongs to, and on an
        // externalized turn that batch is the one in storage — the pointer
        // standing in for it carries only the location. A client that reads the
        // cursor off the pointer alone finds none, and the stream ends after
        // one turn: the failure is not "externalization is broken" but "the
        // second exchange returned nothing", which is why it is worth naming
        // here.
        if (md.containsKey(Metadata.STREAM_STATE) || md.containsKey(Metadata.CALL_STATE)) {
            captureTokens(md);
            md = stripTokens(md);
        }
        // The dictionaries travel with the batch: a dictionary-encoded column
        // handed on without them cannot be written or read at all, and the
        // failure lands a layer away from the fetch that dropped them.
        return new AnnotatedBatch(resolved.root(), md, resolved.dictionaries(), null);
    }

    /** Free the previous externalised batch, if any. */
    private void releaseResolvedBatch() {
        if (resolvedBatch == null) return;
        resolvedBatch.close();
        resolvedBatch = null;
    }

    /** Read the current response to its end, discarding batches, to capture its cursor. */
    private void discardCurrentResponse() throws IOException {
        if (currentReader == null) return;
        while (nextDataBatch(true) != null) {
            // Discarded: the caller is cancelling, so only the cursor matters.
        }
        closeCurrentReader();
    }

    /**
     * Walk the init response to its end so its tokens are captured, refusing
     * only what an exchange's init response genuinely cannot contain.
     *
     * <p>What it cannot contain is <em>rows</em>: an exchange produces output
     * only in answer to an input batch, so a populated batch sitting in the
     * init response means the caller opened a producer and is driving it as an
     * exchange — worth naming, because the alternative symptom is a stream that
     * silently skips its first turn.</p>
     *
     * <p>A zero-row batch carrying no cursor is <em>framing</em>, not data, and
     * must be walked past. The reference server emits exactly that for a
     * zero-column exchange: its init response is a bare zero-row, zero-column
     * batch with empty metadata, followed by the cursor batch. (With columns it
     * emits one batch carrying both.) Refusing the first shape would mean this
     * client cannot read what the reference server actually sends, which makes
     * the client wrong however reasonable the rule sounded.</p>
     */
    private void drainToTokens() throws IOException {
        if (currentReader == null) return;
        while (true) {
            AnnotatedBatch stray = nextDataBatch(true);
            if (stray == null) break;
            if (stray.root().getRowCount() > 0) {
                throw new RpcError("ProtocolError",
                        method + ": exchange() found a data batch in the init response; "
                                + "this is a producer stream — use tick()", "");
            }
            // Framing, not data — so it must not spend this response's
            // one-data-batch budget either.
            dataSeenInCurrentResponse = false;
        }
        closeCurrentReader();
    }

    private void installResponse(byte[] body) throws IOException {
        closeCurrentReader();
        // The cursor is per-response: a response that offers none is the
        // worker saying the stream is over, so it must not be inherited from
        // the previous turn.
        stateToken = null;
        dataSeenInCurrentResponse = false;
        currentReader = new IpcStreamReader(new ByteArrayInputStream(body), Allocators.root());
    }

    private void closeCurrentReader() {
        if (currentReader == null) return;
        try {
            currentReader.close();
        } catch (Exception ignore) {
            // Backed by a byte array; nothing that can fail meaningfully.
        }
        currentReader = null;
    }

    private void captureTokens(Map<String, String> md) {
        String state = md.get(Metadata.STREAM_STATE);
        if (state != null) stateToken = state;
        String call = md.get(Metadata.CALL_STATE);
        if (call != null) callToken = call;
    }

    private static Map<String, String> stripTokens(Map<String, String> md) {
        Map<String, String> out = new LinkedHashMap<>(md);
        out.remove(Metadata.STREAM_STATE);
        out.remove(Metadata.CALL_STATE);
        return out;
    }

    /** Request metadata for a continuation: the cursor, the call token, and optionally cancel. */
    private Map<String, String> tokenMetadata(boolean cancel) {
        Map<String, String> md = new LinkedHashMap<>();
        md.put(Metadata.STREAM_STATE, stateToken);
        if (callToken != null) md.put(Metadata.CALL_STATE, callToken);
        if (cancel) md.put(Metadata.CANCEL, "1");
        return md;
    }

    /** A producer tick (or a cancel): a zero-row batch of the empty schema carrying the tokens. */
    private byte[] continuationBody(boolean cancel, Map<String, String> customMetadata) throws IOException {
        Map<String, String> md = new LinkedHashMap<>();
        if (customMetadata != null) md.putAll(customMetadata);
        md.putAll(tokenMetadata(cancel));
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        try (IpcStreamWriter w = new IpcStreamWriter(buf)) {
            w.writeSchema(RpcStream.EMPTY_SCHEMA);
            Wire.writeZeroBatch(w, RpcStream.EMPTY_SCHEMA, md);
        }
        return buf.toByteArray();
    }

    /** An exchange turn: the caller's batch, with the tokens layered onto its metadata. */
    private byte[] exchangeBody(AnnotatedBatch input) throws IOException {
        Map<String, String> md = new LinkedHashMap<>(input.customMetadata());
        md.putAll(tokenMetadata(false));
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        try (IpcStreamWriter w = new IpcStreamWriter(buf)) {
            w.writeSchema(input.root().getSchema());
            w.writeBatch(input.root(), md, input.dictionaryProvider());
        }
        return buf.toByteArray();
    }
}
