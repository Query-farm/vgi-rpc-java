// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

import farm.query.vgirpc.external.LocationResolver;
import farm.query.vgirpc.log.Message;
import farm.query.vgirpc.marshal.Marshalling;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.transport.RpcTransport;
import farm.query.vgirpc.wire.Allocators;
import farm.query.vgirpc.wire.IpcStreamReader;
import farm.query.vgirpc.wire.IpcStreamWriter;
import farm.query.vgirpc.wire.Metadata;
import farm.query.vgirpc.wire.Wire;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Consumer;

/**
 * Client-side streaming session. Lazily opens the input and output IPC
 * streams on first use. Not thread-safe.
 *
 * @param <S> the server-side {@link StreamState} type declared by the service
 *     interface; purely a compile-time marker here — {@link #state()} always
 *     throws on the client
 */
public final class ClientStreamSession<S extends StreamState> extends RpcStream<S> {

    private final RpcTransport transport;
    private final Schema inputSchema;
    private final Schema outputSchema;
    private final ArrowSerializableRecord header;
    private final Consumer<Message> onLog;
    /**
     * Resolves external-location pointer batches on the output stream, or
     * {@code null} when the connection was built without an
     * {@link farm.query.vgirpc.external.ExternalLocationConfig}. Resolution is
     * opt-in because it costs a fetch against storage the client must be able
     * to reach; a stream that never sees a pointer is unaffected either way.
     */
    private final LocationResolver locationResolver;

    private IpcStreamWriter inputWriter;
    /**
     * The schema the input IPC stream actually carries, learned from the first
     * thing written to it.
     *
     * <p>It is not always {@link #inputSchema}. {@link IpcStreamWriter} defers
     * the schema message and, when the first write is a real batch, emits
     * <em>that batch's</em> schema — and on a client session {@code inputSchema}
     * is always {@link RpcStream#EMPTY_SCHEMA}, because the proxy has no way to
     * know an exchange's input schema (see {@code RpcConnection.doStream}); the
     * caller supplies it one batch at a time. Every zero-row control batch
     * written afterwards — the {@code vgi_rpc.cancel} token — has to match what
     * the stream declared, or the peer's {@code VectorLoader} rejects it
     * ("no more field nodes for field …") and the control signal is lost.
     */
    private Schema inputWireSchema;
    private IpcStreamReader outputReader;
    private boolean closed;
    /**
     * Set once the server's output IPC stream has signalled EOS. Reading past
     * that point blocks forever on a persistent transport (subprocess / pipe /
     * socket): the server has already moved on to awaiting the next request, so
     * no further output bytes are coming.
     */
    private boolean outputExhausted;
    /**
     * The root handed out for the most recent <em>resolved</em> pointer batch.
     * Unlike an inline batch — whose root belongs to {@link IpcStreamReader} and
     * is recycled on the next read — a resolved root is freshly allocated by
     * {@link LocationResolver#resolve}, so somebody has to free it. Keeping it
     * here and closing it on the next read (or at {@link #close()}) makes a
     * resolved batch obey exactly the same caller contract as an inline one
     * ("valid until the next call — copy what you must keep"), instead of
     * making the caller remember which kind of batch it just received.
     */
    private VectorSchemaRoot resolvedRoot;

    /**
     * Create a client streaming session with no external-location resolution.
     * The input/output streams are opened lazily on the first {@link #tick()},
     * {@link #exchange(AnnotatedBatch)}, or {@link #close()}.
     *
     * @param transport the underlying transport
     * @param inputSchema schema of input batches; {@code null} becomes {@link RpcStream#EMPTY_SCHEMA}
     * @param outputSchema schema of output batches; {@code null} becomes {@link RpcStream#EMPTY_SCHEMA}
     * @param header optional stream header record returned by the server, or {@code null}
     * @param onLog sink for log batches received on the output stream; may be {@code null}
     */
    public ClientStreamSession(RpcTransport transport, Schema inputSchema, Schema outputSchema,
                               ArrowSerializableRecord header, Consumer<Message> onLog) {
        this(transport, inputSchema, outputSchema, header, onLog, null);
    }

    /**
     * Create a client streaming session that transparently resolves
     * external-location pointer batches on the output stream.
     *
     * @param transport the underlying transport
     * @param inputSchema schema of input batches; {@code null} becomes {@link RpcStream#EMPTY_SCHEMA}
     * @param outputSchema schema of output batches; {@code null} becomes {@link RpcStream#EMPTY_SCHEMA}
     * @param header optional stream header record returned by the server, or {@code null}
     * @param onLog sink for log batches received on the output stream; may be {@code null}
     * @param locationResolver resolver for {@code vgi_rpc.location} pointer batches, or
     *     {@code null} to leave resolution disabled — in which case a pointer batch is
     *     reported as an error rather than silently dropped
     */
    public ClientStreamSession(RpcTransport transport, Schema inputSchema, Schema outputSchema,
                               ArrowSerializableRecord header, Consumer<Message> onLog,
                               LocationResolver locationResolver) {
        this.transport = transport;
        this.inputSchema = inputSchema != null ? inputSchema : RpcStream.EMPTY_SCHEMA;
        this.outputSchema = outputSchema != null ? outputSchema : RpcStream.EMPTY_SCHEMA;
        this.header = header;
        this.onLog = onLog != null ? onLog : m -> {};
        this.locationResolver = locationResolver;
    }

    @Override public Schema outputSchema() { return outputSchema; }
    @Override public Schema inputSchema() { return inputSchema; }
    @Override public S state() { throw new UnsupportedOperationException("state() not available on client session"); }
    @Override public ArrowSerializableRecord header() { return header; }

    /**
     * Send a tick (a zero-row batch matching the input schema) and return the
     * next data batch from the producer. The returned batch's root is owned by
     * the reader and is reused on the next call — copy any data you must keep.
     *
     * @return the next output {@link AnnotatedBatch}
     * @throws NoSuchElementException when the producer has finished (output stream closed)
     * @throws RpcError on transport failure or if the server reported an error
     */
    @Override
    public AnnotatedBatch tick() {
        ensureNotClosed();
        try {
            writeTickOrBatch(null, null);
            return readNextDataBatch();
        } catch (NoSuchElementException e) {
            close();
            throw e;
        } catch (IOException e) {
            closed = true;
            throw new RpcError("TransportError", "stream tick failed: " + e.getMessage(), "");
        }
    }

    /**
     * Send an input batch and read the next output data batch (exchange streams).
     * The returned batch's root is owned by the reader and reused on the next
     * call — copy any data you must keep.
     *
     * @param input the input batch to send for this tick
     * @return the next output {@link AnnotatedBatch}
     * @throws NoSuchElementException if the server ended the stream instead of
     *     answering this input batch
     * @throws RpcError on transport failure or if the server reported an error
     */
    @Override
    public AnnotatedBatch exchange(AnnotatedBatch input) {
        ensureNotClosed();
        try {
            writeTickOrBatch(input.root(), input.customMetadata());
            return readNextDataBatch();
        } catch (NoSuchElementException e) {
            // Same close-on-end-of-stream contract as tick(). An exchange
            // normally never sees EOS — the server answers every input batch —
            // so reaching it means the server has torn the stream down and gone
            // back to awaiting the next request. Leaving the session open would
            // let the caller write another input batch into a transport where
            // nobody is reading stream input, which corrupts every later call on
            // the connection rather than just failing this one.
            close();
            throw e;
        } catch (IOException e) {
            closed = true;
            throw new RpcError("TransportError", "stream exchange failed: " + e.getMessage(), "");
        }
    }

    /**
     * Signal end-of-stream to the server by closing the input stream (writing
     * EOS), then drain any remaining output so the transport is left clean for
     * the next call. Idempotent.
     */
    @Override
    public void close() {
        if (closed) return;
        closed = true;
        try {
            if (inputWriter == null) {
                // Open an empty-schema input stream just so we can write EOS
                inputWriter = new IpcStreamWriter(transport.writer());
                inputWriter.writeSchema(EMPTY_SCHEMA);
            }
            inputWriter.close();
            transport.writer().flush();
        } catch (Exception ignore) {}
        drainOutput();
    }

    /**
     * Abort the stream early: write a zero-row batch carrying the
     * {@code vgi_rpc.cancel} metadata flag, then {@link #close()}. The server
     * stops producing and the session is closed.
     */
    @Override
    public void cancel() {
        if (closed) return;
        try {
            // Goes through writeTickOrBatch so the token batch is built from the
            // schema the input stream actually declared — which, once an
            // exchange has sent its first real batch, is the caller's schema and
            // not inputSchema. See the inputWireSchema field.
            writeTickOrBatch(null, Map.of(Metadata.CANCEL, "1"));
        } catch (Exception ignore) {}
        close();
    }

    // ------------------------------------------------------------------

    private void ensureNotClosed() {
        if (closed) throw new RpcError("ProtocolError", "RpcStream has been closed or cancelled", "");
    }

    private void writeTickOrBatch(VectorSchemaRoot batch, Map<String, String> meta) throws IOException {
        if (inputWriter == null) {
            inputWriter = new IpcStreamWriter(transport.writer());
            inputWriter.writeSchema(inputSchema);
        }
        if (batch == null) {
            // Zero-row tick / control batch. It must match whatever schema the
            // stream already declared (see inputWireSchema), which is only
            // inputSchema while nothing has been written yet.
            if (inputWireSchema == null) inputWireSchema = inputSchema;
            Wire.writeZeroBatch(inputWriter, inputWireSchema, meta);
        } else {
            // The writer emits this batch's schema if it is the first one, so
            // that — not inputSchema — is what the stream carries from here on.
            if (inputWireSchema == null) inputWireSchema = batch.getSchema();
            inputWriter.writeBatch(batch, meta);
        }
        transport.writer().flush();
    }

    private AnnotatedBatch readNextDataBatch() throws IOException {
        if (outputReader == null) {
            outputReader = new IpcStreamReader(transport.reader(), Allocators.root());
        }
        releaseResolvedRoot();
        while (true) {
            Map<String, String> md = outputReader.readNextBatch();
            if (md == null) {
                // Server closed output stream without sending data — producer finished
                outputExhausted = true;
                throw new NoSuchElementException();
            }
            VectorSchemaRoot root = outputReader.root();
            Wire.BatchKind kind = Wire.classify(root.getRowCount(), md);
            if (kind == Wire.BatchKind.LOG) {
                onLog.accept(Wire.messageFromMetadata(md));
                continue;
            }
            if (kind == Wire.BatchKind.ERROR) {
                // An error batch is terminal: RpcServer stops its tick loop and
                // writes EOS right behind it. Close before raising so the
                // trailing EOS is consumed and the session is marked spent —
                // otherwise a caller that retries after catching the error
                // writes into a transport that has moved on to awaiting the next
                // request, and the connection is unusable from then on (the
                // failure then surfaces on some later, unrelated call).
                RpcError error = Wire.errorFromMetadata(md);
                close();
                throw error;
            }
            // Transparent resolution of external-location pointer batches, the
            // streaming counterpart of the unary path in RpcConnection.doUnary.
            //
            // A server externalises a batch that is too large to send inline by
            // replacing it with a ZERO-ROW batch carrying vgi_rpc.location (see
            // RpcServer.flushCollector -> Externalizer). Wire.classify only knows
            // about log/error metadata, so such a batch classifies as DATA: without
            // this branch the caller is handed an *empty* batch and the stream
            // carries on as if nothing happened — silent row loss, on the very
            // path (large results) where externalisation is used at all.
            if (LocationResolver.isPointer(root.getRowCount(), md)) {
                return resolvePointerBatch(md);
            }
            // Note: the AnnotatedBatch here does NOT own the root — it's reused by the reader.
            // Caller must consume / copy data before the next read.
            return new AnnotatedBatch(root, md);
        }
    }

    /**
     * Fetch and decode an external-location pointer batch, returning the real
     * batch in its place.
     *
     * <p>An unresolvable pointer is a hard failure, never an empty batch. The
     * client has been told, explicitly, that rows exist somewhere it cannot
     * reach; continuing would hand the caller a short result that looks
     * complete. Losing rows silently is strictly worse than failing the stream,
     * so both "resolution not configured" and "resolution failed" raise
     * {@link RpcError} with error type {@code ExternalLocationError} — matching
     * what {@code doUnary} raises when a fetch fails, so a caller can handle
     * both call shapes with one catch.
     *
     * <p>The metadata carried downstream is the resolver's merged view
     * ({@link LocationResolver.Resolved#customMetadata()}): the pointer's own
     * metadata with the {@code vgi_rpc.location*} keys stripped, overlaid with
     * the metadata the externalised batch was written with. That is the
     * metadata the batch would have had if it had travelled inline, which is
     * the whole point of the resolution being transparent.
     */
    private AnnotatedBatch resolvePointerBatch(Map<String, String> pointerMeta) {
        String url = pointerMeta.get(Metadata.LOCATION);
        if (locationResolver == null) {
            throw new RpcError("ExternalLocationError",
                    "stream produced an externalized batch (" + Metadata.LOCATION + "=" + url
                            + ") but this connection has no ExternalLocationConfig; "
                            + "construct RpcConnection with one to resolve it", "");
        }
        LocationResolver.Resolved resolved;
        try {
            resolved = locationResolver.resolve(pointerMeta);
        } catch (Exception fe) {
            throw new RpcError("ExternalLocationError",
                    "failed to resolve " + url + ": " + fe.getMessage(), "");
        }
        // Held for release on the next read / close — see the resolvedRoot field.
        resolvedRoot = resolved.root();
        return new AnnotatedBatch(resolvedRoot, resolved.customMetadata());
    }

    /** Free the previous resolved batch's caller-facing root, if any. */
    private void releaseResolvedRoot() {
        if (resolvedRoot == null) return;
        try { resolvedRoot.close(); } catch (Exception ignore) { /* best-effort */ }
        resolvedRoot = null;
    }

    private void drainOutput() {
        releaseResolvedRoot();
        // Already at EOS: the stream is spent and the server is waiting on the
        // next request. Reading again would block until the transport dies.
        if (outputExhausted) return;
        if (outputReader == null) {
            try {
                outputReader = new IpcStreamReader(transport.reader(), Allocators.root());
            } catch (Exception ignore) { return; }
        }
        for (int i = 0; i < 10_000; i++) {
            try {
                Map<String, String> md = outputReader.readNextBatch();
                if (md == null) { outputExhausted = true; break; }
                Wire.BatchKind kind = Wire.classify(outputReader.root().getRowCount(), md);
                if (kind == Wire.BatchKind.LOG) onLog.accept(Wire.messageFromMetadata(md));
                if (kind == Wire.BatchKind.ERROR) break;
                // Deliberately NOT resolving pointer batches here. Draining exists
                // to walk the output stream to its EOS marker so the transport is
                // clean for the next call; every batch it reads is discarded. A
                // pointer's payload lives in external storage, not on this stream,
                // so skipping the fetch drops exactly the same rows the inline
                // batch beside it is already dropping — while saving a network
                // round-trip (and egress) per abandoned batch on close()/cancel().
            } catch (Exception e) { break; }
        }
    }
}
