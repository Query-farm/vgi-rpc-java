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

    private IpcStreamWriter inputWriter;
    private IpcStreamReader outputReader;
    private boolean closed;

    /**
     * Create a client streaming session. The input/output streams are opened
     * lazily on the first {@link #tick()}, {@link #exchange(AnnotatedBatch)},
     * or {@link #close()}.
     *
     * @param transport the underlying transport
     * @param inputSchema schema of input batches; {@code null} becomes {@link RpcStream#EMPTY_SCHEMA}
     * @param outputSchema schema of output batches; {@code null} becomes {@link RpcStream#EMPTY_SCHEMA}
     * @param header optional stream header record returned by the server, or {@code null}
     * @param onLog sink for log batches received on the output stream; may be {@code null}
     */
    public ClientStreamSession(RpcTransport transport, Schema inputSchema, Schema outputSchema,
                               ArrowSerializableRecord header, Consumer<Message> onLog) {
        this.transport = transport;
        this.inputSchema = inputSchema != null ? inputSchema : RpcStream.EMPTY_SCHEMA;
        this.outputSchema = outputSchema != null ? outputSchema : RpcStream.EMPTY_SCHEMA;
        this.header = header;
        this.onLog = onLog != null ? onLog : m -> {};
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
     * @throws RpcError on transport failure or if the server reported an error
     */
    @Override
    public AnnotatedBatch exchange(AnnotatedBatch input) {
        ensureNotClosed();
        try {
            writeTickOrBatch(input.root(), input.customMetadata());
            return readNextDataBatch();
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
            Map<String, String> cancel = Map.of(Metadata.CANCEL, "1");
            if (inputWriter == null) {
                inputWriter = new IpcStreamWriter(transport.writer());
                inputWriter.writeSchema(EMPTY_SCHEMA);
            }
            Wire.writeZeroBatch(inputWriter, inputSchema, cancel);
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
            // Emit a zero-row tick matching the input schema
            Wire.writeZeroBatch(inputWriter, inputSchema, meta);
        } else {
            inputWriter.writeBatch(batch, meta);
        }
        transport.writer().flush();
    }

    private AnnotatedBatch readNextDataBatch() throws IOException {
        if (outputReader == null) {
            outputReader = new IpcStreamReader(transport.reader(), Allocators.root());
        }
        while (true) {
            Map<String, String> md = outputReader.readNextBatch();
            if (md == null) {
                // Server closed output stream without sending data — producer finished
                throw new NoSuchElementException();
            }
            VectorSchemaRoot root = outputReader.root();
            Wire.BatchKind kind = Wire.classify(root.getRowCount(), md);
            if (kind == Wire.BatchKind.LOG) {
                onLog.accept(Wire.messageFromMetadata(md));
                continue;
            }
            if (kind == Wire.BatchKind.ERROR) {
                throw Wire.errorFromMetadata(md);
            }
            // Note: the AnnotatedBatch here does NOT own the root — it's reused by the reader.
            // Caller must consume / copy data before the next read.
            return new AnnotatedBatch(root, md);
        }
    }

    private void drainOutput() {
        if (outputReader == null) {
            try {
                outputReader = new IpcStreamReader(transport.reader(), Allocators.root());
            } catch (Exception ignore) { return; }
        }
        for (int i = 0; i < 10_000; i++) {
            try {
                Map<String, String> md = outputReader.readNextBatch();
                if (md == null) break;
                Wire.BatchKind kind = Wire.classify(outputReader.root().getRowCount(), md);
                if (kind == Wire.BatchKind.LOG) onLog.accept(Wire.messageFromMetadata(md));
                if (kind == Wire.BatchKind.ERROR) break;
            } catch (Exception e) { break; }
        }
    }
}
