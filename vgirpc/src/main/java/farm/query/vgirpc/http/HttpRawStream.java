// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.RawStream;
import farm.query.vgirpc.RpcError;
import farm.query.vgirpc.StreamState;
import farm.query.vgirpc.wire.Wire;

import java.io.IOException;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * {@link RawStream} over an {@link HttpRpcStream}: the untyped view of a
 * streaming call, where batches cross the boundary as framed Arrow IPC rather
 * than as decoded Java values.
 *
 * <p>Everything about the wire — the continuation cursor, the call token, log
 * relay, error surfacing, external-location resolution, lockstep — is the typed
 * stream's code running unchanged underneath. The only thing added here is the
 * reframing, and the one piece of bookkeeping reframing makes possible.</p>
 *
 * <h2>Why a tick settles the cursor and an exchange does not</h2>
 *
 * <p>A producer response carries its cursor in a zero-row batch that
 * <em>trails</em> the data batch, so at the moment {@link HttpRpcStream#tick()}
 * hands back a batch, the cursor that resumes after it has not been read yet.
 * The typed stream cannot read on at that point — it would be recycling the
 * reader's root out from under a caller who has not looked at it. This wrapper
 * can, because it reframes the batch to bytes first, so it walks the response
 * out to the cursor before returning. That is what makes
 * {@link #stateToken()} answer for the batch just returned rather than for the
 * one before it, and it is the same order the reference client uses: buffer the
 * whole turn, commit its cursor, then yield.</p>
 *
 * <p>An exchange response piggy-backs its cursor on the data batch itself, so
 * there is nothing to walk out to and nothing to settle — reading on would only
 * risk turning a well-formed response into a lockstep complaint.</p>
 *
 * <p>Not thread-safe; one stream belongs to one caller.</p>
 */
final class HttpRawStream implements RawStream {

    private final HttpRpcStream<StreamState> stream;
    private final byte[] header;
    private final boolean exchange;

    /**
     * @param stream the typed stream doing the work
     * @param header the stream's header framed as a one-batch IPC stream, or
     *     {@code null} when the method declares none
     * @param exchange whether this is an exchange stream rather than a producer;
     *     supplied by the caller from the method's declaration, never inferred
     */
    HttpRawStream(HttpRpcStream<StreamState> stream, byte[] header, boolean exchange) {
        this.stream = stream;
        this.header = header;
        this.exchange = exchange;
    }

    @Override
    public byte[] header() { return header; }

    @Override
    public byte[] tick(Map<String, String> customMetadata) {
        AnnotatedBatch batch;
        try {
            batch = stream.tick(customMetadata);
        } catch (NoSuchElementException end) {
            return null;
        }
        byte[] framed = frame(batch);
        // Only now that the batch is bytes is it safe to read on for the cursor.
        if (!exchange) stream.settleCursor();
        return framed;
    }

    @Override
    public byte[] exchange(AnnotatedBatch input) {
        try {
            return frame(stream.exchange(input));
        } catch (NoSuchElementException end) {
            return null;
        }
    }

    @Override
    public String stateToken() { return stream.stateToken(); }

    @Override
    public void cancel() { stream.cancel(); }

    @Override
    public void close() { stream.close(); }

    private static byte[] frame(AnnotatedBatch batch) {
        try {
            return Wire.writeOneBatch(batch.root(), batch.customMetadata(), batch.dictionaryProvider());
        } catch (IOException e) {
            throw new RpcError("TransportError",
                    "could not reframe stream batch: " + e.getMessage(), "");
        }
    }
}
