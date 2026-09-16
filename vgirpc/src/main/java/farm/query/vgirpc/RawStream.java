// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

import java.util.Map;

/**
 * An open streaming call driven without a service interface.
 *
 * <p>The untyped counterpart of {@link RpcStream}: batches cross this surface as
 * framed Arrow IPC streams ({@link farm.query.vgirpc.wire.Wire#writeOneBatch})
 * rather than as decoded Java values, and the method is named by a string rather
 * than by a {@code @Rpc} interface. That is what a client relaying somebody
 * else's batches needs — a gateway, a proxy, a bridge to a dynamically typed
 * caller — none of which can name a Java type for a payload they only forward.
 *
 * <p>Everything else is the typed client's behaviour, because it is the typed
 * client's code: request framing, the routing key, log relay, error surfacing,
 * external-location resolution, stream lockstep and (over HTTP) continuation
 * tokens all run exactly as they do behind {@link RpcConnection#proxy(Class)}.
 *
 * <p>Not thread-safe; one stream belongs to one caller.
 */
public interface RawStream extends AutoCloseable {

    /**
     * The stream's header payload, framed as a one-batch IPC stream.
     *
     * @return the header bytes, or {@code null} when the method declares none
     */
    byte[] header();

    /**
     * Advance a producer stream.
     *
     * @param customMetadata per-tick Arrow custom metadata to send upstream, or
     *     {@code null} for a bare tick
     * @return the next batch framed as a one-batch IPC stream, or {@code null}
     *     at end of stream
     * @throws RpcError on a transport failure or a peer-reported error
     */
    byte[] tick(Map<String, String> customMetadata);

    /**
     * Send one batch and return the peer's answer for it (exchange streams).
     *
     * @param input the batch to send, with its custom metadata
     * @return the reply framed as a one-batch IPC stream, or {@code null} when
     *     the peer ended the stream instead of answering
     * @throws RpcError on a transport failure or a peer-reported error
     */
    byte[] exchange(AnnotatedBatch input);

    /**
     * The opaque continuation token for the batch most recently returned.
     *
     * <p>{@code null} on every transport that carries no resumable stream state
     * — which is every byte-stream transport, where the stream <em>is</em> the
     * connection. Over HTTP it is the cursor a client presents to resume.
     *
     * @return the resume token, or {@code null}
     */
    String stateToken();

    /** Abort the stream, telling the peer, so it can release the work. */
    void cancel();

    /** Release the stream without cancelling it. Idempotent. */
    @Override
    void close();
}
