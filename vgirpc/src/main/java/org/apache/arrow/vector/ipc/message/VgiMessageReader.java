// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package org.apache.arrow.vector.ipc.message;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;

import farm.query.vgirpc.wire.OversizedMessageException;
import org.apache.arrow.flatbuf.KeyValue;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.ipc.ReadChannel;

/**
 * {@link MessageChannelReader} that captures each message's
 * {@code custom_metadata} and refuses an unholdable body without desyncing the
 * stream.
 *
 * <p>In this package deliberately: {@link MessageResult}'s constructor is
 * package-private, and re-implementing {@link #readNext()} is the only place the
 * body length is known at the moment the allocation fails. The same trick as
 * {@code org.apache.arrow.memory.VgiPooledAllocators}.
 *
 * <p>Stock {@code MessageChannelReader.readNext()} reads the header and then
 * allocates the body in one step. When that allocation throws — a body larger
 * than the allocator limit, or larger than {@code INT_MAX} and so unholdable at
 * any limit — the header is already consumed while the body bytes are still in
 * the channel. Propagating there leaves the stream mid-frame, and every later
 * read on that connection is garbage; in practice the peer blocks writing a body
 * nobody drains and the connection dies. Draining first is what turns that into
 * an error for one call.
 */
public final class VgiMessageReader extends MessageChannelReader {

    /** Drain buffer size. Big enough that a multi-GiB drain is not syscall-bound. */
    private static final int DRAIN_CHUNK = 1 << 23; // 8 MiB

    private Map<String, String> lastCustomMetadata = Map.of();

    /**
     * @param in the channel to read messages from
     * @param allocator allocator for message bodies
     */
    public VgiMessageReader(ReadChannel in, BufferAllocator allocator) {
        super(in, allocator);
    }

    /** @return {@code custom_metadata} of the most recently read message. */
    public Map<String, String> lastCustomMetadata() { return lastCustomMetadata; }

    @Override
    public MessageResult readNext() throws IOException {
        MessageMetadataResult metadata = MessageSerializer.readMessage(in);
        if (metadata == null) {
            return null; // clean end of stream
        }
        Message message = metadata.getMessage();

        if (!metadata.messageHasBody()) {
            lastCustomMetadata = captureMetadata(message);
            return new MessageResult(message, null);
        }

        long bodyLength = metadata.getMessageBodyLength();
        ArrowBuf body;
        try {
            body = MessageSerializer.readMessageBody(in, bodyLength, allocator);
        } catch (OutOfMemoryException | OutOfMemoryError e) {
            // The header is spent and the body is still queued. Consume it so the
            // next message starts on a frame boundary, then report a refusal the
            // caller can answer with for this call alone.
            drain(bodyLength);
            throw new OversizedMessageException(bodyLength, e);
        }
        try {
            // Decode caller-controlled metadata only after consuming the body.
            // If a key/value is invalid UTF-8, the caller can now drain the
            // stream's EOS marker and reuse the persistent connection instead
            // of being left in the middle of this record-batch frame.
            lastCustomMetadata = captureMetadata(message);
        } catch (RuntimeException malformedMetadata) {
            body.close();
            throw malformedMetadata;
        }
        return new MessageResult(message, body);
    }

    /** Read and discard exactly {@code remaining} bytes. */
    private void drain(long remaining) throws IOException {
        ByteBuffer chunk = ByteBuffer.allocate((int) Math.min(remaining, DRAIN_CHUNK));
        while (remaining > 0) {
            chunk.clear();
            if (remaining < chunk.capacity()) {
                chunk.limit((int) remaining);
            }
            int read = in.readFully(chunk);
            if (read <= 0) {
                // Peer stopped short of the length it declared. Nothing left to
                // resynchronise to, so let the caller see the stream as ended
                // rather than pretend the frame boundary was recovered.
                throw new IOException("stream ended with " + remaining
                        + " bytes of a declared message body unread");
            }
            remaining -= read;
        }
    }

    private static Map<String, String> captureMetadata(Message m) {
        int n = m.customMetadataLength();
        if (n == 0) {
            return Map.of();
        }
        Map<String, String> map = new LinkedHashMap<>(n);
        for (int i = 0; i < n; i++) {
            KeyValue kv = m.customMetadata(i);
            map.put(kv.key(), kv.value());
        }
        return map;
    }
}
