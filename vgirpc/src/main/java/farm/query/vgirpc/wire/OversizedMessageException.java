// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.wire;

import java.io.IOException;

/**
 * A message whose body this runtime cannot hold, refused without desyncing the
 * stream.
 *
 * The JVM caps a {@code byte[]} — and an {@link org.apache.arrow.memory.ArrowBuf}
 * index — at {@code INT_MAX} elements, so a peer that sends {@code 2**31 + 1}
 * bytes cannot be served at any heap size or allocator limit. That is a property
 * of the language, not a fault to be tuned away, and the cross-language
 * conformance suite treats a typed refusal as a conforming answer.
 *
 * What it does not tolerate is a refusal that wedges the connection. The body
 * bytes the header promised are drained before this is thrown, so the next
 * message starts on a frame boundary and the caller can answer this as an error
 * for one call and keep serving.
 */
public final class OversizedMessageException extends IOException {

    private static final long serialVersionUID = 1L;

    private final long bodyLength;

    /**
     * @param bodyLength the declared body length, in bytes, that could not be held
     * @param cause the allocation failure that prompted the refusal
     */
    public OversizedMessageException(long bodyLength, Throwable cause) {
        super("message body of " + bodyLength
                + " bytes exceeds what this runtime can allocate; refused and drained", cause);
        this.bodyLength = bodyLength;
    }

    /** @return the declared body length, in bytes, that could not be held. */
    public long bodyLength() { return bodyLength; }
}
