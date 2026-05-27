// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.shm;

import java.nio.channels.WritableByteChannel;

/**
 * A {@link WritableByteChannel} into a shared-memory region that reports the
 * exact number of bytes written, so the caller can record the true serialized
 * length of a pointer batch after Arrow's {@code WriteChannel} closes.
 */
public interface ShmWriteChannel extends WritableByteChannel {
    /** Bytes written so far (the actual serialized length). */
    long written();
}
