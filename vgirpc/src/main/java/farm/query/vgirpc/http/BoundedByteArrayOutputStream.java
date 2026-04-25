// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import java.io.ByteArrayOutputStream;

/**
 * {@link ByteArrayOutputStream} that throws {@link PayloadTooLargeException}
 * the moment a write would push the buffered size past {@code limit}. Used by
 * the HTTP transport to enforce request/response size caps without first
 * letting the JVM heap balloon under attack.
 */
final class BoundedByteArrayOutputStream extends ByteArrayOutputStream {

    private final long limit;

    BoundedByteArrayOutputStream(long limit) {
        super(Math.toIntExact(Math.min(limit, 8192)));
        this.limit = limit;
    }

    @Override
    public synchronized void write(int b) {
        ensureCapacity(1);
        super.write(b);
    }

    @Override
    public synchronized void write(byte[] b, int off, int len) {
        ensureCapacity(len);
        super.write(b, off, len);
    }

    private void ensureCapacity(int incoming) {
        if ((long) count + incoming > limit) {
            throw new PayloadTooLargeException("output exceeds " + limit
                    + " bytes; large batches must use the external-location protocol");
        }
    }
}
