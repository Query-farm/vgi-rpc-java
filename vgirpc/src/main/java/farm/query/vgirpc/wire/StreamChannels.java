// Copyright 2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0
package farm.query.vgirpc.wire;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;

/** Stream adapters that do not hold a Java monitor across blocking I/O.
 * Channels.newChannel does so on Java 21, pinning virtual-thread carriers.
 * Idle RPC connections can then prevent every new connection from running.
 * RPC paths build an adapter per call, so buffers grow on demand as the JDK's do.
 */
final class StreamChannels {
    private static final int CHUNK_SIZE = 8192;

    private StreamChannels() { }

    static ReadableByteChannel readable(InputStream input) {
        Objects.requireNonNull(input);
        return new ReadableByteChannel() {
            private final ReentrantLock lock = new ReentrantLock();
            private byte[] scratch = new byte[0];
            private volatile boolean open = true;

            @Override
            public int read(ByteBuffer dst) throws IOException {
                lock.lock();
                try {
                    if (!open) throw new ClosedChannelException();
                    if (dst.isReadOnly()) throw new ReadOnlyBufferException();
                    if (!dst.hasRemaining()) return 0;
                    int wanted = Math.min(dst.remaining(), CHUNK_SIZE);
                    if (scratch.length < wanted) scratch = new byte[wanted];
                    int count = input.read(scratch, 0, wanted);
                    if (count > 0) dst.put(scratch, 0, count);
                    return count;
                } finally {
                    lock.unlock();
                }
            }

            @Override
            public boolean isOpen() { return open; }

            @Override
            public void close() throws IOException {
                open = false;
                // Do not take the I/O lock: closing the socket must unblock read.
                input.close();
            }
        };
    }

    static WritableByteChannel writable(OutputStream output) {
        Objects.requireNonNull(output);
        return new WritableByteChannel() {
            private final ReentrantLock lock = new ReentrantLock();
            private byte[] scratch = new byte[0];
            private volatile boolean open = true;

            @Override
            public int write(ByteBuffer src) throws IOException {
                lock.lock();
                try {
                    if (!open) throw new ClosedChannelException();
                    int total = src.remaining();
                    while (src.hasRemaining()) {
                        int count = Math.min(src.remaining(), CHUNK_SIZE);
                        if (scratch.length < count) scratch = new byte[count];
                        // Advance the caller's position only after a successful
                        // write; OutputStream.write promises the entire chunk.
                        src.get(src.position(), scratch, 0, count);
                        output.write(scratch, 0, count);
                        src.position(src.position() + count);
                    }
                    return total;
                } finally {
                    lock.unlock();
                }
            }

            @Override
            public boolean isOpen() { return open; }

            @Override
            public void close() throws IOException {
                open = false;
                output.close();
            }
        };
    }
}
