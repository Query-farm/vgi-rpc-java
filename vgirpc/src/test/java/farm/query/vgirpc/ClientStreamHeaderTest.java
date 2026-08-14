// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.schema.StreamHeader;
import farm.query.vgirpc.transport.RpcTransport;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Producer streams driven by the {@link RpcConnection#proxy} client over a
 * <em>persistent</em> transport (pipe / subprocess / socket).
 *
 * <p>Streaming had only ever been client-tested over HTTP, which reads the
 * header through {@code HttpStreamHandler} rather than {@code doStream}. That
 * left two defects in the persistent-transport client, both fixed here:
 *
 * <ol>
 *   <li>{@code resolveHeaderType} looked for the header type in a second
 *       {@code RpcStream} type argument. {@code RpcStream<S extends StreamState>}
 *       declares exactly one type parameter — which is precisely why
 *       {@link StreamHeader} exists — so the lookup never matched and the
 *       client skipped the header stream the server had written, then decoded
 *       the header batch as the stream's first data batch.</li>
 *   <li>{@code close()} unconditionally drained the output stream. After the
 *       producer signalled EOS the server has already gone back to awaiting the
 *       next request, so that read blocked until the transport died.</li>
 * </ol>
 */
final class ClientStreamHeaderTest {

    static final Schema OUT_SCHEMA = new Schema(List.of(
            new Field("n", FieldType.notNullable(new ArrowType.Int(64, true)), null)));

    /** Stream header — the thing a client must consume before the data stream. */
    public record Head(String tag, long total) implements ArrowSerializableRecord {}

    /** Emits {@code count} rows one per tick, then finishes. */
    public static final class CountingProducer extends ProducerState {
        public long remaining;
        public long next;

        public CountingProducer() {}

        CountingProducer(long count) { this.remaining = count; }

        @Override public void produce(OutputCollector out, CallContext ctx) {
            if (remaining <= 0) {
                out.finish();
                return;
            }
            VectorSchemaRoot root = VectorSchemaRoot.create(OUT_SCHEMA, Allocators.root());
            root.allocateNew();
            ((BigIntVector) root.getVector(0)).setSafe(0, next);
            root.setRowCount(1);
            out.emit(root);
            next++;
            remaining--;
            if (remaining == 0) out.finish();
        }
    }

    public interface CountingService {
        @StreamHeader(Head.class)
        RpcStream<CountingProducer> count(long n);

        /** Control: a stream with no declared header. */
        RpcStream<CountingProducer> count_headerless(long n);

        long ping(long v);
    }

    public static final class Impl implements CountingService {
        @Override public RpcStream<CountingProducer> count(long n) {
            return RpcStream.producer(OUT_SCHEMA, new CountingProducer(n), new Head("counting", n));
        }

        @Override public RpcStream<CountingProducer> count_headerless(long n) {
            return RpcStream.producer(OUT_SCHEMA, new CountingProducer(n), null);
        }

        @Override public long ping(long v) { return v; }
    }

    @Test
    @Timeout(30)
    void deliversTheDeclaredStreamHeaderAndThenTheData() throws Exception {
        try (Harness h = Harness.start()) {
            CountingService svc = h.client();

            RpcStream<CountingProducer> stream = svc.count(3);

            Head head = (Head) stream.header();
            assertNotNull(head, "@StreamHeader must be read off the wire by the client");
            assertEquals("counting", head.tag());
            assertEquals(3L, head.total());

            // The header must NOT have been mis-delivered as a data batch.
            assertEquals(List.of(0L, 1L, 2L), drain(stream));
        }
    }

    @Test
    @Timeout(30)
    void closingAfterEndOfStreamDoesNotBlock() throws Exception {
        try (Harness h = Harness.start()) {
            RpcStream<CountingProducer> stream = h.client().count(1);
            assertEquals(List.of(0L), drain(stream));
            // Idempotent, and must not read the spent output stream again.
            stream.close();
            stream.close();
        }
    }

    @Test
    @Timeout(30)
    void reusesTheConnectionAcrossStreamsAndUnaryCalls() throws Exception {
        try (Harness h = Harness.start()) {
            CountingService svc = h.client();
            assertEquals(List.of(0L, 1L), drain(svc.count(2)));
            assertEquals(42L, svc.ping(42L), "unary call after a stream must still work");
            assertEquals(List.of(0L, 1L, 2L, 3L), drain(svc.count(4)));
            assertEquals(List.of(), drain(svc.count(0)));
            assertEquals(7L, svc.ping(7L));
        }
    }

    @Test
    @Timeout(30)
    void headerlessStreamsStillWork() throws Exception {
        try (Harness h = Harness.start()) {
            RpcStream<CountingProducer> stream = h.client().count_headerless(2);
            assertEquals(null, stream.header());
            assertEquals(List.of(0L, 1L), drain(stream));
        }
    }

    // ------------------------------------------------------------------

    private static List<Long> drain(RpcStream<?> stream) {
        List<Long> out = new ArrayList<>();
        ClientStreamSession<?> session = (ClientStreamSession<?>) stream;
        try {
            while (true) {
                AnnotatedBatch batch;
                try {
                    batch = session.tick();
                } catch (NoSuchElementException endOfStream) {
                    break;
                }
                VectorSchemaRoot root = batch.root();
                BigIntVector v = (BigIntVector) root.getVector("n");
                for (int i = 0; i < root.getRowCount(); i++) out.add(v.get(i));
            }
        } finally {
            session.close();
        }
        return out;
    }

    /** Server on a daemon thread, client on the test thread, joined by a pipe pair. */
    private static final class Harness implements AutoCloseable {
        private final RpcConnection connection;
        private final CountingService proxy;
        private final RpcTransport clientTransport;
        private final Thread serverThread;

        private Harness(RpcConnection connection, CountingService proxy,
                        RpcTransport clientTransport, Thread serverThread) {
            this.connection = connection;
            this.proxy = proxy;
            this.clientTransport = clientTransport;
            this.serverThread = serverThread;
        }

        static Harness start() throws Exception {
            PipedOutputStream clientOut = new PipedOutputStream();
            PipedInputStream serverIn = new PipedInputStream(clientOut, 1 << 16);
            PipedOutputStream serverOut = new PipedOutputStream();
            PipedInputStream clientIn = new PipedInputStream(serverOut, 1 << 16);

            RpcTransport serverTransport = new PipeTransport(serverIn, serverOut);
            RpcTransport clientTransport = new PipeTransport(clientIn, clientOut);

            RpcServer server = new RpcServer(CountingService.class, new Impl());
            Thread thread = new Thread(() -> server.serve(serverTransport), "rpc-server");
            thread.setDaemon(true);
            thread.start();

            RpcConnection connection = new RpcConnection(clientTransport);
            return new Harness(connection, connection.proxy(CountingService.class),
                    clientTransport, thread);
        }

        CountingService client() { return proxy; }

        @Override public void close() throws InterruptedException {
            connection.close();
            clientTransport.close();
            serverThread.join(2000);
        }
    }

    private static final class PipeTransport implements RpcTransport {
        private final InputStream in;
        private final OutputStream out;

        PipeTransport(InputStream in, OutputStream out) {
            this.in = in;
            this.out = out;
        }

        @Override public InputStream reader() { return in; }

        @Override public OutputStream writer() { return out; }

        @Override public void close() {
            try { out.flush(); } catch (Exception ignore) { /* best-effort */ }
            try { out.close(); } catch (Exception ignore) { /* best-effort */ }
            try { in.close(); } catch (Exception ignore) { /* best-effort */ }
        }
    }
}
