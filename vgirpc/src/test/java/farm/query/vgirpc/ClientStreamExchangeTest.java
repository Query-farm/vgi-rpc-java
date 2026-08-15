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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exchange streams driven by the {@link RpcConnection#proxy} client over a
 * <em>persistent</em> transport (pipe / subprocess / socket).
 *
 * <p>{@link ClientStreamHeaderTest} covers the producer half — client ticks,
 * server pushes. This is the other half: the client sends an input batch and
 * the server answers one output batch per turn. The two halves share
 * {@code readNextDataBatch}, so the output-side fixes made there (the
 * {@code @StreamHeader} read, the {@code outputExhausted} latch,
 * {@code vgi_rpc.location} resolution) are inherited — what is <em>not</em>
 * shared is the input side, which only an exchange exercises: the client writes
 * a real schema onto the input IPC stream, where a producer only ever writes
 * zero-row ticks of the empty schema.
 *
 * <p>That difference is what {@link #cancelReachesTheWorkerMidExchange()}
 * pins. {@code ClientStreamSession} is constructed with
 * {@link RpcStream#EMPTY_SCHEMA} as its input schema (the client learns the
 * real one from the caller's batches, not from the server), so a zero-row
 * control batch built from that field — the {@code vgi_rpc.cancel} token — did
 * not match the schema the input stream had already declared, and the server
 * failed to decode it instead of running {@code onCancel}.
 */
final class ClientStreamExchangeTest {

    static final Schema IN_SCHEMA = new Schema(List.of(
            new Field("v", FieldType.nullable(new ArrowType.Int(64, true)), null)));
    static final Schema OUT_SCHEMA = new Schema(List.of(
            new Field("doubled", FieldType.nullable(new ArrowType.Int(64, true)), null)));

    /** Input value that makes the server-side state throw, for the error path. */
    private static final long POISON = -777L;

    /** Fired by {@link Doubler#onCancel}; one per {@link Harness}. */
    private static final CountDownLatch CANCELLED = new CountDownLatch(1);

    /** Stream header, so the exchange path also exercises header consumption. */
    public record Head(String tag) implements ArrowSerializableRecord {}

    /** Doubles every value in the input batch; echoes the input's tag metadata back. */
    public static final class Doubler extends ExchangeState {
        public long turns;

        public Doubler() {}

        @Override
        public void exchange(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
            VectorSchemaRoot in = input.root();
            BigIntVector src = (BigIntVector) in.getVector("v");
            int rows = in.getRowCount();
            VectorSchemaRoot root = VectorSchemaRoot.create(OUT_SCHEMA, Allocators.root());
            root.allocateNew();
            BigIntVector dst = (BigIntVector) root.getVector("doubled");
            for (int i = 0; i < rows; i++) {
                if (src.get(i) == POISON) {
                    root.close();
                    throw new IllegalStateException("poisoned input");
                }
                dst.setSafe(i, src.get(i) * 2);
            }
            dst.setValueCount(rows);
            root.setRowCount(rows);
            turns++;
            Map<String, String> md = new LinkedHashMap<>();
            md.put("x.turn", Long.toString(turns));
            // Round-trips the caller's per-batch input metadata, so the test can
            // prove exchange() puts it on the wire (a producer tick has none).
            md.put("x.echo", String.valueOf(input.customMetadata().get("x.tag")));
            out.emit(root, md);
        }

        @Override public void onCancel(CallContext ctx) { CANCELLED.countDown(); }
    }

    public interface DoublingService {
        @StreamHeader(Head.class)
        RpcStream<Doubler> doubling();

        long ping(long v);
    }

    public static final class Impl implements DoublingService {
        @Override public RpcStream<Doubler> doubling() {
            return RpcStream.exchange(IN_SCHEMA, OUT_SCHEMA, new Doubler(), new Head("doubling"));
        }

        @Override public long ping(long v) { return v; }
    }

    // ------------------------------------------------------------------

    @Test
    @Timeout(30)
    void answersEveryExchangeTurnOnOneStream() throws Exception {
        try (Harness h = Harness.start()) {
            RpcStream<Doubler> stream = h.client().doubling();
            assertEquals("doubling", ((Head) stream.header()).tag());

            ClientStreamSession<?> session = (ClientStreamSession<?>) stream;
            try {
                assertEquals(List.of(2L, 4L, 6L), send(session, 1, 2, 3));
                assertEquals(List.of(20L), send(session, 10));
                // A zero-row input batch is data with no rows, not end-of-stream —
                // the turns after it are what prove the stream stayed in step.
                assertEquals(List.of(), send(session));
                assertEquals(List.of(-2L), send(session, -1));
            } finally {
                session.close();
            }
        }
    }

    @Test
    @Timeout(30)
    void carriesPerBatchMetadataInBothDirections() throws Exception {
        try (Harness h = Harness.start()) {
            ClientStreamSession<?> session = (ClientStreamSession<?>) h.client().doubling();
            try (VectorSchemaRoot input = inputBatch(5)) {
                AnnotatedBatch out = session.exchange(
                        new AnnotatedBatch(input, Map.of("x.tag", "first")));
                assertEquals("1", out.customMetadata().get("x.turn"),
                        "server-side per-batch metadata must reach the client");
                assertEquals("first", out.customMetadata().get("x.echo"),
                        "client-side per-batch metadata must reach the server");
            } finally {
                session.close();
            }
        }
    }

    @Test
    @Timeout(30)
    void reusesTheConnectionAcrossExchangesAndUnaryCalls() throws Exception {
        try (Harness h = Harness.start()) {
            DoublingService svc = h.client();

            ClientStreamSession<?> first = (ClientStreamSession<?>) svc.doubling();
            assertEquals(List.of(2L, 4L), send(first, 1, 2));
            first.close();

            assertEquals(42L, svc.ping(42L), "unary call after an exchange must still work");

            ClientStreamSession<?> second = (ClientStreamSession<?>) svc.doubling();
            assertEquals(List.of(6L), send(second, 3));
            assertEquals(List.of(8L), send(second, 4));
            second.close();
            second.close();  // idempotent, and must not read the spent stream

            assertEquals(7L, svc.ping(7L));
        }
    }

    /**
     * A worker that raises mid-exchange must surface as a raised
     * {@link RpcError} on the turn that caused it — not a hang, and not a
     * silently empty batch — and must leave the connection usable.
     *
     * <p>The retry is the load-bearing part. An error batch is terminal: the
     * server stops its tick loop and writes EOS behind it, so a caller that
     * catches the error and tries another turn used to write an input batch
     * into a transport already back to awaiting the next <em>request</em>. The
     * turn itself failed with a bare {@link java.util.NoSuchElementException},
     * and the damage surfaced later, on the next unrelated call — here, the
     * {@code ping}.
     */
    @Test
    @Timeout(30)
    void propagatesAWorkerErrorMidExchange() throws Exception {
        try (Harness h = Harness.start()) {
            DoublingService svc = h.client();
            ClientStreamSession<?> session = (ClientStreamSession<?>) svc.doubling();
            assertEquals(List.of(2L), send(session, 1));

            RpcError err = assertThrows(RpcError.class, () -> send(session, POISON));
            assertTrue(err.getMessage().contains("poisoned input"), err.getMessage());

            RpcError retry = assertThrows(RpcError.class, () -> send(session, 1),
                    "a spent stream must refuse further turns, not write into the transport");
            assertEquals("ProtocolError", retry.errorType());

            session.close();
            assertEquals(9L, svc.ping(9L), "connection must survive a failed exchange turn");
        }
    }

    /**
     * {@link RpcStream#cancel()} must deliver {@code vgi_rpc.cancel} to a
     * <em>started</em> exchange — the case where the input IPC stream has
     * already declared the caller's real schema. Cancelling before the first
     * batch always worked (the stream is still empty, so an empty-schema token
     * batch is consistent with it); cancelling after one is the regression.
     */
    @Test
    @Timeout(30)
    void cancelReachesTheWorkerMidExchange() throws Exception {
        try (Harness h = Harness.start()) {
            DoublingService svc = h.client();
            ClientStreamSession<?> session = (ClientStreamSession<?>) svc.doubling();
            assertEquals(List.of(2L, 4L), send(session, 1, 2));

            session.cancel();
            assertTrue(CANCELLED.await(10, TimeUnit.SECONDS),
                    "onCancel must fire for a cancel sent after the exchange has started");

            assertEquals(5L, svc.ping(5L), "connection must survive a cancel");
        }
    }

    // ------------------------------------------------------------------

    /** One exchange turn: send {@code values} as the {@code v} column, decode the answer. */
    private static List<Long> send(ClientStreamSession<?> session, long... values) {
        try (VectorSchemaRoot input = inputBatch(values)) {
            AnnotatedBatch out = session.exchange(new AnnotatedBatch(input, null));
            VectorSchemaRoot root = out.root();
            BigIntVector doubled = (BigIntVector) root.getVector("doubled");
            List<Long> decoded = new ArrayList<>(root.getRowCount());
            for (int i = 0; i < root.getRowCount(); i++) decoded.add(doubled.get(i));
            return decoded;
        }
    }

    private static VectorSchemaRoot inputBatch(long... values) {
        VectorSchemaRoot root = VectorSchemaRoot.create(IN_SCHEMA, Allocators.root());
        root.allocateNew();
        BigIntVector v = (BigIntVector) root.getVector("v");
        for (int i = 0; i < values.length; i++) v.setSafe(i, values[i]);
        v.setValueCount(values.length);
        root.setRowCount(values.length);
        return root;
    }

    /** Server on a daemon thread, client on the test thread, joined by a pipe pair. */
    private static final class Harness implements AutoCloseable {
        private final RpcConnection connection;
        private final DoublingService proxy;
        private final RpcTransport clientTransport;
        private final Thread serverThread;

        private Harness(RpcConnection connection, DoublingService proxy,
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

            RpcServer server = new RpcServer(DoublingService.class, new Impl());
            Thread thread = new Thread(() -> server.serve(serverTransport), "rpc-server");
            thread.setDaemon(true);
            thread.start();

            RpcConnection connection = new RpcConnection(clientTransport);
            return new Harness(connection, connection.proxy(DoublingService.class),
                    clientTransport, thread);
        }

        DoublingService client() { return proxy; }

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
