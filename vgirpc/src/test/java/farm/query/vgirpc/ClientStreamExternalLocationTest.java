// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

import com.sun.net.httpserver.HttpServer;
import farm.query.vgirpc.external.ExternalLocationConfig;
import farm.query.vgirpc.external.ExternalStorage;
import farm.query.vgirpc.transport.RpcTransport;
import farm.query.vgirpc.wire.Allocators;
import farm.query.vgirpc.wire.Metadata;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Externalised batches on a <em>streaming</em> response.
 *
 * <p>A server that cannot (or will not) send a batch inline replaces it with a
 * zero-row pointer batch carrying {@code vgi_rpc.location}; the client is meant
 * to fetch the referenced IPC bytes and hand the caller the real batch. The
 * unary client has always done this ({@code RpcConnection.doUnary}); the
 * streaming client did not, and because {@code Wire.classify} only recognises
 * log/error metadata, a pointer batch classified as ordinary DATA. Callers were
 * handed the pointer's <em>empty</em> root and the stream carried on — silent
 * row loss on the one path (large results) where externalisation happens at
 * all.
 *
 * <p>This exercises the round trip end to end over a persistent (pipe)
 * transport, and pins the two decisions that go with it: batch metadata
 * survives resolution, and a pointer that <em>cannot</em> be resolved fails the
 * stream instead of degrading to an empty batch.
 */
final class ClientStreamExternalLocationTest {

    private static final Schema OUT_SCHEMA = new Schema(List.of(
            new Field("n", FieldType.notNullable(new ArrowType.Int(64, true)), null)));

    /** Metadata a producer stamps on its batch; must survive externalisation. */
    private static final String BATCH_TAG = "test.batch_tag";

    /** Rows per emitted batch — comfortably past the 16-byte externalisation threshold. */
    private static final int ROWS = 64;

    /** Emits {@code batches} batches of {@link #ROWS} rows, then finishes. */
    public static final class BulkProducer extends ProducerState {
        public long remaining;
        public long next;

        public BulkProducer() {}

        BulkProducer(long batches) { this.remaining = batches; }

        @Override public void produce(OutputCollector out, CallContext ctx) {
            if (remaining <= 0) {
                out.finish();
                return;
            }
            VectorSchemaRoot root = VectorSchemaRoot.create(OUT_SCHEMA, Allocators.root());
            root.allocateNew();
            BigIntVector v = (BigIntVector) root.getVector(0);
            for (int i = 0; i < ROWS; i++) v.setSafe(i, next++);
            root.setRowCount(ROWS);
            out.emit(root, Map.of(BATCH_TAG, "batch-" + remaining));
            remaining--;
            if (remaining == 0) out.finish();
        }
    }

    /**
     * Exchange counterpart of {@link BulkProducer}: answers each input batch
     * with the same rows doubled, big enough to be externalised.
     */
    public static final class Doubler extends ExchangeState {
        public Doubler() {}

        @Override public void exchange(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
            BigIntVector src = (BigIntVector) input.root().getVector("n");
            int rows = input.root().getRowCount();
            VectorSchemaRoot root = VectorSchemaRoot.create(OUT_SCHEMA, Allocators.root());
            root.allocateNew();
            BigIntVector v = (BigIntVector) root.getVector(0);
            for (int i = 0; i < rows; i++) v.setSafe(i, src.get(i) * 2);
            v.setValueCount(rows);
            root.setRowCount(rows);
            out.emit(root, Map.of(BATCH_TAG, "exchanged"));
        }
    }

    public interface BulkService {
        RpcStream<BulkProducer> bulk(long batches);

        RpcStream<Doubler> doubling();
    }

    public static final class Impl implements BulkService {
        @Override public RpcStream<BulkProducer> bulk(long batches) {
            return RpcStream.producer(OUT_SCHEMA, new BulkProducer(batches));
        }

        @Override public RpcStream<Doubler> doubling() {
            return RpcStream.exchange(OUT_SCHEMA, OUT_SCHEMA, new Doubler());
        }
    }

    // ------------------------------------------------------------------

    private HttpServer storageHttp;
    private int storagePort;
    private final Map<String, byte[]> objects = new ConcurrentHashMap<>();

    @BeforeEach
    void startStorage() throws Exception {
        storageHttp = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        storageHttp.createContext("/obj/", exchange -> {
            String key = exchange.getRequestURI().getPath().substring("/obj/".length());
            byte[] body = objects.get(key);
            if (body == null) { exchange.sendResponseHeaders(404, -1); exchange.close(); return; }
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.getResponseBody().close();
        });
        storageHttp.start();
        storagePort = storageHttp.getAddress().getPort();
    }

    @AfterEach
    void stopStorage() { if (storageHttp != null) storageHttp.stop(0); }

    /** In-memory object store, served back over the loopback HTTP server above. */
    private final class MapStorage implements ExternalStorage {
        @Override public URI upload(byte[] body, String contentEncoding) {
            String key = UUID.randomUUID().toString();
            objects.put(key, body);
            return URI.create("http://127.0.0.1:" + storagePort + "/obj/" + key);
        }
    }

    private ExternalLocationConfig config() {
        return ExternalLocationConfig.builder()
                .storage(new MapStorage())
                .thresholdBytes(16)     // force externalisation of any real batch
                .urlValidator(ExternalLocationConfig.permissiveValidator())
                .build();
    }

    // ------------------------------------------------------------------

    @Test
    @Timeout(60)
    void resolvesExternalizedBatchesOnAProducerStream() throws Exception {
        ExternalLocationConfig cfg = config();
        try (Harness h = Harness.start(cfg, cfg)) {
            List<Long> rows = new ArrayList<>();
            List<Map<String, String>> metas = new ArrayList<>();
            drain(h.client().bulk(3), rows, metas);

            // Prove externalisation actually happened — otherwise this test would
            // pass just as happily against the broken client.
            assertEquals(3, objects.size(), "each data batch should have been externalized");

            List<Long> expected = new ArrayList<>();
            for (long i = 0; i < 3L * ROWS; i++) expected.add(i);
            assertEquals(expected, rows, "externalized batches must arrive with their rows intact");

            // Metadata semantics: the location keys are consumed by the resolution
            // (they describe the transport hop, not the data), everything the
            // producer stamped on the batch travels through unchanged.
            assertEquals(3, metas.size());
            for (Map<String, String> md : metas) {
                assertFalse(md.containsKey(Metadata.LOCATION),
                        "vgi_rpc.location must be stripped once resolved");
                assertTrue(md.getOrDefault(BATCH_TAG, "").startsWith("batch-"),
                        "producer batch metadata must survive externalisation: " + md);
            }
        }
    }

    /**
     * The same resolution on an <em>exchange</em> stream.
     *
     * <p>Both stream shapes read their output through {@code readNextDataBatch},
     * so resolution ought to be inherited — but "ought to" is what the producer
     * path also looked like before it was driven from a client, so the exchange
     * half is pinned rather than assumed. The client fetch is what proves it:
     * the pointer batch carries zero rows, so an unresolved one would show up
     * here as an empty answer to a non-empty question.
     */
    @Test
    @Timeout(60)
    void resolvesExternalizedBatchesOnAnExchangeStream() throws Exception {
        ExternalLocationConfig cfg = config();
        try (Harness h = Harness.start(cfg, cfg)) {
            ClientStreamSession<?> session = (ClientStreamSession<?>) h.client().doubling();
            try (VectorSchemaRoot input = VectorSchemaRoot.create(OUT_SCHEMA, Allocators.root())) {
                input.allocateNew();
                BigIntVector n = (BigIntVector) input.getVector("n");
                for (int i = 0; i < ROWS; i++) n.setSafe(i, i);
                n.setValueCount(ROWS);
                input.setRowCount(ROWS);

                AnnotatedBatch answer = session.exchange(new AnnotatedBatch(input, null));
                assertEquals(1, objects.size(), "the answer should have been externalized");
                assertFalse(answer.customMetadata().containsKey(Metadata.LOCATION),
                        "vgi_rpc.location must be stripped once resolved");
                assertEquals("exchanged", answer.customMetadata().get(BATCH_TAG),
                        "worker batch metadata must survive externalisation");

                List<Long> rows = new ArrayList<>();
                BigIntVector doubled = (BigIntVector) answer.root().getVector("n");
                for (int i = 0; i < answer.root().getRowCount(); i++) rows.add(doubled.get(i));
                List<Long> expected = new ArrayList<>();
                for (long i = 0; i < ROWS; i++) expected.add(i * 2);
                assertEquals(expected, rows);
            } finally {
                session.close();
            }
        }
    }

    @Test
    @Timeout(60)
    void failsLoudlyWhenThePointerCannotBeResolved() throws Exception {
        // Server externalises; client has no ExternalLocationConfig, so it cannot
        // fetch the payload. The rows exist and are unreachable — the one thing
        // the client must not do is hand back the pointer's empty root and let the
        // caller believe it saw the whole stream.
        try (Harness h = Harness.start(config(), null)) {
            RpcStream<BulkProducer> stream = h.client().bulk(1);
            RpcError err = assertThrows(RpcError.class,
                    () -> drain(stream, new ArrayList<>(), new ArrayList<>()));
            assertTrue(err.getMessage().contains(Metadata.LOCATION),
                    "the failure must name the unresolvable location: " + err.getMessage());
        }
    }

    @Test
    @Timeout(60)
    void inlineStreamsAreUnaffectedByAConfiguredResolver() throws Exception {
        // A threshold no batch reaches: nothing is externalized, and the resolver
        // must stay entirely out of the way of the ordinary inline path.
        ExternalLocationConfig never = ExternalLocationConfig.builder()
                .storage(new MapStorage())
                .thresholdBytes(1 << 30)
                .urlValidator(ExternalLocationConfig.permissiveValidator())
                .build();
        try (Harness h = Harness.start(never, never)) {
            List<Long> rows = new ArrayList<>();
            drain(h.client().bulk(2), rows, new ArrayList<>());
            assertEquals(0, objects.size(), "nothing should have been externalized");
            assertEquals(2 * ROWS, rows.size());
        }
    }

    // ------------------------------------------------------------------

    /** Tick a producer stream to exhaustion, collecting rows and batch metadata. */
    private static void drain(RpcStream<?> stream, List<Long> rows, List<Map<String, String>> metas) {
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
                if (root.getRowCount() == 0) continue;   // ticks that produced nothing
                metas.add(batch.customMetadata());
                BigIntVector v = (BigIntVector) root.getVector("n");
                for (int i = 0; i < root.getRowCount(); i++) rows.add(v.get(i));
            }
        } finally {
            session.close();
        }
    }

    /** Server on a daemon thread, client on the test thread, joined by a pipe pair. */
    private static final class Harness implements AutoCloseable {
        private final RpcConnection connection;
        private final BulkService proxy;
        private final RpcTransport clientTransport;
        private final Thread serverThread;

        private Harness(RpcConnection connection, BulkService proxy,
                        RpcTransport clientTransport, Thread serverThread) {
            this.connection = connection;
            this.proxy = proxy;
            this.clientTransport = clientTransport;
            this.serverThread = serverThread;
        }

        /**
         * @param serverConfig externalisation config for the server (what it uploads)
         * @param clientConfig resolution config for the client, or {@code null} to
         *     leave the client unable to resolve pointers
         */
        static Harness start(ExternalLocationConfig serverConfig,
                             ExternalLocationConfig clientConfig) throws Exception {
            PipedOutputStream clientOut = new PipedOutputStream();
            PipedInputStream serverIn = new PipedInputStream(clientOut, 1 << 16);
            PipedOutputStream serverOut = new PipedOutputStream();
            PipedInputStream clientIn = new PipedInputStream(serverOut, 1 << 16);

            RpcTransport serverTransport = new PipeTransport(serverIn, serverOut);
            RpcTransport clientTransport = new PipeTransport(clientIn, clientOut);

            RpcServer server = new RpcServer(BulkService.class, new Impl());
            server.setExternalConfig(serverConfig);
            Thread thread = new Thread(() -> server.serve(serverTransport), "rpc-server");
            thread.setDaemon(true);
            thread.start();

            RpcConnection connection = new RpcConnection(clientTransport, m -> {}, clientConfig);
            return new Harness(connection, connection.proxy(BulkService.class),
                    clientTransport, thread);
        }

        BulkService client() { return proxy; }

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
