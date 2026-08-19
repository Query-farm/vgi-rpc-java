// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.ExchangeState;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.ProducerState;
import farm.query.vgirpc.RpcError;
import farm.query.vgirpc.RpcServer;
import farm.query.vgirpc.RpcStream;
import farm.query.vgirpc.log.Message;
import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.schema.StreamHeader;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end cover for {@link HttpRpcConnection} against a real
 * {@link HttpServer} on a loopback ephemeral port.
 *
 * <p>{@code ClientStreamHeaderTest} and {@code ClientStreamExchangeTest} pin the
 * same call shapes over a <em>persistent</em> transport, where a stream is one
 * continuous pair of byte streams. HTTP has no such continuity: every turn is a
 * separate request whose only link to the last is a signed cursor riding the
 * body's Arrow {@code custom_metadata}. So the things worth asserting here are
 * the ones that only a request/response transport can get wrong — the cursor
 * being carried across turns at all, the trailing token batch not being handed
 * to the caller as an empty result, the two concatenated IPC streams of a
 * {@code @StreamHeader} init response being split at the right byte, and an
 * error surfacing as {@link RpcError} whether it arrives inside a 200 body or
 * behind a 500.</p>
 *
 * <p>The server is deliberately left with its default codec set enabled. The
 * client states {@code Accept-Encoding: identity}, so every test here is also
 * an assertion that the opt-out works: if it did not, the bodies would arrive
 * zstd-compressed and nothing would parse.</p>
 */
final class HttpRpcConnectionTest {

    static final Schema OUT_SCHEMA = new Schema(List.of(
            new Field("n", FieldType.notNullable(new ArrowType.Int(64, true)), null)));
    static final Schema IN_SCHEMA = new Schema(List.of(
            new Field("v", FieldType.nullable(new ArrowType.Int(64, true)), null)));
    static final Schema DOUBLED_SCHEMA = new Schema(List.of(
            new Field("doubled", FieldType.nullable(new ArrowType.Int(64, true)), null)));

    private static final String UNARY_FAILURE = "unary method refused";
    private static final String PRODUCE_FAILURE = "producer raised on its first tick";
    private static final String EXCHANGE_FAILURE = "exchange turn refused the input";
    private static final long POISON = -777L;

    /** Fired by {@link Doubler#onCancel}. */
    private static final CountDownLatch CANCELLED = new CountDownLatch(1);
    /** Fired by {@link CountingProducer#onCancel}. */
    private static final CountDownLatch PRODUCER_CANCELLED = new CountDownLatch(1);

    @Test
    void disabledExternalResolutionRedactsThePointerUrl() {
        String secret = "unit-secret-signature";
        RpcError error = assertThrows(RpcError.class, () -> HttpRpcConnection.failOnPointerBatch(Map.of(
                Metadata.LOCATION,
                "https://alice:password@example.com/data?X-Amz-Signature=" + secret + "#fragment-secret")));
        assertTrue(error.getMessage().contains("https://example.com/data"));
        assertFalse(error.getMessage().contains("alice"));
        assertFalse(error.getMessage().contains("password"));
        assertFalse(error.getMessage().contains(secret));
        assertFalse(error.getMessage().contains("fragment-secret"));
    }

    /** Stream header — the record a client must consume before the body stream. */
    public record Head(String tag, long total) implements ArrowSerializableRecord {}

    /** Emits one row per tick, so a multi-row drain needs real continuation POSTs. */
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

        @Override public void onCancel(CallContext ctx) { PRODUCER_CANCELLED.countDown(); }
    }

    /** Logs on its first tick, so the client's log sink has something to receive. */
    public static final class ChattyProducer extends ProducerState {
        public boolean spoken;

        public ChattyProducer() {}

        @Override public void produce(OutputCollector out, CallContext ctx) {
            if (!spoken) {
                spoken = true;
                ctx.clientLog(farm.query.vgirpc.log.Level.INFO, "halfway");
            }
            out.finish();
        }
    }

    /** Logs on <em>every</em> tick, so a client must see one log per turn. */
    public static final class NoisyProducer extends ProducerState {
        public long remaining;

        public NoisyProducer() {}

        NoisyProducer(long count) { this.remaining = count; }

        @Override public void produce(OutputCollector out, CallContext ctx) {
            ctx.clientLog(farm.query.vgirpc.log.Level.INFO, "tick " + remaining);
            if (remaining <= 0) {
                out.finish();
                return;
            }
            VectorSchemaRoot root = VectorSchemaRoot.create(OUT_SCHEMA, Allocators.root());
            root.allocateNew();
            ((BigIntVector) root.getVector(0)).setSafe(0, remaining);
            root.setRowCount(1);
            out.emit(root);
            remaining--;
        }
    }

    /** Raises in-band, after the header stream has already been written. */
    public static final class RaisingProducer extends ProducerState {
        public RaisingProducer() {}

        @Override public void produce(OutputCollector out, CallContext ctx) {
            throw new IllegalStateException(PRODUCE_FAILURE);
        }
    }

    /** Doubles the input column; echoes the caller's per-batch metadata back. */
    public static final class Doubler extends ExchangeState {
        public long turns;

        public Doubler() {}

        @Override
        public void exchange(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
            ctx.clientLog(farm.query.vgirpc.log.Level.INFO, "turn for " + input.root().getRowCount() + " rows");
            VectorSchemaRoot in = input.root();
            BigIntVector src = (BigIntVector) in.getVector("v");
            int rows = in.getRowCount();
            VectorSchemaRoot root = VectorSchemaRoot.create(DOUBLED_SCHEMA, Allocators.root());
            root.allocateNew();
            BigIntVector dst = (BigIntVector) root.getVector("doubled");
            for (int i = 0; i < rows; i++) {
                if (src.get(i) == POISON) {
                    root.close();
                    ctx.clientLog(farm.query.vgirpc.log.Level.WARN, "about to fail");
                    throw new IllegalStateException(EXCHANGE_FAILURE);
                }
                dst.setSafe(i, src.get(i) * 2);
            }
            dst.setValueCount(rows);
            root.setRowCount(rows);
            turns++;
            out.emit(root, Map.of(
                    "x.turn", Long.toString(turns),
                    "x.echo", String.valueOf(input.customMetadata().get("x.tag"))));
        }

        @Override public void onCancel(CallContext ctx) { CANCELLED.countDown(); }
    }

    public interface DemoService {
        long ping(long v);

        long answer();

        String greet(String name);

        long boom(long v);

        @StreamHeader(Head.class)
        RpcStream<CountingProducer> count(long n);

        RpcStream<CountingProducer> count_headerless(long n);

        RpcStream<ChattyProducer> chatty();

        RpcStream<NoisyProducer> noisy(long n);

        @StreamHeader(Head.class)
        RpcStream<RaisingProducer> raise_in_produce();

        @StreamHeader(Head.class)
        RpcStream<Doubler> doubling();
    }

    public static final class Impl implements DemoService {
        @Override public long ping(long v) { return v; }

        @Override public long answer() { return 42L; }

        @Override public String greet(String name) { return "hello " + name; }

        @Override public long boom(long v) { throw new IllegalStateException(UNARY_FAILURE); }

        @Override public RpcStream<CountingProducer> count(long n) {
            return RpcStream.producer(OUT_SCHEMA, new CountingProducer(n), new Head("counting", n));
        }

        @Override public RpcStream<CountingProducer> count_headerless(long n) {
            return RpcStream.producer(OUT_SCHEMA, new CountingProducer(n));
        }

        @Override public RpcStream<ChattyProducer> chatty() {
            return RpcStream.producer(OUT_SCHEMA, new ChattyProducer());
        }

        @Override public RpcStream<NoisyProducer> noisy(long n) {
            return RpcStream.producer(OUT_SCHEMA, new NoisyProducer(n));
        }

        @Override public RpcStream<RaisingProducer> raise_in_produce() {
            return RpcStream.producer(OUT_SCHEMA, new RaisingProducer(), new Head("before-the-failure", 0));
        }

        @Override public RpcStream<Doubler> doubling() {
            return RpcStream.exchange(IN_SCHEMA, DOUBLED_SCHEMA, new Doubler(), new Head("doubling", 0));
        }
    }

    private HttpServer server;
    private HttpRpcConnection connection;
    private final List<Message> logs = new CopyOnWriteArrayList<>();

    @BeforeEach
    void start() throws Exception {
        server = new HttpServer(new RpcServer(DemoService.class, new Impl()),
                HttpServer.Config.builder().prefix("/vgi").build());
        server.start();
        connection = HttpRpcConnection.builder(endpoint()).onLog(logs::add).build();
    }

    @AfterEach
    void stop() throws Exception {
        if (connection != null) connection.close();
        if (server != null) server.stop();
    }

    private String endpoint() { return "http://127.0.0.1:" + server.port() + "/vgi"; }

    private DemoService client() { return connection.proxy(DemoService.class); }

    // --- unary ------------------------------------------------------------

    @Test
    @Timeout(30)
    void unaryCallsRoundTrip() {
        DemoService svc = client();
        assertEquals(42L, svc.ping(42L));
        assertEquals("hello world", svc.greet("world"));
        // A no-argument method still sends a one-row batch of the empty schema;
        // a zero-row one reads back as "no params" and never binds.
        assertEquals(42L, svc.answer());
    }

    @Test
    @Timeout(30)
    void aWorkerErrorOnAUnaryCallSurfacesAsRpcError() {
        DemoService svc = client();
        RpcError err = assertThrows(RpcError.class, () -> svc.boom(1L));
        assertTrue(err.getMessage().contains(UNARY_FAILURE), err.getMessage());
        assertEquals(9L, svc.ping(9L), "the connection must survive a failed call");
    }

    // --- producer streams -------------------------------------------------

    /**
     * The core producer contract: the init response's batch, then one
     * continuation POST per later batch, ending at the response that offers no
     * cursor. Five rows means four continuations — enough that a client which
     * only ever read the init response would fail loudly.
     */
    @Test
    @Timeout(30)
    void producerStreamsDrainToEndOfStream() {
        try (RpcStream<?> stream = client().count(5)) {
            assertEquals(List.of(0L, 1L, 2L, 3L, 4L), drain(stream));
        }
    }

    @Test
    @Timeout(30)
    void anEmptyProducerEndsImmediately() {
        try (RpcStream<?> stream = client().count(0)) {
            assertEquals(List.of(), drain(stream));
        }
    }

    @Test
    @Timeout(30)
    void tickAfterEndOfStreamKeepsThrowing() {
        try (RpcStream<?> stream = client().count(1)) {
            assertEquals(List.of(0L), drain(stream));
            // drain() consumed the terminal NoSuchElementException, which closes
            // the stream; a further tick must refuse rather than POST a retired
            // cursor.
            assertThrows(RpcError.class, stream::tick);
            stream.close();
            stream.close();
        }
    }

    @Test
    @Timeout(30)
    void theDeclaredStreamHeaderArrivesBeforeTheData() {
        try (RpcStream<?> stream = client().count(3)) {
            Head head = (Head) stream.header();
            assertNotNull(head, "@StreamHeader must be read off the init response");
            assertEquals("counting", head.tag());
            assertEquals(3L, head.total());
            // Proves the header stream and the body stream were split at the
            // right byte: a header mistaken for data would show up here.
            assertEquals(List.of(0L, 1L, 2L), drain(stream));
        }
    }

    @Test
    @Timeout(30)
    void headerlessStreamsStillWork() {
        try (RpcStream<?> stream = client().count_headerless(2)) {
            assertNull(stream.header());
            assertEquals(List.of(0L, 1L), drain(stream));
        }
    }

    @Test
    @Timeout(30)
    void workerLogBatchesReachTheLogSink() {
        try (RpcStream<?> stream = client().chatty()) {
            assertEquals(List.of(), drain(stream));
        }
        assertTrue(logs.stream().anyMatch(m -> "halfway".equals(m.message())),
                "log batches interleaved into a stream response must reach onLog, not the caller: " + logs);
    }

    /**
     * A producer that raises writes its exception <em>behind</em> the header
     * stream, inside a plain 200 that carries no {@code X-VGI-RPC-Error} flag
     * (see {@link InBandStreamErrorHeaderTest}). A client that stopped at the
     * header stream's end-of-stream would report a generic transport failure
     * with the worker's message discarded.
     */
    @Test
    @Timeout(30)
    void anInBandProducerFailureSurfacesAsRpcError() {
        DemoService svc = client();
        try (RpcStream<?> stream = svc.raise_in_produce()) {
            assertEquals("before-the-failure", ((Head) stream.header()).tag());
            RpcError err = assertThrows(RpcError.class, stream::tick);
            assertTrue(err.getMessage().contains(PRODUCE_FAILURE), err.getMessage());
        }
        assertEquals(3L, svc.ping(3L), "the connection must survive an in-band stream failure");
    }

    // --- exchange streams -------------------------------------------------

    @Test
    @Timeout(30)
    void exchangeStreamsAnswerEveryTurn() {
        DemoService svc = client();
        try (RpcStream<?> stream = svc.doubling()) {
            assertEquals("doubling", ((Head) stream.header()).tag());
            assertEquals(List.of(2L, 4L, 6L), send(stream, 1, 2, 3));
            assertEquals(List.of(20L), send(stream, 10));
            // A zero-row input is data with no rows, not end-of-stream; the
            // turns after it prove the cursor stayed in step.
            assertEquals(List.of(), send(stream));
            assertEquals(List.of(-2L), send(stream, -1));
        }
    }

    @Test
    @Timeout(30)
    void exchangeCarriesPerBatchMetadataInBothDirections() {
        try (RpcStream<?> stream = client().doubling();
             VectorSchemaRoot input = inputBatch(5)) {
            AnnotatedBatch out = stream.exchange(new AnnotatedBatch(input, Map.of("x.tag", "first")));
            assertEquals("1", out.customMetadata().get("x.turn"),
                    "worker per-batch metadata must reach the client");
            assertEquals("first", out.customMetadata().get("x.echo"),
                    "client per-batch metadata must reach the worker");
            assertNull(out.customMetadata().get("vgi_rpc.stream_state#b64"),
                    "the continuation cursor is transport bookkeeping and must not leak to the caller");
        }
    }

    @Test
    @Timeout(30)
    void aWorkerErrorMidExchangeSurfacesAsRpcErrorAndSpendsTheStream() {
        DemoService svc = client();
        RpcStream<?> stream = svc.doubling();
        try {
            assertEquals(List.of(2L), send(stream, 1));

            RpcError err = assertThrows(RpcError.class, () -> send(stream, POISON));
            assertTrue(err.getMessage().contains(EXCHANGE_FAILURE), err.getMessage());

            RpcError retry = assertThrows(RpcError.class, () -> send(stream, 1),
                    "a spent stream must refuse further turns");
            assertEquals("ProtocolError", retry.errorType());
        } finally {
            stream.close();
        }
        assertEquals(11L, svc.ping(11L), "the connection must survive a failed exchange turn");
    }

    @Test
    @Timeout(30)
    void cancelReachesTheWorker() throws Exception {
        DemoService svc = client();
        RpcStream<?> stream = svc.doubling();
        assertEquals(List.of(2L, 4L), send(stream, 1, 2));

        stream.cancel();
        assertTrue(CANCELLED.await(10, TimeUnit.SECONDS),
                "cancel() must deliver vgi_rpc.cancel so the worker runs onCancel");
        assertEquals(5L, svc.ping(5L), "the connection must survive a cancel");
    }

    // --- connection reuse --------------------------------------------------

    @Test
    @Timeout(30)
    void oneConnectionServesManyCallsOfEveryShape() {
        DemoService svc = client();
        assertEquals(List.of(0L, 1L), drain(svc.count(2)));
        assertEquals(42L, svc.ping(42L));
        try (RpcStream<?> ex = svc.doubling()) {
            assertEquals(List.of(6L), send(ex, 3));
        }
        assertEquals(List.of(0L, 1L, 2L, 3L), drain(svc.count(4)));
        assertEquals("hello again", svc.greet("again"));
    }

    // --- auth --------------------------------------------------------------

    @Test
    @Timeout(30)
    void bearerTokensAreSentOnEveryRequest() throws Exception {
        HttpServer guarded = new HttpServer(new RpcServer(DemoService.class, new Impl()),
                HttpServer.Config.builder()
                        .prefix("/vgi")
                        .authenticator(farm.query.vgirpc.http.auth.BearerAuthenticator.fromMap(
                                Map.of("s3cret", new farm.query.vgirpc.AuthContext(
                                        "bearer", true, "alice", Map.of()))))
                        .build());
        guarded.start();
        String url = "http://127.0.0.1:" + guarded.port() + "/vgi";
        try {
            try (HttpRpcConnection authed = HttpRpcConnection.builder(url).bearerToken("s3cret").build()) {
                DemoService svc = authed.proxy(DemoService.class);
                assertEquals(7L, svc.ping(7L));
                // Streams authenticate per turn too, so a token that only rode
                // the init request would fail on the first continuation.
                assertEquals(List.of(0L, 1L, 2L), drain(svc.count(3)));
            }
            try (HttpRpcConnection anon = HttpRpcConnection.builder(url).build()) {
                RpcError err = assertThrows(RpcError.class, () -> anon.proxy(DemoService.class).ping(1L));
                assertEquals("AuthenticationError", err.errorType(),
                        "a 401 JSON envelope must become an RpcError, not a parse failure");
            }
        } finally {
            guarded.stop();
        }
    }

    @Test
    @Timeout(30)
    void anUnroutableUrlFailsAsAnRpcErrorNotAnIoException() throws Exception {
        try (HttpRpcConnection bad = HttpRpcConnection.builder(endpoint() + "/nope").build()) {
            RpcError err = assertThrows(RpcError.class, () -> bad.proxy(DemoService.class).ping(1L));
            assertEquals("HttpError", err.errorType(), err.getMessage());
        }
    }

    /**
     * A continuation must carry the call token, not just the cursor.
     *
     * <p>The server keeps a call-state cache keyed by the (authenticated) call
     * id, so while it is warm a client that never echoed
     * {@code vgi_rpc.call_state} looks perfectly correct. Disabling the cache
     * reproduces the deployment where it never helps — a cold process, an
     * evicted entry, or a request load-balanced onto a node that never served
     * this stream's {@code /init} — which is where a client that omitted the
     * token starts failing, under load and nowhere else.
     */
    @Test
    @Timeout(30)
    void continuationsWorkWithTheCallStateCacheDisabled() throws Exception {
        HttpServer cold = new HttpServer(new RpcServer(DemoService.class, new Impl()),
                HttpServer.Config.builder().prefix("/vgi").callStateCacheMaxEntries(0).build());
        cold.start();
        try (HttpRpcConnection conn =
                     HttpRpcConnection.builder("http://127.0.0.1:" + cold.port() + "/vgi").build()) {
            DemoService svc = conn.proxy(DemoService.class);
            assertEquals(List.of(0L, 1L, 2L, 3L), drain(svc.count(4)));
            try (RpcStream<?> ex = svc.doubling()) {
                assertEquals(List.of(2L), send(ex, 1));
                assertEquals(List.of(4L), send(ex, 2));
            }
        } finally {
            cold.stop();
        }
    }

    /**
     * {@link RpcStream#batches()} is the ergonomic way to read a producer, and
     * it goes through {@code tick()} — so it has to see exactly the batches a
     * hand-rolled tick loop does, including stopping at the same place.
     */
    @Test
    @Timeout(30)
    void theBatchesIteratorReadsAProducerToTheEnd() {
        List<Long> seen = new ArrayList<>();
        try (RpcStream<?> stream = client().count(3)) {
            for (AnnotatedBatch batch : stream.batches()) {
                BigIntVector v = (BigIntVector) batch.root().getVector("n");
                for (int i = 0; i < batch.root().getRowCount(); i++) seen.add(v.get(i));
            }
        }
        assertEquals(List.of(0L, 1L, 2L), seen);
    }

    /**
     * Cancelling a producer must reach the worker even though the client has
     * not yet read a cursor.
     *
     * <p>A producer's continuation token trails its data batch, so a stream
     * cancelled before (or right after) its first tick holds no cursor to
     * present — and a client that simply checked "do I have a token?" would
     * close locally and leave the worker's state to expire on a timer, with
     * {@code onCancel} never running. That is a silent no-op on the streams a
     * caller most wants to abandon: the long ones.
     */
    @Test
    @Timeout(30)
    void cancelReachesTheWorkerOnAProducerStreamBeforeAnyTick() throws Exception {
        DemoService svc = client();
        RpcStream<?> stream = svc.count(1_000);
        stream.cancel();
        assertTrue(PRODUCER_CANCELLED.await(10, TimeUnit.SECONDS),
                "cancel() before the first tick must still deliver vgi_rpc.cancel");
        assertEquals(4L, svc.ping(4L), "the connection must survive a cancel");
    }

    /**
     * A worker's log lines must survive past the first turn.
     *
     * <p>Over the stream transports {@code RpcServer} binds the client-log sink
     * to the output stream once and leaves it bound for the whole tick loop, so
     * a log from any turn reaches the caller. HTTP has no single output stream:
     * each turn writes its own response, so the sink has to be re-bound every
     * time — and a turn whose sink is never bound buffers its messages into a
     * list that dies with the request. That is not a dropped nicety: worker logs
     * are the only in-band diagnostic a caller has for a long stream, and losing
     * every one after the first is worst precisely on the streams long enough to
     * need them.
     */
    @Test
    @Timeout(30)
    void workerLogsSurviveEveryProducerTurn() {
        try (RpcStream<?> stream = client().noisy(3)) {
            assertEquals(List.of(3L, 2L, 1L), drain(stream));
        }
        List<String> texts = logs.stream().map(Message::message).toList();
        assertEquals(List.of("tick 3", "tick 2", "tick 1", "tick 0"), texts,
                "every producer turn's log must reach the client, not just the init turn");
    }

    @Test
    @Timeout(30)
    void workerLogsSurviveEveryExchangeTurn() {
        try (RpcStream<?> stream = client().doubling()) {
            assertEquals(List.of(2L), send(stream, 1));
            assertEquals(List.of(4L, 6L), send(stream, 2, 3));
        }
        List<String> texts = logs.stream().map(Message::message).toList();
        assertEquals(List.of("turn for 1 rows", "turn for 2 rows"), texts,
                "an exchange turn's logs are written on its own response or nowhere");
    }

    /**
     * The logs a turn wrote before it failed must travel with the failure.
     *
     * <p>An error batch replaces the response body on the HTTP transport, so a
     * turn that logged its way up to the fault and then threw used to answer
     * with the exception alone — while the same worker over a pipe delivered
     * both, because its sink was already bound to the output stream. Losing the
     * breadcrumbs on the one response that needed them is the wrong half to
     * drop.
     */
    @Test
    @Timeout(30)
    void logsWrittenBeforeAFailureTravelWithTheError() {
        try (RpcStream<?> stream = client().doubling()) {
            RpcError err = assertThrows(RpcError.class, () -> send(stream, POISON));
            assertTrue(err.getMessage().contains(EXCHANGE_FAILURE), err.getMessage());
        }
        List<String> texts = logs.stream().map(Message::message).toList();
        assertTrue(texts.contains("about to fail"),
                "logs emitted before the throw must ride the error response: " + texts);
    }

    // --- helpers -----------------------------------------------------------

    /** Tick a producer stream to end-of-stream, decoding the {@code n} column. */
    private static List<Long> drain(RpcStream<?> stream) {
        List<Long> out = new ArrayList<>();
        try {
            while (true) {
                AnnotatedBatch batch;
                try {
                    batch = stream.tick();
                } catch (NoSuchElementException endOfStream) {
                    break;
                }
                VectorSchemaRoot root = batch.root();
                BigIntVector v = (BigIntVector) root.getVector("n");
                for (int i = 0; i < root.getRowCount(); i++) out.add(v.get(i));
            }
        } finally {
            stream.close();
        }
        return out;
    }

    /** One exchange turn: send {@code values} as the {@code v} column, decode the answer. */
    private static List<Long> send(RpcStream<?> stream, long... values) {
        try (VectorSchemaRoot input = inputBatch(values)) {
            AnnotatedBatch out = stream.exchange(new AnnotatedBatch(input, null));
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
}
