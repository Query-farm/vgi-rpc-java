// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.ClientStreamSession;
import farm.query.vgirpc.ExchangeState;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.ProducerState;
import farm.query.vgirpc.RpcConnection;
import farm.query.vgirpc.RpcServer;
import farm.query.vgirpc.RpcStream;
import farm.query.vgirpc.external.ExternalLocationConfig;
import farm.query.vgirpc.external.LocationResolver;
import farm.query.vgirpc.marshal.Marshalling;
import farm.query.vgirpc.transport.RpcTransport;
import farm.query.vgirpc.wire.Allocators;
import farm.query.vgirpc.wire.IpcStreamReader;
import farm.query.vgirpc.wire.IpcStreamWriter;
import farm.query.vgirpc.wire.Metadata;
import farm.query.vgirpc.wire.Wire;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A stream method is handed each input's own custom metadata -- every turn, every
 * transport -- and never the transport's bookkeeping.
 *
 * <p>That metadata is application data, per input: the DuckDB extension puts the
 * result-cache validators ({@code vgi.cache.if_none_match}) on every exchange input
 * and dynamic-filter deltas on every tick, and a worker reads them off
 * {@link AnnotatedBatch#customMetadata()}. Over HTTP each turn is its own request
 * and also carries the stream's cursor and call token on the same batch. This
 * server handed those tokens to {@code process()} along with the input's keys,
 * on exchange and producer continuations alike -- a transport-parity break (the
 * pipe never puts them on a batch), and a sealed token application code must not
 * read.
 *
 * <p>An externalized input is the second half. A resolved batch carries the
 * <em>fetched</em> batch's metadata plus the reader's
 * {@code vgi_rpc.location.source} / {@code fetch_ms} stamp, never the pointer's
 * (WIRE_PROTOCOL.md §12). This server merged the pointer's keys in, over HTTP and
 * the pipe alike. The shared conformance suite gives its pointer and payload the
 * same keys, so it cannot see a pointer-only key survive; the pointers here carry
 * one.
 *
 * <p>Every request is built by hand and sent to a real server -- over real HTTP,
 * or through {@link RpcConnection} on a pipe. The object an externalized input
 * points at is request data, served by a local HTTP server standing in for the
 * client's storage. Every response is the server's own.
 */
@Timeout(60)
final class StreamInputMetadataTest {

    private static final String ARROW = "application/vnd.apache.arrow.stream";

    /** The application key the test states report the value of. */
    static final String KEY = "vgi.test.input";
    /** A second application key: every key passes, not just the one read. */
    static final String EXTRA = "vgi.test.extra";
    /** Put on a pointer and never on its payload, so it cannot leak by coincidence. */
    static final String POINTER_ONLY = "vgi.test.pointer_only";

    /** The HTTP stream tokens and the cancel marker: bookkeeping, not input. */
    static final List<String> TRANSPORT_KEYS =
            List.of(Metadata.STREAM_STATE, Metadata.CALL_STATE, Metadata.CANCEL);

    static final Schema IN_SCHEMA = new Schema(List.of(new Field("value",
            FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null)));
    static final Schema OUT_SCHEMA = new Schema(List.of(
            new Field("seen", FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("keys", FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("source", FieldType.nullable(new ArrowType.Utf8()), null)));

    /**
     * One output row describing the metadata an input was handed: {@code seen} is
     * the value of {@link #KEY} (empty when absent), {@code keys} every key present,
     * sorted and comma-joined, and {@code source} the {@code vgi_rpc.location.source}
     * stamp (empty when absent).
     */
    static VectorSchemaRoot report(Map<String, String> md) {
        VectorSchemaRoot root = VectorSchemaRoot.create(OUT_SCHEMA, Allocators.root());
        root.allocateNew();
        ((VarCharVector) root.getVector("seen")).setSafe(0, utf8(md.getOrDefault(KEY, "")));
        ((VarCharVector) root.getVector("keys")).setSafe(0, utf8(String.join(",", new TreeSet<>(md.keySet()))));
        ((VarCharVector) root.getVector("source"))
                .setSafe(0, utf8(md.getOrDefault(Metadata.LOCATION_SOURCE, "")));
        root.setRowCount(1);
        return root;
    }

    /** Exchange: one row per input, describing that input's metadata. */
    public static final class Report extends ExchangeState {
        public Report() {}

        @Override public void exchange(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
            out.emit(report(input.customMetadata()));
        }
    }

    /** Producer: one row per tick describing the tick's metadata, for {@code ticks} ticks. */
    public static final class Ticks extends ProducerState {
        long remaining;

        public Ticks() {}

        Ticks(long ticks) { this.remaining = ticks; }

        @Override public void produce(OutputCollector out, CallContext ctx) {
            tick(Map.of(), out);
        }

        @Override public void produce(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
            tick(input.customMetadata(), out);
        }

        private void tick(Map<String, String> md, OutputCollector out) {
            if (remaining <= 0) {
                out.finish();
                return;
            }
            remaining--;
            out.emit(report(md));
        }
    }

    public interface MetaService {
        RpcStream<Report> report();

        RpcStream<Ticks> ticks(long ticks);
    }

    public static final class Impl implements MetaService {
        @Override public RpcStream<Report> report() {
            return RpcStream.exchange(IN_SCHEMA, OUT_SCHEMA, new Report());
        }

        @Override public RpcStream<Ticks> ticks(long ticks) {
            return RpcStream.producer(OUT_SCHEMA, new Ticks(ticks));
        }
    }

    private RpcServer rpc;
    private HttpServer server;
    private String base;
    private com.sun.net.httpserver.HttpServer storage;
    private final Map<String, byte[]> objects = new ConcurrentHashMap<>();

    @BeforeEach
    void start() throws Exception {
        storage = com.sun.net.httpserver.HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        storage.createContext("/obj/", exchange -> {
            byte[] body = objects.get(exchange.getRequestURI().getPath().substring("/obj/".length()));
            if (body == null) {
                exchange.sendResponseHeaders(404, -1);
                exchange.close();
                return;
            }
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.getResponseBody().close();
        });
        storage.start();

        rpc = new RpcServer(MetaService.class, new Impl());
        rpc.setLocationResolver(new LocationResolver(ExternalLocationConfig.builder()
                .urlValidator(ExternalLocationConfig.permissiveValidator()).build()));
        server = new HttpServer(rpc, HttpServer.Config.builder()
                .prefix("/vgi").supportedEncodings(List.of()).build());
        server.start();
        base = "http://127.0.0.1:" + server.port() + "/vgi/MetaService/";
    }

    @AfterEach
    void stop() throws Exception {
        if (server != null) server.stop();
        if (storage != null) storage.stop(0);
    }

    // --- HTTP exchange ------------------------------------------------------

    /**
     * Three turns with different metadata each, so a frozen or carried-over value
     * fails a later turn; the last carries none and must see none. No turn may see
     * the tokens that addressed it.
     */
    @Test
    void anHttpExchangeTurnSeesItsOwnInputMetadataAndNotTheTokens() throws Exception {
        Tokens tokens = init("report", Map.of(), Map.of()).tokens;

        List<Map<String, String>> turns = List.of(
                Map.of(KEY, "first"),
                Map.of(KEY, "second", EXTRA, "1"),
                Map.of());
        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < turns.size(); i++) {
            Map<String, String> md = new LinkedHashMap<>(turns.get(i));
            md.putAll(tokens.asMetadata());
            Turn turn = post("report/exchange", dataBody(i, md));
            rows.add(turn.single());
            tokens = tokens.advancedBy(turn.tokens);
        }

        assertEquals(List.of("first", "second", ""), rows.stream().map(Row::seen).toList(),
                "each exchange input's metadata must reach exchange() with that input; got " + rows);
        assertTrue(rows.get(1).keys().contains(EXTRA), "every application key passes, not one: " + rows.get(1));
        assertEquals(List.of(), rows.get(2).keys(), "the bare turn must see no metadata at all: " + rows.get(2));
        for (int i = 0; i < rows.size(); i++) assertNoTransportKeys("turn " + (i + 1), rows.get(i));
    }

    /**
     * An externalized exchange input whose pointer and payload disagree: the method
     * sees the payload's value, the reader's provenance stamp, and nothing that was
     * only on the pointer -- neither its location keys, nor the tokens, nor an
     * application key the payload does not carry.
     */
    @Test
    void anExternalizedHttpExchangeInputCarriesThePayloadsMetadataNotThePointers() throws Exception {
        Tokens tokens = init("report", Map.of(), Map.of()).tokens;

        // The payload carries the tokens too, as the reference's case does: a
        // resolved input must be stripped as well, not only an inline one.
        Map<String, String> payloadMeta = new LinkedHashMap<>(tokens.asMetadata());
        payloadMeta.put(KEY, "from-payload");
        Map<String, String> pointerMeta = new LinkedHashMap<>(tokens.asMetadata());
        pointerMeta.put(KEY, "from-pointer");
        pointerMeta.put(POINTER_ONLY, "1");
        String url = upload(payloadMeta);
        pointerMeta.putAll(pointerTo(url));

        Row row = post("report/exchange", pointerBody(pointerMeta)).single();

        assertEquals("from-payload", row.seen(), "resolved metadata is the fetched payload's, not the pointer's");
        assertResolvedProvenance(row, url);
        assertNoTransportKeys("externalized turn", row);
    }

    // --- HTTP producer ------------------------------------------------------

    /**
     * {@code /init} runs a producer's first turn, so its request metadata is the
     * first tick's; each continuation's tick sees its own. The init body here also
     * carries a crafted cursor -- an honest client has none yet -- which the first
     * tick must not see either.
     */
    @Test
    void anHttpProducerTickSeesItsOwnMetadataAndNotTheTokens() throws Exception {
        Turn first = init("ticks", Map.of("ticks", 3L),
                Map.of(KEY, "at-init", Metadata.STREAM_STATE, "crafted-by-the-client"));
        List<Row> rows = new ArrayList<>(List.of(first.single()));
        Tokens tokens = first.tokens;
        for (String value : List.of("tick-2", "tick-3")) {
            Map<String, String> md = new LinkedHashMap<>(tokens.asMetadata());
            md.put(KEY, value);
            Turn turn = post("ticks/exchange", tickBody(md));
            rows.add(turn.single());
            tokens = tokens.advancedBy(turn.tokens);
        }

        assertEquals(List.of("at-init", "tick-2", "tick-3"), rows.stream().map(Row::seen).toList(),
                "each tick must see its own request's metadata; got " + rows);
        for (int i = 0; i < rows.size(); i++) assertNoTransportKeys("tick " + (i + 1), rows.get(i));
    }

    // --- pipe ---------------------------------------------------------------

    /**
     * The same pointer rule on the pipe transport, which shares the resolver: the
     * method sees the payload's metadata and provenance, not the pointer's keys.
     */
    @Test
    void anExternalizedPipeExchangeInputCarriesThePayloadsMetadataNotThePointers() throws Exception {
        String url = upload(Map.of(KEY, "from-payload"));
        Map<String, String> pointerMeta = new LinkedHashMap<>(Map.of(KEY, "from-pointer", POINTER_ONLY, "1"));
        pointerMeta.putAll(pointerTo(url));

        PipedOutputStream clientOut = new PipedOutputStream();
        PipedInputStream serverIn = new PipedInputStream(clientOut, 1 << 16);
        PipedOutputStream serverOut = new PipedOutputStream();
        PipedInputStream clientIn = new PipedInputStream(serverOut, 1 << 16);
        RpcTransport serverSide = new PipeTransport(serverIn, serverOut);
        RpcTransport clientSide = new PipeTransport(clientIn, clientOut);
        Thread serving = new Thread(() -> rpc.serve(serverSide), "rpc-server");
        serving.setDaemon(true);
        serving.start();

        Row inline;
        Row resolved;
        try (RpcConnection connection = new RpcConnection(clientSide)) {
            ClientStreamSession<?> session =
                    (ClientStreamSession<?>) connection.proxy(MetaService.class).report();
            try (VectorSchemaRoot data = inputBatch(7.0);
                 VectorSchemaRoot pointer = zeroRows(IN_SCHEMA)) {
                inline = Row.of(session.exchange(new AnnotatedBatch(data, Map.of(KEY, "inline"))).root());
                resolved = Row.of(session.exchange(new AnnotatedBatch(pointer, pointerMeta)).root());
            } finally {
                session.close();
            }
        } finally {
            clientSide.close();
            serving.join(2000);
        }

        assertEquals("inline", inline.seen(), "control: an inline pipe input's metadata reaches exchange()");
        assertEquals("from-payload", resolved.seen(), "resolved metadata is the fetched payload's, not the pointer's");
        assertResolvedProvenance(resolved, url);
    }

    // --- assertions ---------------------------------------------------------

    private static void assertNoTransportKeys(String where, Row row) {
        for (String key : TRANSPORT_KEYS) {
            assertFalse(row.keys().contains(key),
                    where + ": " + key + " is transport bookkeeping and must not reach application code; got "
                            + row.keys());
        }
    }

    private static void assertResolvedProvenance(Row row, String url) {
        assertTrue(row.keys().contains(Metadata.LOCATION_FETCH),
                "the reader must stamp " + Metadata.LOCATION_FETCH + " on a resolved input; got " + row.keys());
        assertEquals(url, row.source(), "the reader's " + Metadata.LOCATION_SOURCE + " stamp, not the pointer's");
        for (String key : List.of(Metadata.LOCATION, Metadata.LOCATION_SHA256, POINTER_ONLY)) {
            assertFalse(row.keys().contains(key),
                    key + " was only on the pointer and must not reach application code; got " + row.keys());
        }
    }

    // --- requests (built by hand) -------------------------------------------

    private Turn init(String method, Map<String, Object> kwargs, Map<String, String> extra) throws Exception {
        Schema params = rpc.methods().get(method).paramsSchema();
        Map<String, String> md = Wire.requestMetadata(method);
        md.putAll(extra);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (IpcStreamWriter w = new IpcStreamWriter(out)) {
            w.writeSchema(params);
            if (params.getFields().isEmpty()) {
                try (VectorSchemaRoot zero = VectorSchemaRoot.create(params, Allocators.root())) {
                    zero.setRowCount(1);
                    w.writeBatch(zero, md);
                }
            } else {
                try (Marshalling.EncodedRow enc = Marshalling.encodeRowForWire(params, kwargs, Allocators.root())) {
                    w.writeBatch(enc.root(), md, enc.provider());
                }
            }
        }
        Turn turn = post(method + "/init", out.toByteArray());
        assertNotNull(turn.tokens.cursor, "/init must mint a cursor");
        assertNotNull(turn.tokens.call, "/init must mint a call token");
        return turn;
    }

    private static VectorSchemaRoot inputBatch(double value) {
        VectorSchemaRoot root = VectorSchemaRoot.create(IN_SCHEMA, Allocators.root());
        root.allocateNew();
        ((Float8Vector) root.getVector("value")).setSafe(0, value);
        root.setRowCount(1);
        return root;
    }

    private static byte[] dataBody(double value, Map<String, String> md) throws Exception {
        try (VectorSchemaRoot root = inputBatch(value)) {
            return oneBatch(root, md);
        }
    }

    private static VectorSchemaRoot zeroRows(Schema schema) {
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, Allocators.root());
        root.allocateNew();
        root.setRowCount(0);
        return root;
    }

    /** A zero-row batch of the input schema: with the location keys, a pointer. */
    private static byte[] pointerBody(Map<String, String> md) throws Exception {
        try (VectorSchemaRoot root = zeroRows(IN_SCHEMA)) {
            return oneBatch(root, md);
        }
    }

    /** A producer tick: a zero-row batch of the empty schema. */
    private static byte[] tickBody(Map<String, String> md) throws Exception {
        try (VectorSchemaRoot root = zeroRows(RpcStream.EMPTY_SCHEMA)) {
            return oneBatch(root, md);
        }
    }

    private static byte[] oneBatch(VectorSchemaRoot root, Map<String, String> md) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (IpcStreamWriter w = new IpcStreamWriter(out)) {
            w.writeSchema(root.getSchema());
            w.writeBatch(root, md);
        }
        return out.toByteArray();
    }

    /** Store an externalized input -- one data batch, value 1.0 -- and return its URL. */
    private String upload(Map<String, String> payloadMeta) throws Exception {
        byte[] payload = dataBody(1.0, payloadMeta);
        String key = UUID.randomUUID().toString();
        objects.put(key, payload);
        return "http://127.0.0.1:" + storage.getAddress().getPort() + "/obj/" + key;
    }

    /** The location keys of a pointer to {@code url}, sha256-pinned to what is stored there. */
    private Map<String, String> pointerTo(String url) throws Exception {
        byte[] payload = objects.get(url.substring(url.lastIndexOf('/') + 1));
        String sha = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(payload));
        return Map.of(Metadata.LOCATION, url, Metadata.LOCATION_SHA256, sha);
    }

    private Turn post(String path, byte[] body) throws Exception {
        HttpResponse<byte[]> resp;
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()) {
            resp = client.send(HttpRequest.newBuilder(URI.create(base + path))
                    .timeout(Duration.ofSeconds(20))
                    .header("Content-Type", ARROW)
                    .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                    .build(), HttpResponse.BodyHandlers.ofByteArray());
        }
        assertEquals(200, resp.statusCode(), path + " answered " + resp.statusCode());
        return Turn.read(resp.body());
    }

    // --- responses (the server's own, only read) ----------------------------

    record Row(String seen, List<String> keys, String source) {
        static Row of(VectorSchemaRoot root) {
            assertEquals(1, root.getRowCount(), "one row per input");
            String keys = string(root, "keys");
            return new Row(string(root, "seen"), keys.isEmpty() ? List.of() : List.of(keys.split(",")),
                    string(root, "source"));
        }

        private static String string(VectorSchemaRoot root, String column) {
            return new String(((VarCharVector) root.getVector(column)).get(0), StandardCharsets.UTF_8);
        }
    }

    record Tokens(String cursor, String call) {
        Map<String, String> asMetadata() {
            Map<String, String> md = new LinkedHashMap<>();
            if (cursor != null) md.put(Metadata.STREAM_STATE, cursor);
            if (call != null) md.put(Metadata.CALL_STATE, call);
            return md;
        }

        /** A continuation re-mints only the cursor; the call token is kept from /init. */
        Tokens advancedBy(Tokens next) {
            return new Tokens(next.cursor != null ? next.cursor : cursor, next.call != null ? next.call : call);
        }
    }

    record Turn(List<Row> rows, Tokens tokens) {
        Row single() {
            assertEquals(1, rows.size(), "expected exactly one data batch; got " + rows);
            return rows.get(0);
        }

        static Turn read(byte[] body) throws Exception {
            List<Row> rows = new ArrayList<>();
            String cursor = null;
            String call = null;
            try (IpcStreamReader r = new IpcStreamReader(new ByteArrayInputStream(body), Allocators.root())) {
                Map<String, String> md;
                while ((md = r.readNextBatch()) != null) {
                    int rowCount = r.root().getRowCount();
                    if (Wire.classify(rowCount, md) == Wire.BatchKind.ERROR) throw Wire.errorFromMetadata(md);
                    if (md.containsKey(Metadata.STREAM_STATE)) cursor = md.get(Metadata.STREAM_STATE);
                    if (md.containsKey(Metadata.CALL_STATE)) call = md.get(Metadata.CALL_STATE);
                    if (rowCount > 0) rows.add(Row.of(r.root()));
                }
            }
            return new Turn(rows, new Tokens(cursor, call));
        }
    }

    private static byte[] utf8(String s) { return s.getBytes(StandardCharsets.UTF_8); }

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
