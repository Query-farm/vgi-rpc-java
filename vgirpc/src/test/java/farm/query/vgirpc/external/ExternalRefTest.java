// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.external;

import com.github.luben.zstd.Zstd;
import com.sun.net.httpserver.HttpServer;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.RpcConnection;
import farm.query.vgirpc.RpcError;
import farm.query.vgirpc.RpcServer;
import farm.query.vgirpc.ServiceIntrospector;
import farm.query.vgirpc.log.Message;
import farm.query.vgirpc.marshal.Marshalling;
import farm.query.vgirpc.transport.RpcTransport;
import farm.query.vgirpc.wire.Allocators;
import farm.query.vgirpc.wire.IpcStreamReader;
import farm.query.vgirpc.wire.IpcStreamWriter;
import farm.query.vgirpc.wire.Metadata;
import farm.query.vgirpc.wire.Wire;
import org.apache.arrow.vector.VectorSchemaRoot;
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
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pre-published {@link ExternalRef} results: the value type's validation,
 * {@link Externalizer#publishExternal}, and the unary dispatcher answering with
 * a ref's pointer over a byte-stream (pipe) transport. The HTTP half lives in
 * {@code http/ExternalRefHttpTest}.
 */
final class ExternalRefTest {

    /** A service whose methods answer with whatever ref the test installs. */
    public interface RefService {
        String fetch(String key, CallContext ctx);
        long counter(CallContext ctx);
        void nothing(CallContext ctx);
        String plain(String value);
    }

    static final class RefImpl implements RefService {
        final Map<String, ExternalRef> refs = new ConcurrentHashMap<>();
        @Override public String fetch(String key, CallContext ctx) {
            ExternalRef ref = refs.get(key);
            if (ref == null) return "inline:" + key;
            ctx.respondWithExternalRef(ref);
            return null;   // ignored -- and null for a non-nullable result is fine
        }
        @Override public long counter(CallContext ctx) {
            ctx.respondWithExternalRef(refs.get("counter"));
            return 0L;
        }
        @Override public void nothing(CallContext ctx) {
            ctx.respondWithExternalRef(ExternalRef.of("http://127.0.0.1:1/never"));
        }
        @Override public String plain(String value) { return value; }
    }

    private static final Schema FETCH_RESULT =
            ServiceIntrospector.describe(RefService.class).get("fetch").resultSchema();
    private static final Schema FETCH_PARAMS =
            ServiceIntrospector.describe(RefService.class).get("fetch").paramsSchema();
    private static final Schema COUNTER_RESULT =
            ServiceIntrospector.describe(RefService.class).get("counter").resultSchema();

    private HttpServer http;
    private int port;
    private final Map<String, byte[]> objects = new ConcurrentHashMap<>();
    private final Map<String, String> encodings = new ConcurrentHashMap<>();
    private final AtomicInteger uploads = new AtomicInteger();

    @BeforeEach
    void start() throws Exception {
        http = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        http.createContext("/obj/", exchange -> {
            String key = exchange.getRequestURI().getPath().substring("/obj/".length());
            byte[] body = objects.get(key);
            if (body == null) { exchange.sendResponseHeaders(404, -1); exchange.close(); return; }
            String enc = encodings.get(key);
            if (enc != null) exchange.getResponseHeaders().add("Content-Encoding", enc);
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.getResponseBody().close();
        });
        http.start();
        port = http.getAddress().getPort();
    }

    @AfterEach
    void stop() { if (http != null) http.stop(0); }

    /** In-memory storage served by the test HTTP server; counts uploads. */
    private final class MapStorage implements ExternalStorage {
        @Override
        public URI upload(byte[] body, String contentEncoding) {
            uploads.incrementAndGet();
            String key = UUID.randomUUID().toString();
            objects.put(key, body);
            if (contentEncoding != null) encodings.put(key, contentEncoding);
            return URI.create("http://127.0.0.1:" + port + "/obj/" + key);
        }
    }

    private byte[] object(String url) {
        return objects.get(url.substring(url.lastIndexOf('/') + 1));
    }

    private static String sha256Hex(byte[] b) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(b));
    }

    private static ExternalLocationConfig clientConfig() {
        return ExternalLocationConfig.builder()
                .urlValidator(ExternalLocationConfig.permissiveValidator())
                .build();
    }

    // --- ExternalRef value ----------------------------------------------------

    @Test
    void validates_url_and_digest_shape() {
        String good = "0123456789abcdef".repeat(4);
        assertEquals(good, new ExternalRef("https://x/y", good).sha256());
        assertNull(ExternalRef.of("https://x/y").sha256());
        assertThrows(IllegalArgumentException.class, () -> new ExternalRef("", null));
        assertThrows(NullPointerException.class, () -> new ExternalRef(null, null));
        assertThrows(IllegalArgumentException.class, () -> new ExternalRef("u", good.toUpperCase()));
        assertThrows(IllegalArgumentException.class, () -> new ExternalRef("u", good.substring(1)));
        assertThrows(IllegalArgumentException.class, () -> new ExternalRef("u", good + "0"));
        assertThrows(IllegalArgumentException.class, () -> new ExternalRef("u", "g" + good.substring(1)));
    }

    @Test
    void pointer_metadata_carries_the_digest_only_when_present() {
        String digest = "ab".repeat(32);
        assertEquals(Map.of(Metadata.LOCATION, "https://x/y", Metadata.LOCATION_SHA256, digest),
                new ExternalRef("https://x/y", digest).pointerMetadata());
        assertEquals(Map.of(Metadata.LOCATION, "https://x/y"),
                ExternalRef.of("https://x/y").pointerMetadata());
    }

    @Test
    void respond_with_ref_is_refused_outside_a_unary_dispatch() {
        CallContext ctx = new CallContext(null, m -> { }, null, "srv", "m", "P", "");
        assertThrows(IllegalStateException.class,
                () -> ctx.respondWithExternalRef(ExternalRef.of("https://x/y")));
    }

    // --- publishExternal ------------------------------------------------------

    @Test
    void publish_uploads_once_and_names_the_raw_stream_digest() throws Exception {
        ExternalRef ref = Externalizer.publishExternal(FETCH_RESULT, "hello", new MapStorage(), null, true);
        assertEquals(1, uploads.get());
        byte[] raw = object(ref.url());
        assertNotNull(raw);
        assertNull(encodings.get(ref.url().substring(ref.url().lastIndexOf('/') + 1)));
        assertEquals(sha256Hex(raw), ref.sha256());
        // The object is the result stream: result schema + exactly one 1-row batch.
        try (IpcStreamReader r = new IpcStreamReader(new ByteArrayInputStream(raw), Allocators.root())) {
            assertEquals(FETCH_RESULT, r.schema());
            assertNotNull(r.readNextBatch());
            assertEquals(1, r.root().getRowCount());
            assertEquals("hello", r.root().getVector("result").getObject(0).toString());
            assertNull(r.readNextBatch());
        }
    }

    @Test
    void publish_without_digest_omits_it() throws Exception {
        ExternalRef ref = Externalizer.publishExternal(FETCH_RESULT, "x", new MapStorage(), null, false);
        assertNull(ref.sha256());
        assertFalse(ref.pointerMetadata().containsKey(Metadata.LOCATION_SHA256));
    }

    @Test
    void publish_compresses_like_the_per_call_externalizer() throws Exception {
        ExternalRef ref = Externalizer.publishExternal(
                FETCH_RESULT, "zz".repeat(500), new MapStorage(), ExternalLocationConfig.Compression.zstd(), true);
        String key = ref.url().substring(ref.url().lastIndexOf('/') + 1);
        assertEquals("zstd", encodings.get(key));
        byte[] raw = Zstd.decompress(objects.get(key), 1 << 20);
        assertEquals(sha256Hex(raw), ref.sha256(), "the digest covers the raw, pre-compression bytes");
    }

    @Test
    void publish_matches_the_per_call_externalizer_byte_for_byte() throws Exception {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("result", "same bytes");
        try (Marshalling.EncodedRow enc = Marshalling.encodeRowForWire(FETCH_RESULT, row, Allocators.root())) {
            ExternalRef ref = Externalizer.publishExternal(enc.root(), new MapStorage(), null);
            ExternalLocationConfig cfg = ExternalLocationConfig.builder()
                    .storage(new MapStorage()).thresholdBytes(0)
                    .urlValidator(ExternalLocationConfig.permissiveValidator()).build();
            Externalizer.Pointer ptr = Externalizer.maybeExternalize(enc.root(), null, cfg, FETCH_RESULT);
            assertNotNull(ptr);
            try (VectorSchemaRoot pointer = ptr.root()) {
                assertEquals(0, pointer.getRowCount());
                assertArrayEquals(object(ref.url()), object(ptr.customMetadata().get(Metadata.LOCATION)));
                assertEquals(ref.sha256(), ptr.customMetadata().get(Metadata.LOCATION_SHA256));
            }
        }
    }

    @Test
    void publish_requires_exactly_one_row() throws Exception {
        try (VectorSchemaRoot empty = VectorSchemaRoot.create(FETCH_RESULT, Allocators.root())) {
            empty.allocateNew();
            empty.setRowCount(0);
            assertThrows(IllegalArgumentException.class,
                    () -> Externalizer.publishExternal(empty, new MapStorage(), null));
        }
        assertEquals(0, uploads.get());
    }

    // --- dispatcher (byte-stream transport) -----------------------------------

    @Test
    @Timeout(30)
    void dispatcher_writes_the_pointer_without_storage_or_upload() throws Exception {
        RefImpl impl = new RefImpl();
        ExternalRef ref = Externalizer.publishExternal(FETCH_RESULT, "hi", new MapStorage(), null, true);
        impl.refs.put("k", ref);
        RpcServer server = new RpcServer(RefService.class, impl);   // no external config at all
        uploads.set(0);

        List<Batch> response = serveRaw(server, request("fetch", Map.of("key", "k")));
        assertEquals(1, response.size());
        Batch b = response.get(0);
        assertEquals(0, b.rows(), "a ref always answers with a zero-row pointer");
        assertEquals(FETCH_RESULT, b.schema());
        assertEquals(ref.url(), b.meta().get(Metadata.LOCATION));
        assertEquals(ref.sha256(), b.meta().get(Metadata.LOCATION_SHA256));
        assertEquals(0, uploads.get(), "returning a ref uploads nothing");
    }

    @Test
    @Timeout(30)
    void dispatcher_omits_the_digest_key_for_a_ref_without_one() throws Exception {
        RefImpl impl = new RefImpl();
        impl.refs.put("k", Externalizer.publishExternal(FETCH_RESULT, "hi", new MapStorage(), null, false));
        List<Batch> response = serveRaw(new RpcServer(RefService.class, impl), request("fetch", Map.of("key", "k")));
        assertEquals(1, response.size());
        assertNotNull(response.get(0).meta().get(Metadata.LOCATION));
        assertFalse(response.get(0).meta().containsKey(Metadata.LOCATION_SHA256));
    }

    @Test
    @Timeout(30)
    void a_ref_ignores_storage_config_and_threshold() throws Exception {
        RefImpl impl = new RefImpl();
        ExternalRef ref = Externalizer.publishExternal(FETCH_RESULT, "tiny", new MapStorage(), null, true);
        impl.refs.put("k", ref);
        RpcServer server = new RpcServer(RefService.class, impl);
        // Storage configured with a threshold nothing reaches: an ordinary
        // result stays inline, a ref still goes as its pointer.
        server.setExternalConfig(ExternalLocationConfig.builder()
                .storage(new MapStorage()).thresholdBytes(1L << 30)
                .urlValidator(ExternalLocationConfig.permissiveValidator()).build());
        uploads.set(0);
        List<Batch> response = serveRaw(server, request("fetch", Map.of("key", "k")));
        assertEquals(ref.url(), response.get(0).meta().get(Metadata.LOCATION));
        assertEquals(0, uploads.get());
        List<Batch> inline = serveRaw(server, request("fetch", Map.of("key", "absent")));
        assertEquals(1, inline.get(0).rows());
        assertFalse(inline.get(0).meta().containsKey(Metadata.LOCATION));
    }

    @Test
    @Timeout(30)
    void a_void_method_cannot_answer_with_a_ref() throws Exception {
        List<Batch> response = serveRaw(new RpcServer(RefService.class, new RefImpl()), request("nothing", Map.of()));
        assertEquals(1, response.size());
        assertEquals(Wire.BatchKind.ERROR, Wire.classify(0, response.get(0).meta()));
    }

    @Test
    @Timeout(30)
    void client_resolves_a_ref_over_a_pipe() throws Exception {
        RefImpl impl = new RefImpl();
        impl.refs.put("with", Externalizer.publishExternal(FETCH_RESULT, "digest", new MapStorage(), null, true));
        impl.refs.put("without", Externalizer.publishExternal(FETCH_RESULT, "nodigest", new MapStorage(), null, false));
        impl.refs.put("counter", Externalizer.publishExternal(COUNTER_RESULT, 42L, new MapStorage(), null, true));
        impl.refs.put("zstd", Externalizer.publishExternal(FETCH_RESULT, "squeezed".repeat(100), new MapStorage(),
                ExternalLocationConfig.Compression.zstd(), true));
        withPipe(new RpcServer(RefService.class, impl), proxy -> {
            int before = uploads.get();
            assertEquals("digest", proxy.fetch("with", null));
            assertEquals("nodigest", proxy.fetch("without", null));
            assertEquals(42L, proxy.counter(null));
            assertEquals("squeezed".repeat(100), proxy.fetch("zstd", null));
            assertEquals("inline:absent", proxy.fetch("absent", null));
            assertEquals(before, uploads.get(), "no call uploaded anything");
        });
    }

    @Test
    @Timeout(30)
    void client_rejects_a_ref_whose_digest_does_not_match() throws Exception {
        RefImpl impl = new RefImpl();
        ExternalRef real = Externalizer.publishExternal(FETCH_RESULT, "x", new MapStorage(), null, true);
        impl.refs.put("bad", new ExternalRef(real.url(), "0".repeat(64)));
        withPipe(new RpcServer(RefService.class, impl), proxy ->
                assertThrows(RpcError.class, () -> proxy.fetch("bad", null)));
    }

    // --- helpers --------------------------------------------------------------

    private record Batch(Schema schema, int rows, Map<String, String> meta) {}

    private static byte[] request(String method, Map<String, Object> args) throws Exception {
        Schema params = ServiceIntrospector.describe(RefService.class).get(method).paramsSchema();
        Map<String, String> meta = Wire.requestMetadata(method);
        meta.put(Metadata.PROTOCOL, ServiceIntrospector.protocolName(RefService.class));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (IpcStreamWriter w = new IpcStreamWriter(out)) {
            w.writeSchema(params);
            if (params.getFields().isEmpty()) {
                try (VectorSchemaRoot zero = VectorSchemaRoot.create(params, Allocators.root())) {
                    zero.setRowCount(1);
                    w.writeBatch(zero, meta);
                }
            } else {
                try (Marshalling.EncodedRow enc = Marshalling.encodeRowForWire(
                        params, new LinkedHashMap<>(args), Allocators.root())) {
                    w.writeBatch(enc.root(), meta, enc.provider());
                }
            }
        }
        return out.toByteArray();
    }

    private static List<Batch> serveRaw(RpcServer rpc, byte[] body) throws Exception {
        ByteArrayOutputStream response = new ByteArrayOutputStream();
        RpcTransport transport = new RpcTransport() {
            private final InputStream in = new ByteArrayInputStream(body);
            @Override public InputStream reader() { return in; }
            @Override public OutputStream writer() { return response; }
            @Override public void close() { }
        };
        rpc.serveOne(transport);
        List<Batch> out = new ArrayList<>();
        try (IpcStreamReader r = new IpcStreamReader(
                new ByteArrayInputStream(response.toByteArray()), Allocators.root())) {
            Map<String, String> md;
            while ((md = r.readNextBatch()) != null) {
                out.add(new Batch(r.schema(), r.root().getRowCount(), md));
            }
        }
        return out;
    }

    private interface ProxyBody { void run(RefService proxy) throws Exception; }

    private void withPipe(RpcServer server, ProxyBody body) throws Exception {
        PipedOutputStream clientOut = new PipedOutputStream();
        PipedInputStream serverIn = new PipedInputStream(clientOut, 1 << 16);
        PipedOutputStream serverOut = new PipedOutputStream();
        PipedInputStream clientIn = new PipedInputStream(serverOut, 1 << 16);
        RpcTransport serverTransport = new StreamTransport(serverIn, serverOut);
        RpcTransport clientTransport = new StreamTransport(clientIn, clientOut);
        Thread serverThread = new Thread(() -> server.serve(serverTransport), "rpc-server");
        serverThread.setDaemon(true);
        serverThread.start();
        Consumer<Message> onLog = m -> { };
        try (RpcConnection conn = new RpcConnection(clientTransport, onLog, clientConfig())) {
            body.run(conn.proxy(RefService.class));
        } finally {
            clientTransport.close();
            serverThread.join(2000);
        }
    }

    private static final class StreamTransport implements RpcTransport {
        private final InputStream in;
        private final OutputStream out;
        StreamTransport(InputStream in, OutputStream out) { this.in = in; this.out = out; }
        @Override public InputStream reader() { return in; }
        @Override public OutputStream writer() { return out; }
        @Override public void close() {
            try { out.flush(); } catch (Exception ignore) { }
            try { out.close(); } catch (Exception ignore) { }
            try { in.close(); } catch (Exception ignore) { }
        }
    }

    @Test
    void params_schema_is_unchanged_by_the_context_parameter() {
        // CallContext is off-wire: the ref-answering method's params are just its key.
        assertEquals(1, FETCH_PARAMS.getFields().size());
        assertTrue(FETCH_PARAMS.findField("key") != null);
    }
}
