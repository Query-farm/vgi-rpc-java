// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.RpcError;
import farm.query.vgirpc.RpcServer;
import farm.query.vgirpc.ServiceIntrospector;
import farm.query.vgirpc.external.ExternalLocationConfig;
import farm.query.vgirpc.external.ExternalRef;
import farm.query.vgirpc.external.ExternalStorage;
import farm.query.vgirpc.external.Externalizer;
import farm.query.vgirpc.marshal.Marshalling;
import farm.query.vgirpc.wire.Allocators;
import farm.query.vgirpc.wire.IpcStreamReader;
import farm.query.vgirpc.wire.IpcStreamWriter;
import farm.query.vgirpc.wire.Metadata;
import farm.query.vgirpc.wire.Wire;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The HTTP unary path answers a {@link CallContext#respondWithExternalRef} call
 * with the ref's pointer: zero rows, {@code vgi_rpc.location} (+ digest), no
 * upload, and no charge against {@code max_externalized_response_bytes}.
 */
final class ExternalRefHttpTest {

    /** Answers with the ref the test installed for {@code key}. */
    public interface Published {
        String fetch(String key, CallContext ctx);
        String plain(String value);
    }

    static final class PublishedImpl implements Published {
        final Map<String, ExternalRef> refs = new ConcurrentHashMap<>();
        @Override public String fetch(String key, CallContext ctx) {
            ctx.respondWithExternalRef(refs.get(key));
            return null;
        }
        @Override public String plain(String value) { return value; }
    }

    private static final Schema RESULT =
            ServiceIntrospector.describe(Published.class).get("fetch").resultSchema();
    private static final String PROTOCOL = ServiceIntrospector.protocolName(Published.class);

    private com.sun.net.httpserver.HttpServer storageHttp;
    private int storagePort;
    private final Map<String, byte[]> objects = new ConcurrentHashMap<>();
    private final AtomicInteger uploads = new AtomicInteger();
    private HttpServer server;
    private HttpRpcConnection connection;
    private final PublishedImpl impl = new PublishedImpl();

    @BeforeEach
    void startStorage() throws Exception {
        storageHttp = com.sun.net.httpserver.HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        storageHttp.createContext("/obj/", exchange -> {
            byte[] body = objects.get(exchange.getRequestURI().getPath().substring("/obj/".length()));
            if (body == null) { exchange.sendResponseHeaders(404, -1); exchange.close(); return; }
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.getResponseBody().close();
        });
        storageHttp.start();
        storagePort = storageHttp.getAddress().getPort();
    }

    @AfterEach
    void stop() throws Exception {
        if (connection != null) connection.close();
        if (server != null) server.stop();
        if (storageHttp != null) storageHttp.stop(0);
    }

    private final class MapStorage implements ExternalStorage {
        @Override public URI upload(byte[] body, String contentEncoding) {
            uploads.incrementAndGet();
            String key = UUID.randomUUID().toString();
            objects.put(key, body);
            return URI.create("http://127.0.0.1:" + storagePort + "/obj/" + key);
        }
    }

    private String start(Consumer<HttpServer.Config.Builder> configure, ExternalLocationConfig serverExternal)
            throws Exception {
        HttpServer.Config.Builder config = HttpServer.Config.builder().prefix("/vgi");
        configure.accept(config);
        RpcServer rpc = new RpcServer(Published.class, impl);
        if (serverExternal != null) rpc.setExternalConfig(serverExternal);
        server = new HttpServer(rpc, config.build());
        server.start();
        return "http://127.0.0.1:" + server.port() + "/vgi";
    }

    private Published proxy(String base) {
        connection = HttpRpcConnection.builder(base)
                .externalLocation(ExternalLocationConfig.builder()
                        .urlValidator(ExternalLocationConfig.permissiveValidator()).build())
                .build();
        return connection.proxy(Published.class);
    }

    @Test
    @Timeout(30)
    void unaryResponseIsTheRefsZeroRowPointer() throws Exception {
        ExternalRef with = Externalizer.publishExternal(RESULT, "hi", new MapStorage(), null, true);
        ExternalRef without = Externalizer.publishExternal(RESULT, "hi", new MapStorage(), null, false);
        impl.refs.put("with", with);
        impl.refs.put("without", without);
        String base = start(b -> { }, null);   // no external storage on the server
        uploads.set(0);

        Map<String, String> md = onlyDataBatch(post(base, "with"));
        assertEquals(with.url(), md.get(Metadata.LOCATION));
        assertEquals(with.sha256(), md.get(Metadata.LOCATION_SHA256));

        md = onlyDataBatch(post(base, "without"));
        assertEquals(without.url(), md.get(Metadata.LOCATION));
        assertFalse(md.containsKey(Metadata.LOCATION_SHA256));
        assertEquals(0, uploads.get(), "answering with a ref uploads nothing");
    }

    @Test
    @Timeout(30)
    void refIsNotChargedToTheExternalizedResponseCap() throws Exception {
        String big = "x".repeat(64 * 1024);
        impl.refs.put("big", Externalizer.publishExternal(RESULT, big, new MapStorage(), null, true));
        String base = start(b -> b.maxExternalizedResponseBytes(16),
                ExternalLocationConfig.builder().storage(new MapStorage()).thresholdBytes(1)
                        .urlValidator(ExternalLocationConfig.permissiveValidator()).build());
        uploads.set(0);
        Published proxy = proxy(base);
        assertEquals(big, proxy.fetch("big", null));
        assertEquals(0, uploads.get());
        // Control: the cap is live -- the same payload as an ordinary result
        // is refused before it is uploaded.
        RpcError refused = assertThrows(RpcError.class, () -> proxy.plain(big));
        assertTrue(refused.getMessage().contains("max_externalized_response_bytes"), refused.getMessage());
        assertEquals(0, uploads.get());
    }

    // --- helpers --------------------------------------------------------------

    private static byte[] post(String base, String key) throws Exception {
        Schema params = ServiceIntrospector.describe(Published.class).get("fetch").paramsSchema();
        Map<String, String> meta = Wire.requestMetadata("fetch");
        meta.put(Metadata.PROTOCOL, PROTOCOL);
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        try (IpcStreamWriter w = new IpcStreamWriter(body)) {
            w.writeSchema(params);
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("key", key);
            try (Marshalling.EncodedRow enc = Marshalling.encodeRowForWire(params, row, Allocators.root())) {
                w.writeBatch(enc.root(), meta, enc.provider());
            }
        }
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()) {
            HttpResponse<byte[]> resp = client.send(
                    HttpRequest.newBuilder(URI.create(base + "/" + PROTOCOL + "/fetch"))
                            .timeout(Duration.ofSeconds(20))
                            .header("Content-Type", HttpServer.ARROW_CONTENT_TYPE)
                            .POST(HttpRequest.BodyPublishers.ofByteArray(body.toByteArray()))
                            .build(),
                    HttpResponse.BodyHandlers.ofByteArray());
            assertEquals(200, resp.statusCode());
            return resp.body();
        }
    }

    /** The single non-log batch of a unary response; asserts it is a zero-row pointer. */
    private static Map<String, String> onlyDataBatch(byte[] response) throws Exception {
        List<Map<String, String>> data = new ArrayList<>();
        try (IpcStreamReader r = new IpcStreamReader(new ByteArrayInputStream(response), Allocators.root())) {
            assertEquals(RESULT, r.schema());
            Map<String, String> md;
            while ((md = r.readNextBatch()) != null) {
                if (md.containsKey(Metadata.LOG_LEVEL)) continue;
                assertEquals(0, r.root().getRowCount(), "a ref always answers with a zero-row pointer");
                data.add(md);
            }
        }
        assertEquals(1, data.size());
        assertNotNull(data.get(0).get(Metadata.LOCATION));
        return data.get(0);
    }
}
