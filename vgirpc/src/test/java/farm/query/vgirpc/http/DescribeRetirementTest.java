// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import farm.query.vgirpc.Reflection;
import farm.query.vgirpc.RpcError;
import farm.query.vgirpc.RpcServer;
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
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code __describe__} is retired, and says so.
 *
 * <p>A stale caller used to get "This server does not implement the reserved method
 * '__describe__'". True, and useless: it cannot be told apart from "this server was built
 * without introspection", and the two need opposite fixes -- update the client, or reconfigure
 * the server. The C++ port spent real time on the first while reading an error describing the
 * second.
 *
 * <p>The refusal now names {@code vgi_rpc.Reflection.v1} and both of its entry points, so a
 * stale client is fixable from the error text alone. Only {@code __describe__} is special-cased;
 * every other reserved name keeps the plain capability answer, which is what a client probing
 * for an optional method needs.
 */
@Timeout(30)
final class DescribeRetirementTest {

    private static final String ARROW = "application/vnd.apache.arrow.stream";
    private static final String DESCRIBE = RpcServer.RETIRED_DESCRIBE_METHOD;

    public interface EchoService {
        String echo(String value);
    }

    static final class EchoImpl implements EchoService {
        @Override public String echo(String value) { return value; }
    }

    private HttpServer server;
    private String base;

    @BeforeEach
    void start() throws Exception {
        server = new HttpServer(new RpcServer(EchoService.class, new EchoImpl(), "srv-retired"),
                HttpServer.Config.builder().prefix("/vgi").build());
        server.start();
        base = "http://127.0.0.1:" + server.port() + "/vgi";
    }

    @AfterEach
    void stop() throws Exception {
        if (server != null) server.stop();
    }

    @Test
    void theServerNoLongerOffersDescribeAsAReservedMethod() {
        RpcServer rpc = new RpcServer(EchoService.class, new EchoImpl(), "srv");
        assertFalse(rpc.reservedMethodNames().contains(DESCRIBE),
                "__describe__ is retired; a server that still lists it will still be called");
    }

    @Test
    void theRefusalNamesTheReplacement() {
        String message = RpcServer.reservedMethodRefusal(DESCRIBE).getMessage();
        assertTrue(message.toLowerCase(java.util.Locale.ROOT).contains("retired"), message);
        assertTrue(message.contains(Reflection.PROTOCOL_NAME), message);
        // Both entry points, because naming the protocol alone leaves a caller to guess the
        // method names -- and guessing is what it was already doing.
        assertTrue(message.contains("list_protocols"), message);
        assertTrue(message.contains("describe"), message);
    }

    @Test
    void anotherReservedNameKeepsTheGenericAnswer() {
        // Only describe is special-cased. A client probing for an optional reserved method needs
        // the plain capability answer; a retirement notice for a method that was never retired
        // would send it looking for a migration that does not exist.
        String message = RpcServer.reservedMethodRefusal("__not_a_thing__").getMessage();
        assertFalse(message.toLowerCase(java.util.Locale.ROOT).contains("retired"), message);
        assertTrue(message.contains("__not_a_thing__"), message);
    }

    @Test
    void aRawDescribeRequestIsRefusedWithTheReplacement() {
        RpcError err = assertThrows(RpcError.class,
                () -> serveRaw(new RpcServer(EchoService.class, new EchoImpl(), "srv"),
                        request(DESCRIBE)));
        assertEquals("method_not_implemented", err.errorKind());
        assertTrue(err.getMessage().contains(Reflection.PROTOCOL_NAME), err.getMessage());
    }

    @Test
    void theHttpFlatRouteGivesTheSameRefusal() throws Exception {
        // The two transports must agree: a client that learns "retired, use reflection" on one
        // and "no such method" on the other draws the wrong conclusion from whichever it tried.
        HttpResponse<byte[]> resp = post("/" + DESCRIBE, request(DESCRIBE));
        RpcError err = assertThrows(RpcError.class, () -> decode(resp.body()));
        assertEquals("method_not_implemented", err.errorKind());
        assertTrue(err.getMessage().contains(Reflection.PROTOCOL_NAME), err.getMessage());
        assertTrue(err.getMessage().contains("list_protocols"), err.getMessage());
    }

    @Test
    void reflectionStillAnswersWhatDescribeUsedTo() throws Exception {
        // The refusal is only actionable if the thing it points at works.
        HttpResponse<byte[]> resp = post("/" + Reflection.PROTOCOL_NAME + "/list_protocols",
                request(Reflection.PROTOCOL_NAME, "list_protocols"));
        assertEquals(200, resp.statusCode());
        assertTrue(decode(resp.body()).containsKey("result"));
    }

    // --- helpers -------------------------------------------------------------

    private HttpResponse<byte[]> post(String path, byte[] body) throws Exception {
        try (HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5)).build()) {
            return client.send(HttpRequest.newBuilder(URI.create(base + path))
                    .timeout(Duration.ofSeconds(20))
                    .header("Content-Type", ARROW)
                    .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                    .build(), HttpResponse.BodyHandlers.ofByteArray());
        }
    }

    private static byte[] request(String method) throws Exception {
        return request(null, method);
    }

    private static byte[] request(String protocol, String method) throws Exception {
        Map<String, String> meta = Wire.requestMetadata(method);
        if (protocol != null) meta.put(Metadata.PROTOCOL, protocol);
        Schema params = new Schema(List.of());
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (IpcStreamWriter w = new IpcStreamWriter(out)) {
            w.writeSchema(params);
            try (VectorSchemaRoot zero = VectorSchemaRoot.create(params, Allocators.root())) {
                zero.setRowCount(1);
                w.writeBatch(zero, meta);
            }
        }
        return out.toByteArray();
    }

    private static void serveRaw(RpcServer rpc, byte[] body) throws Exception {
        ByteArrayOutputStream response = new ByteArrayOutputStream();
        RpcTransport transport = new RpcTransport() {
            private final InputStream in = new ByteArrayInputStream(body);
            @Override public InputStream reader() { return in; }
            @Override public OutputStream writer() { return response; }
            @Override public void close() { }
        };
        rpc.serveOne(transport);
        decode(response.toByteArray());
    }

    private static Map<String, Object> decode(byte[] body) throws Exception {
        try (IpcStreamReader r = new IpcStreamReader(new ByteArrayInputStream(body),
                Allocators.root())) {
            Map<String, String> md = r.readNextBatch();
            if (md == null) throw new IllegalStateException("server wrote no batch");
            if (Wire.classify(r.root().getRowCount(), md) == Wire.BatchKind.ERROR) {
                throw Wire.errorFromMetadata(md);
            }
            return new LinkedHashMap<>(
                    Marshalling.decodeRow(r.root(), r.dictionaryProvider(), r.wireSchema()));
        }
    }
}
