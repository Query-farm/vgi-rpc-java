// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.ExchangeState;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.ProducerState;
import farm.query.vgirpc.RpcServer;
import farm.query.vgirpc.RpcStream;
import farm.query.vgirpc.marshal.Marshalling;
import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.schema.StreamHeader;
import farm.query.vgirpc.wire.Allocators;
import farm.query.vgirpc.wire.IpcStreamReader;
import farm.query.vgirpc.wire.IpcStreamWriter;
import farm.query.vgirpc.wire.Metadata;
import farm.query.vgirpc.wire.Wire;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code X-VGI-RPC-Error} says "read this body as an error instead of as the
 * reply you asked for" — which is only true when the error <em>replaced</em>
 * the body.
 *
 * <p>A producer that raises inside {@code produce} during {@code /init} is the
 * one case where it did not. That response body is a <em>sequence</em> of Arrow
 * IPC streams: the declared stream header first, then the producer's own
 * stream, with the EXCEPTION batch written into the second. A client that takes
 * the flag as licence to switch to its unary error reader reads the first
 * stream, hits its end-of-stream, never reaches the exception, and reports a
 * generic transport failure with the worker's message discarded. Flagging that
 * response is therefore worse than not flagging it, and the Python reference's
 * producer loop accordingly leaves the response status at a plain 200.
 *
 * <p>The shared conformance suite's {@code TestErrorHeader} cannot see this:
 * it posts unary bodies, and every unary error path replaces the body. So the
 * assertions live here, paired with the two directions that must keep the flag
 * — an init-method raise, and an exchange turn that raises — so the fix cannot
 * be widened into "never flag a stream".
 */
final class InBandStreamErrorHeaderTest {

    private static final String ARROW = "application/vnd.apache.arrow.stream";

    private static final String PRODUCE_FAILURE = "producer raised on its first tick";
    private static final String INIT_FAILURE = "the stream method itself refused";
    private static final String EXCHANGE_FAILURE = "exchange turn raised";

    static final Schema OUT_SCHEMA = new Schema(List.of(
            new Field("n", FieldType.notNullable(new ArrowType.Int(64, true)), null)));
    static final Schema IN_SCHEMA = new Schema(List.of(
            new Field("v", FieldType.notNullable(new ArrowType.Int(64, true)), null)));

    /** Header record: its stream is what a unary reader stops inside. */
    public record Head(String tag) implements ArrowSerializableRecord {}

    /** Raises in-band, after the header stream has already been written. */
    public static final class RaisingProducer extends ProducerState {
        public RaisingProducer() {}
        @Override public void produce(OutputCollector out, CallContext ctx) {
            throw new IllegalStateException(PRODUCE_FAILURE);
        }
    }

    /** Emits one row and finishes in the same turn — the success control. */
    public static final class OkProducer extends ProducerState {
        public OkProducer() {}
        @Override public void produce(OutputCollector out, CallContext ctx) {
            VectorSchemaRoot root = VectorSchemaRoot.create(OUT_SCHEMA, Allocators.root());
            root.allocateNew();
            ((BigIntVector) root.getVector(0)).setSafe(0, 7L);
            root.setRowCount(1);
            out.emit(root);
            out.finish();
        }
    }

    /** Raises on the {@code /exchange} turn, where the error replaces the body. */
    public static final class RaisingExchange extends ExchangeState {
        public RaisingExchange() {}
        @Override public void exchange(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
            throw new IllegalStateException(EXCHANGE_FAILURE);
        }
    }

    public interface FailingStreamService {
        @StreamHeader(Head.class)
        RpcStream<RaisingProducer> raise_in_produce(long n);

        @StreamHeader(Head.class)
        RpcStream<OkProducer> ok_produce(long n);

        RpcStream<RaisingProducer> raise_in_method(long n);

        RpcStream<RaisingExchange> exchange_raises(long n);
    }

    public static final class Impl implements FailingStreamService {
        @Override public RpcStream<RaisingProducer> raise_in_produce(long n) {
            return RpcStream.producer(OUT_SCHEMA, new RaisingProducer(), new Head("header-before-the-failure"));
        }
        @Override public RpcStream<OkProducer> ok_produce(long n) {
            return RpcStream.producer(OUT_SCHEMA, new OkProducer(), new Head("header-before-the-row"));
        }
        @Override public RpcStream<RaisingProducer> raise_in_method(long n) {
            throw new IllegalStateException(INIT_FAILURE);
        }
        @Override public RpcStream<RaisingExchange> exchange_raises(long n) {
            return RpcStream.exchange(IN_SCHEMA, OUT_SCHEMA, new RaisingExchange());
        }
    }

    private RpcServer rpc;
    private HttpServer server;

    @BeforeEach
    void start() throws Exception {
        rpc = new RpcServer(FailingStreamService.class, new Impl());
        server = new HttpServer(rpc, HttpServer.Config.builder()
                .prefix("/vgi").supportedEncodings(List.of()).build());
        server.start();
    }

    @AfterEach
    void stop() throws Exception {
        if (server != null) server.stop();
    }

    // --- the regression ---------------------------------------------------

    @Test
    void producerRaisingInBandDoesNotFlagTheResponse() throws Exception {
        HttpResponse<byte[]> resp = post("raise_in_produce/init", initRequest("raise_in_produce"));
        assertEquals(200, resp.statusCode());

        // The shape that makes the flag a lie: two concatenated IPC streams,
        // with nothing in the first one to tell a reader anything went wrong.
        List<List<Map<String, String>>> streams = streams(resp.body());
        assertEquals(2, streams.size(),
                "a producer /init with a declared header answers header-stream + producer-stream");
        assertTrue(exception(streams.get(0)) == null,
                "the header stream carries no error; a reader that stops at its EOS learns nothing");
        String message = exception(streams.get(1));
        assertNotNull(message, "the producer stream must carry the EXCEPTION batch");
        assertTrue(message.contains(PRODUCE_FAILURE), message);

        assertFalse(errorFlag(resp),
                "X-VGI-RPC-Error on an in-band producer raise sends clients to a unary error "
                        + "reader that stops at the header stream's EOS and never sees the exception");
    }

    // --- the directions that must keep the flag ---------------------------

    @Test
    void initMethodRaiseStillFlagsTheResponse() throws Exception {
        HttpResponse<byte[]> resp = post("raise_in_method/init", initRequest("raise_in_method"));
        assertEquals(200, resp.statusCode());

        List<List<Map<String, String>>> streams = streams(resp.body());
        assertEquals(1, streams.size(), "an init-method raise replaces the body with one error stream");
        String message = exception(streams.get(0));
        assertNotNull(message);
        assertTrue(message.contains(INIT_FAILURE), message);

        assertTrue(errorFlag(resp),
                "the error replaced the body, so the flag is exactly true and clients rely on it");
    }

    @Test
    void exchangeTurnRaiseStillFlagsTheResponse() throws Exception {
        HttpResponse<byte[]> init = post("exchange_raises/init", initRequest("exchange_raises"));
        assertEquals(200, init.statusCode());
        assertFalse(errorFlag(init));
        Map<String, String> tokens = tokens(init.body());

        HttpResponse<byte[]> resp = post("exchange_raises/exchange",
                exchangeRequest("exchange_raises", tokens));
        assertEquals(200, resp.statusCode());

        List<List<Map<String, String>>> streams = streams(resp.body());
        assertEquals(1, streams.size(), "an exchange raise replaces the body with one error stream");
        String message = exception(streams.get(0));
        assertNotNull(message);
        assertTrue(message.contains(EXCHANGE_FAILURE), message);

        assertTrue(errorFlag(resp), "the reference flags this one; only the producer loop is exempt");
    }

    @Test
    void successfulProducerInitIsUnflagged() throws Exception {
        HttpResponse<byte[]> resp = post("ok_produce/init", initRequest("ok_produce"));
        assertEquals(200, resp.statusCode());
        assertEquals(2, streams(resp.body()).size());
        assertTrue(exception(streams(resp.body()).get(1)) == null);
        assertFalse(errorFlag(resp));
    }

    // --- helpers ----------------------------------------------------------

    private static boolean errorFlag(HttpResponse<byte[]> resp) {
        return resp.headers().firstValue(HttpServer.RPC_ERROR_HEADER).isPresent();
    }

    private HttpResponse<byte[]> post(String path, byte[] body) throws Exception {
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()) {
            return client.send(HttpRequest.newBuilder(
                            URI.create("http://127.0.0.1:" + server.port() + "/vgi/" + path))
                    .timeout(Duration.ofSeconds(30))
                    .header("Content-Type", ARROW)
                    .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                    .build(), HttpResponse.BodyHandlers.ofByteArray());
        }
    }

    private byte[] initRequest(String method) throws Exception {
        Schema params = rpc.methods().get(method).paramsSchema();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (IpcStreamWriter w = new IpcStreamWriter(out);
             VectorSchemaRoot root = Marshalling.encodeRow(params, Map.of("n", 1L), Allocators.root())) {
            w.writeSchema(params);
            w.writeBatch(root, Wire.requestMetadata(method));
        }
        return out.toByteArray();
    }

    private static byte[] exchangeRequest(String method, Map<String, String> tokens) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (IpcStreamWriter w = new IpcStreamWriter(out);
             VectorSchemaRoot root = Marshalling.encodeRow(IN_SCHEMA, Map.of("v", 1L), Allocators.root())) {
            w.writeSchema(IN_SCHEMA);
            Map<String, String> md = Wire.requestMetadata(method);
            md.putAll(tokens);
            w.writeBatch(root, md);
        }
        return out.toByteArray();
    }

    /** The stream-state and call-state tokens an {@code /init} response hands back. */
    private static Map<String, String> tokens(byte[] body) throws Exception {
        Map<String, String> found = new LinkedHashMap<>();
        for (List<Map<String, String>> stream : streams(body)) {
            for (Map<String, String> md : stream) {
                if (md.containsKey(Metadata.STREAM_STATE)) {
                    found.put(Metadata.STREAM_STATE, md.get(Metadata.STREAM_STATE));
                }
                if (md.containsKey(Metadata.CALL_STATE)) {
                    found.put(Metadata.CALL_STATE, md.get(Metadata.CALL_STATE));
                }
            }
        }
        assertTrue(found.containsKey(Metadata.STREAM_STATE), "exchange /init must mint a cursor");
        return found;
    }

    /** Per-batch metadata, grouped by the IPC stream it arrived in. */
    private static List<List<Map<String, String>>> streams(byte[] body) throws Exception {
        List<List<Map<String, String>>> all = new ArrayList<>();
        ByteArrayInputStream in = new ByteArrayInputStream(body);
        while (in.available() > 0) {
            List<Map<String, String>> batches = new ArrayList<>();
            try (IpcStreamReader r = new IpcStreamReader(in, Allocators.root())) {
                Map<String, String> md;
                while ((md = r.readNextBatch()) != null) batches.add(md);
            }
            all.add(batches);
        }
        return all;
    }

    /** The EXCEPTION batch's message within one stream, or {@code null}. */
    private static String exception(List<Map<String, String>> stream) {
        for (Map<String, String> md : stream) {
            if ("EXCEPTION".equals(md.get(Metadata.LOG_LEVEL))) return md.get(Metadata.LOG_MESSAGE);
        }
        return null;
    }
}
