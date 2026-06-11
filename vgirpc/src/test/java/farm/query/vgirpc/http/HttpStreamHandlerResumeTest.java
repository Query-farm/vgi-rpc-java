// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.ProducerState;
import farm.query.vgirpc.RpcServer;
import farm.query.vgirpc.RpcStream;
import farm.query.vgirpc.marshal.Marshalling;
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
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Continuation-only stream resume across processes (mirrors the Python 0.20.0
 * {@code _HttpProxy.resume_stream} contract): a producer continuation token minted
 * by one handler must be honoured by a <em>fresh</em> handler sharing the same
 * token key, with no prior {@code /init} on that handler — the state class comes
 * from construction-time introspection of the implementation's generic return
 * type, not from an init-populated cache.
 */
class HttpStreamHandlerResumeTest {

    static final Schema COUNT_SCHEMA = new Schema(List.of(
            new Field("n", FieldType.notNullable(new ArrowType.Int(64, true)), null)));

    /** One value per turn: n, n+1, … limit-1, then finish. */
    public static final class CounterState extends ProducerState {
        long next;
        long limit;
        public CounterState() {}
        CounterState(long limit) { this.limit = limit; }
        @Override public void produce(OutputCollector out, CallContext ctx) {
            if (next >= limit) { out.finish(); return; }
            VectorSchemaRoot root = VectorSchemaRoot.create(COUNT_SCHEMA, Allocators.root());
            root.allocateNew();
            ((BigIntVector) root.getVector(0)).setSafe(0, next);
            root.setRowCount(1);
            out.emit(root);
            next++;
        }
    }

    public interface CounterService {
        RpcStream<? extends ProducerState> count_to(long limit);
    }

    public static final class CounterServiceImpl implements CounterService {
        // Concrete generic return: resolvable at handler construction.
        @Override public RpcStream<CounterState> count_to(long limit) {
            return RpcStream.producer(COUNT_SCHEMA, new CounterState(limit));
        }
    }

    /** Implementation that keeps the interface's wildcard: not statically resolvable. */
    public static final class WildcardImpl implements CounterService {
        @Override public RpcStream<? extends ProducerState> count_to(long limit) {
            return RpcStream.producer(COUNT_SCHEMA, new CounterState(limit));
        }
    }

    private static final byte[] KEY = new byte[32]; // shared zero key is fine for tests

    private static byte[] initRequest(RpcServer server, String method, Map<String, Object> kwargs)
            throws Exception {
        Schema params = server.methods().get(method).paramsSchema();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (IpcStreamWriter w = new IpcStreamWriter(out);
             VectorSchemaRoot root = Marshalling.encodeRow(params, kwargs, Allocators.root())) {
            w.writeSchema(params);
            w.writeBatch(root, Wire.requestMetadata(method));
        }
        return out.toByteArray();
    }

    private static byte[] continuationRequest(String method, String token) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (IpcStreamWriter w = new IpcStreamWriter(out)) {
            w.writeSchema(RpcStream.EMPTY_SCHEMA);
            Map<String, String> md = Wire.requestMetadata(method);
            md.put(Metadata.STREAM_STATE, token);
            Wire.writeZeroBatch(w, RpcStream.EMPTY_SCHEMA, md);
        }
        return out.toByteArray();
    }

    /** Parse a producer response: data row values + trailing continuation token (null when finished). */
    private record Turn(List<Long> values, String token, String error) {}

    private static Turn readTurn(byte[] response) throws Exception {
        List<Long> values = new ArrayList<>();
        String token = null;
        try (IpcStreamReader r = new IpcStreamReader(new ByteArrayInputStream(response), Allocators.root())) {
            Map<String, String> md;
            while ((md = r.readNextBatch()) != null) {
                if (md.containsKey(Metadata.LOG_LEVEL) && "EXCEPTION".equals(md.get(Metadata.LOG_LEVEL))) {
                    return new Turn(values, null, md.get(Metadata.LOG_MESSAGE));
                }
                VectorSchemaRoot root = r.root();
                if (root.getRowCount() == 0) {
                    if (md.containsKey(Metadata.STREAM_STATE)) token = md.get(Metadata.STREAM_STATE);
                    continue;
                }
                BigIntVector v = (BigIntVector) root.getVector(0);
                for (int i = 0; i < root.getRowCount(); i++) values.add(v.get(i));
            }
        }
        return new Turn(values, token, null);
    }

    @Test
    void continuationResumesOnFreshHandler() throws Exception {
        RpcServer serverA = new RpcServer(CounterService.class, new CounterServiceImpl());
        HttpStreamHandler handlerA = new HttpStreamHandler(serverA, KEY, 0, Long.MAX_VALUE);

        Turn first = readTurn(handlerA.handleInit("count_to", initRequest(serverA, "count_to", Map.of("limit", 3L))));
        assertNull(first.error());
        assertEquals(List.of(0L), first.values());
        assertNotNull(first.token(), "unfinished producer must mint a continuation token");

        // Fresh server + handler: same key, but no init ever served here.
        RpcServer serverB = new RpcServer(CounterService.class, new CounterServiceImpl());
        HttpStreamHandler handlerB = new HttpStreamHandler(serverB, KEY, 0, Long.MAX_VALUE);

        Turn second = readTurn(handlerB.handleExchange("count_to", continuationRequest("count_to", first.token())));
        assertNull(second.error());
        assertEquals(List.of(1L), second.values());
        assertNotNull(second.token());

        Turn third = readTurn(handlerB.handleExchange("count_to", continuationRequest("count_to", second.token())));
        assertEquals(List.of(2L), third.values());

        // The stream finishes on whichever handler holds the final token.
        Turn last = readTurn(handlerB.handleExchange("count_to", continuationRequest("count_to", third.token())));
        assertNull(last.error());
        assertTrue(last.values().isEmpty());
        assertNull(last.token(), "finished producer must not mint a token");
    }

    @Test
    void wildcardImplStillLearnsFromInit() throws Exception {
        RpcServer server = new RpcServer(CounterService.class, new WildcardImpl());
        HttpStreamHandler handler = new HttpStreamHandler(server, KEY, 0, Long.MAX_VALUE);

        // Fresh-handler continuation cannot resolve the state type for a wildcard impl…
        RpcServer serverA = new RpcServer(CounterService.class, new WildcardImpl());
        HttpStreamHandler handlerA = new HttpStreamHandler(serverA, KEY, 0, Long.MAX_VALUE);
        Turn first = readTurn(handlerA.handleInit("count_to", initRequest(serverA, "count_to", Map.of("limit", 2L))));
        assertNotNull(first.token());
        Turn refused = readTurn(handler.handleExchange("count_to", continuationRequest("count_to", first.token())));
        assertNotNull(refused.error());
        assertTrue(refused.error().contains("Cannot resolve state type"), refused.error());

        // …but the in-process path (init then exchange on the same handler) still works.
        Turn cont = readTurn(handlerA.handleExchange("count_to", continuationRequest("count_to", first.token())));
        assertNull(cont.error());
        assertEquals(List.of(1L), cont.values());
    }
}
