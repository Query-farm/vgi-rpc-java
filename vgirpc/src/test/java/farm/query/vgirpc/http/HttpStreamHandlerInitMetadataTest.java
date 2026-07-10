// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.ProducerState;
import farm.query.vgirpc.RpcServer;
import farm.query.vgirpc.RpcStream;
import farm.query.vgirpc.marshal.Marshalling;
import farm.query.vgirpc.wire.Allocators;
import farm.query.vgirpc.wire.IpcStreamReader;
import farm.query.vgirpc.wire.IpcStreamWriter;
import farm.query.vgirpc.wire.Wire;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A producer's first {@code process()} turn must see the {@code /init} request
 * batch's {@code custom_metadata}.
 *
 * <p>The subprocess transport delivers per-call signals (conditional-revalidation
 * validators such as {@code vgi.cache.if_none_match}, dynamic-filter updates) as
 * the tick's input-batch metadata. The stateless HTTP path used to hand the
 * producer a synthetic empty batch with no metadata, so a producer that reads
 * them silently behaved as if the client had sent none.
 */
class HttpStreamHandlerInitMetadataTest {

    private static final String PROBE_KEY = "vgi.cache.if_none_match";

    static final Schema ECHO_SCHEMA = new Schema(List.of(
            new Field("seen", FieldType.nullable(new ArrowType.Utf8()), null)));

    /** Emits one row: the value of {@link #PROBE_KEY} on its first tick, or {@code "<absent>"}. */
    public static final class EchoMetaState extends ProducerState {
        boolean done;

        public EchoMetaState() {}

        @Override public void produce(OutputCollector out, CallContext ctx) {
            emit(out, "<no-input-overload>");
        }

        @Override public void produce(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
            String seen = input.customMetadata().get(PROBE_KEY);
            emit(out, seen != null ? seen : "<absent>");
        }

        private void emit(OutputCollector out, String seen) {
            if (done) { out.finish(); return; }
            done = true;
            VectorSchemaRoot root = VectorSchemaRoot.create(ECHO_SCHEMA, Allocators.root());
            root.allocateNew();
            ((VarCharVector) root.getVector(0)).setSafe(0, new Text(seen));
            root.setRowCount(1);
            out.emit(root);
        }
    }

    public interface EchoService {
        RpcStream<EchoMetaState> echo_meta(long ignored);
    }

    public static final class EchoServiceImpl implements EchoService {
        @Override public RpcStream<EchoMetaState> echo_meta(long ignored) {
            return RpcStream.producer(ECHO_SCHEMA, new EchoMetaState());
        }
    }

    private static byte[] initRequest(RpcServer server, String method,
                                        Map<String, Object> kwargs, Map<String, String> extraMeta)
            throws Exception {
        Schema params = server.methods().get(method).paramsSchema();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (IpcStreamWriter w = new IpcStreamWriter(out);
             VectorSchemaRoot root = Marshalling.encodeRow(params, kwargs, Allocators.root())) {
            w.writeSchema(params);
            Map<String, String> md = Wire.requestMetadata(method);
            md.putAll(extraMeta);
            w.writeBatch(root, md);
        }
        return out.toByteArray();
    }

    /** First data row of a producer response. */
    private static String firstRow(byte[] response) throws Exception {
        try (IpcStreamReader r = new IpcStreamReader(new ByteArrayInputStream(response), Allocators.root())) {
            while (r.readNextBatch() != null) {
                VectorSchemaRoot root = r.root();
                if (root.getRowCount() > 0) return root.getVector(0).getObject(0).toString();
            }
        }
        throw new AssertionError("no data batch in response");
    }

    @Test
    void initRequestMetadataReachesTheProducersFirstTick() throws Exception {
        RpcServer server = new RpcServer(EchoService.class, new EchoServiceImpl());
        HttpStreamHandler handler = new HttpStreamHandler(server, new byte[32], 0, Long.MAX_VALUE);

        byte[] response = handler.handleInit("echo_meta",
                initRequest(server, "echo_meta", Map.of("ignored", 1L), Map.of(PROBE_KEY, "\"rev-v1\"")));

        assertEquals("\"rev-v1\"", firstRow(response));
    }

    @Test
    void absentMetadataIsReportedAsAbsentNotAsAMissingInputBatch() throws Exception {
        RpcServer server = new RpcServer(EchoService.class, new EchoServiceImpl());
        HttpStreamHandler handler = new HttpStreamHandler(server, new byte[32], 0, Long.MAX_VALUE);

        byte[] response = handler.handleInit("echo_meta",
                initRequest(server, "echo_meta", Map.of("ignored", 1L), Map.of()));

        assertEquals("<absent>", firstRow(response));
    }
}
