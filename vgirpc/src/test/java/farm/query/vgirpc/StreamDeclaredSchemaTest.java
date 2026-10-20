// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

import farm.query.vgirpc.http.HttpRpcConnection;
import farm.query.vgirpc.http.HttpServer;
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
import java.util.List;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * An inline stream declares the method's output schema, not the schema of
 * whichever root the handler happened to emit first.
 *
 * <p>The reference reconciles every emitted batch to {@code output_schema}
 * ({@code OutputCollector.emit} casts a batch whose schema differs) and opens
 * each response stream with {@code new_ipc_stream(sink, output_schema)}, so a
 * client always reads the declared schema. This port's writers record the
 * declared schema but let the first batch's own schema define the stream, so
 * (before {@code OutputCollector.emit} reconciled to the output schema) a
 * handler that built its root with a nullable field where the method declares
 * {@code not null} put a nullable schema on the wire -- and only when no log
 * batch (built from the declared schema) happened to go first. A client that
 * checks what it reads against the declared contract (DuckDB's VGI client
 * compares nested types, which carry child nullability) sees a different
 * schema from a Java worker than from the reference for the same protocol.
 */
final class StreamDeclaredSchemaTest {

    static final Schema IN_SCHEMA = new Schema(List.of(
            new Field("v", FieldType.nullable(new ArrowType.Int(64, true)), null)));
    /** What the method declares. */
    static final Schema OUT_SCHEMA = new Schema(List.of(
            new Field("v", FieldType.notNullable(new ArrowType.Int(64, true)), null)));
    /** What the handler builds: same layout, looser nullability. */
    static final Schema HANDLER_SCHEMA = new Schema(List.of(
            new Field("v", FieldType.nullable(new ArrowType.Int(64, true)), null)));

    static VectorSchemaRoot handlerRoot(long... values) {
        VectorSchemaRoot root = VectorSchemaRoot.create(HANDLER_SCHEMA, Allocators.root());
        root.allocateNew();
        BigIntVector v = (BigIntVector) root.getVector(0);
        for (int i = 0; i < values.length; i++) v.setSafe(i, values[i]);
        v.setValueCount(values.length);
        root.setRowCount(values.length);
        return root;
    }

    public static final class Echo extends ExchangeState {
        public Echo() {}
        @Override public void exchange(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
            BigIntVector in = (BigIntVector) input.root().getVector(0);
            long[] values = new long[input.root().getRowCount()];
            for (int i = 0; i < values.length; i++) values[i] = in.get(i);
            out.emit(handlerRoot(values));
        }
    }

    public static final class Count extends ProducerState {
        public long n;
        public Count() {}
        @Override public void produce(OutputCollector out, CallContext ctx) {
            if (n >= 2) { out.finish(); return; }
            out.emit(handlerRoot(n++));
        }
    }

    public interface SchemaService {
        RpcStream<Echo> echo();
        RpcStream<Count> count();
        long ping(long v);
    }

    public static final class Impl implements SchemaService {
        @Override public RpcStream<Echo> echo() { return RpcStream.exchange(IN_SCHEMA, OUT_SCHEMA, new Echo()); }
        @Override public RpcStream<Count> count() { return RpcStream.producer(OUT_SCHEMA, new Count()); }
        @Override public long ping(long v) { return v; }
    }

    // ------------------------------------------------------------------

    @Test
    @Timeout(30)
    void pipeExchangeDeclaresTheOutputSchema() throws Exception {
        withPipe(svc -> { assertExchange(svc.echo()); return null; });
    }

    @Test
    @Timeout(30)
    void pipeProducerDeclaresTheOutputSchema() throws Exception {
        withPipe(svc -> { assertProducer(svc.count()); return null; });
    }

    @Test
    @Timeout(30)
    void httpExchangeDeclaresTheOutputSchema() throws Exception {
        withHttp(svc -> { assertExchange(svc.echo()); return null; });
    }

    @Test
    @Timeout(30)
    void httpProducerDeclaresTheOutputSchema() throws Exception {
        withHttp(svc -> { assertProducer(svc.count()); return null; });
    }

    private static void assertExchange(RpcStream<Echo> stream) {
        try {
            for (long turn = 1; turn <= 2; turn++) {
                VectorSchemaRoot input = VectorSchemaRoot.create(IN_SCHEMA, Allocators.root());
                try {
                    input.allocateNew();
                    ((BigIntVector) input.getVector(0)).setSafe(0, turn);
                    input.setRowCount(1);
                    AnnotatedBatch out = stream.exchange(new AnnotatedBatch(input, null));
                    assertEquals(OUT_SCHEMA, out.root().getSchema(),
                            "turn " + turn + ": the stream must declare the method's output schema");
                    assertEquals(turn, ((BigIntVector) out.root().getVector(0)).get(0));
                } finally {
                    input.close();
                }
            }
        } finally {
            stream.close();
        }
    }

    private static void assertProducer(RpcStream<Count> stream) {
        try {
            for (long i = 0; i < 2; i++) {
                AnnotatedBatch out = stream.tick();
                assertEquals(OUT_SCHEMA, out.root().getSchema(),
                        "tick " + i + ": the stream must declare the method's output schema");
                assertEquals(i, ((BigIntVector) out.root().getVector(0)).get(0));
            }
        } finally {
            stream.close();
        }
    }

    // Harnesses ----------------------------------------------------------

    private static void withPipe(Function<SchemaService, Void> body) throws Exception {
        PipedOutputStream clientOut = new PipedOutputStream();
        PipedInputStream serverIn = new PipedInputStream(clientOut, 1 << 16);
        PipedOutputStream serverOut = new PipedOutputStream();
        PipedInputStream clientIn = new PipedInputStream(serverOut, 1 << 16);
        RpcTransport serverTransport = new PipeTransport(serverIn, serverOut);
        RpcTransport clientTransport = new PipeTransport(clientIn, clientOut);
        RpcServer server = new RpcServer(SchemaService.class, new Impl());
        Thread thread = new Thread(() -> server.serve(serverTransport), "rpc-server");
        thread.setDaemon(true);
        thread.start();
        RpcConnection connection = new RpcConnection(clientTransport);
        try {
            SchemaService svc = connection.proxy(SchemaService.class);
            body.apply(svc);
            assertEquals(5L, svc.ping(5L), "the connection must stay usable");
        } finally {
            connection.close();
            clientTransport.close();
            thread.join(2000);
        }
    }

    private static void withHttp(Function<SchemaService, Void> body) throws Exception {
        HttpServer server = new HttpServer(new RpcServer(SchemaService.class, new Impl()),
                HttpServer.Config.builder().prefix("/vgi").build());
        server.start();
        try (HttpRpcConnection connection =
                     HttpRpcConnection.builder("http://127.0.0.1:" + server.port() + "/vgi").build()) {
            body.apply(connection.proxy(SchemaService.class));
        } finally {
            server.stop();
        }
    }

    private static final class PipeTransport implements RpcTransport {
        private final InputStream in;
        private final OutputStream out;
        PipeTransport(InputStream in, OutputStream out) { this.in = in; this.out = out; }
        @Override public InputStream reader() { return in; }
        @Override public OutputStream writer() { return out; }
        @Override public void close() {
            try { out.flush(); } catch (Exception ignore) { /* best-effort */ }
            try { out.close(); } catch (Exception ignore) { /* best-effort */ }
            try { in.close(); } catch (Exception ignore) { /* best-effort */ }
        }
    }
}
