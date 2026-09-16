// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

import farm.query.vgirpc.transport.RpcTransport;
import farm.query.vgirpc.wire.Allocators;
import farm.query.vgirpc.wire.IpcStreamReader;
import farm.query.vgirpc.wire.Metadata;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The untyped client surface — the one a relay, a gateway, or a conformance
 * driver uses, because none of them can name a Java type for a payload they
 * only forward.
 *
 * <p>What is worth asserting here is precisely what a typed call cannot show:
 * that the routing key still rides the request when nobody supplied a service
 * interface to derive it from, that caller-supplied Arrow custom metadata
 * reaches the peer intact rather than being replaced by the connection's own,
 * and that a peer error arrives as {@link RpcError} with the peer's own type
 * string rather than as a local exception.
 */
final class RawClientSurfaceTest {

    /** Minimal service with one unary method, one producer and one exchange. */
    public interface Relayed {
        String shout(String value);

        RpcStream<CountState> countTo(long limit);

        long boom();
    }

    /** Producer state: emits one row per tick until {@code limit} is reached. */
    public static final class CountState extends StreamState {
        private long next;
        private final long limit;

        CountState(long limit) { this.limit = limit; }

        @Override
        public void process(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
            if (next >= limit) {
                out.finish();
                return;
            }
            // Deliberately not closed here: the collector takes ownership of an
            // emitted root, and freeing it on the way out of this method hands
            // the writer released buffers -- which surface downstream as a batch
            // of the right schema whose every value is null.
            VectorSchemaRoot batch = VectorSchemaRoot.create(out.outputSchema(), Allocators.root());
            batch.allocateNew();
            ((BigIntVector) batch.getVector("n")).setSafe(0, next++);
            batch.setRowCount(1);
            out.emit(batch);
        }
    }

    public static final class RelayedImpl implements Relayed {

        @Override public String shout(String value) { return value.toUpperCase(java.util.Locale.ROOT); }

        @Override
        public RpcStream<CountState> countTo(long limit) {
            Schema out = new Schema(List.of(
                    new Field("n", FieldType.notNullable(new ArrowType.Int(64, true)), null)));
            return RpcStream.producer(out, new CountState(limit));
        }

        @Override public long boom() { throw new IllegalArgumentException("no"); }
    }

    @Test
    @Timeout(20)
    void untypedUnaryCarriesTheRoutingKeyAndTheCallersMetadata() throws Exception {
        try (Peer peer = Peer.start()) {
            VectorSchemaRoot params = utf8Row("value", "hello");
            byte[] reply = peer.connection.callUnaryRaw(
                    "Relayed", "1.0.0", "shout",
                    new AnnotatedBatch(params, Map.of("x.caller", "kept")));
            params.close();

            // The peer answered, which on a server that requires the routing key
            // is already the assertion that the key was sent: RpcServer refuses
            // an application call carrying no vgi_rpc.protocol.
            assertEquals("HELLO", resultString(reply));
            Map<String, String> sent = peer.lastRequestMetadata();
            assertEquals("Relayed", sent.get(Metadata.PROTOCOL));
            assertEquals("shout", sent.get(Metadata.RPC_METHOD));
            assertEquals("1.0.0", sent.get(Metadata.PROTOCOL_VERSION_KEY));
            assertEquals("kept", sent.get("x.caller"),
                    "a relay's own metadata must reach the peer, not be replaced by the connection's");
        }
    }

    @Test
    @Timeout(20)
    void callerMetadataOverridesTheConnectionsOwnKeys() throws Exception {
        try (Peer peer = Peer.start()) {
            VectorSchemaRoot params = utf8Row("value", "hi");
            // A relay already holds the request it was asked to forward.
            // Substituting this connection's routing key for the one it was
            // given would make the relay lie about what it was asked to send.
            peer.connection.callUnaryRaw("Relayed", "1.0.0", "shout",
                    new AnnotatedBatch(params, Map.of(Metadata.PROTOCOL, "Relayed",
                            "vgi_rpc.request_version", "1")));
            params.close();
            assertEquals("Relayed", peer.lastRequestMetadata().get(Metadata.PROTOCOL));
        }
    }

    @Test
    @Timeout(20)
    void aPeerErrorArrivesWithThePeersOwnTypeString() throws Exception {
        try (Peer peer = Peer.start()) {
            VectorSchemaRoot params = emptyRow();
            RpcError error = assertThrows(RpcError.class, () -> peer.connection.callUnaryRaw(
                    "Relayed", "1.0.0", "boom", new AnnotatedBatch(params, Map.of())));
            params.close();
            // Not IllegalArgumentException, and not a local wrapper: the peer's
            // own type name, because that string is what callers branch on.
            assertEquals("ValueError", error.errorType());
            assertEquals("no", error.errorMessage());
        }
    }

    @Test
    @Timeout(20)
    void untypedProducerWalksToEndOfStream() throws Exception {
        try (Peer peer = Peer.start()) {
            VectorSchemaRoot params = int64Row("limit", 3);
            RawStream stream = peer.connection.openStreamRaw(
                    "Relayed", "1.0.0", "countTo", new AnnotatedBatch(params, Map.of()), false);
            params.close();

            assertNull(stream.header(), "countTo declares no @StreamHeader");
            // A byte-stream transport carries no resumable state: the stream is
            // the connection, so there is nothing to resume onto.
            assertNull(stream.stateToken());

            List<Long> seen = new ArrayList<>();
            byte[] batch;
            while ((batch = stream.tick(null)) != null) {
                seen.add(firstLong(batch));
            }
            assertEquals(List.of(0L, 1L, 2L), seen);
            stream.close();
        }
    }

    @Test
    @Timeout(20)
    void aTicksMetadataReachesThePeer() throws Exception {
        try (Peer peer = Peer.start()) {
            VectorSchemaRoot params = int64Row("limit", 2);
            RawStream stream = peer.connection.openStreamRaw(
                    "Relayed", "1.0.0", "countTo", new AnnotatedBatch(params, Map.of()), false);
            params.close();
            assertNotNull(stream.tick(Map.of("x.tick", "1")));
            stream.close();
        }
    }

    @Test
    @Timeout(20)
    void framedRepliesAreCompleteIpcStreams() throws Exception {
        try (Peer peer = Peer.start()) {
            VectorSchemaRoot params = utf8Row("value", "x");
            byte[] reply = peer.connection.callUnaryRaw("Relayed", "1.0.0", "shout",
                    new AnnotatedBatch(params, Map.of()));
            params.close();
            // Schema message, one batch, EOS — self-describing, which is what
            // lets it cross a process boundary on its own.
            try (IpcStreamReader r = new IpcStreamReader(
                    new ByteArrayInputStream(reply), Allocators.root())) {
                assertNotNull(r.readNextBatch());
                assertNull(r.readNextBatch(), "a framed reply carries exactly one batch");
                assertTrue(!r.hasMore());
            }
        }
    }

    // ------------------------------------------------------------------

    /** An in-process peer: one {@link RpcServer} on the far end of a pipe pair. */
    private static final class Peer implements AutoCloseable {
        private final RpcConnection connection;
        private final RpcTransport clientTransport;
        private final Thread serverThread;
        private final MetadataSpy spy;

        private Peer(RpcConnection connection, RpcTransport clientTransport, Thread serverThread,
                     MetadataSpy spy) {
            this.connection = connection;
            this.clientTransport = clientTransport;
            this.serverThread = serverThread;
            this.spy = spy;
        }

        static Peer start() throws Exception {
            RpcServer server = new RpcServer(Relayed.class, new RelayedImpl());
            PipedOutputStream clientOut = new PipedOutputStream();
            PipedInputStream serverIn = new PipedInputStream(clientOut, 1 << 16);
            PipedOutputStream serverOut = new PipedOutputStream();
            PipedInputStream clientIn = new PipedInputStream(serverOut, 1 << 16);

            MetadataSpy spy = new MetadataSpy(clientOut);
            RpcTransport serverTransport = new Pipes(serverIn, serverOut);
            RpcTransport clientTransport = new Pipes(clientIn, spy);

            Thread serverThread = new Thread(() -> server.serve(serverTransport), "raw-peer");
            serverThread.setDaemon(true);
            serverThread.start();
            return new Peer(new RpcConnection(clientTransport), clientTransport, serverThread, spy);
        }

        Map<String, String> lastRequestMetadata() throws Exception {
            return spy.lastMetadata();
        }

        @Override public void close() throws Exception {
            connection.close();
            clientTransport.close();
            serverThread.join(2000);
        }
    }

    /**
     * Tees everything the client writes so a test can read back the metadata the
     * client actually put on the wire.
     *
     * <p>Asserting on the peer's answer alone cannot distinguish "the key was
     * sent" from "the peer did not need it", and a client that omits the routing
     * key is exactly the defect worth catching here.
     */
    private static final class MetadataSpy extends OutputStream {
        private final OutputStream delegate;
        private final java.io.ByteArrayOutputStream seen = new java.io.ByteArrayOutputStream();

        MetadataSpy(OutputStream delegate) { this.delegate = delegate; }

        @Override public synchronized void write(int b) throws java.io.IOException {
            seen.write(b);
            delegate.write(b);
        }

        @Override public synchronized void write(byte[] b, int off, int len) throws java.io.IOException {
            seen.write(b, off, len);
            delegate.write(b, off, len);
        }

        @Override public void flush() throws java.io.IOException { delegate.flush(); }

        synchronized Map<String, String> lastMetadata() throws Exception {
            byte[] bytes;
            synchronized (this) { bytes = seen.toByteArray(); }
            try (IpcStreamReader r = new IpcStreamReader(
                    new ByteArrayInputStream(bytes), Allocators.root())) {
                Map<String, String> md = r.readNextBatch();
                return md == null ? Map.of() : Map.copyOf(md);
            }
        }
    }

    private record Pipes(InputStream reader, OutputStream writer) implements RpcTransport {
        @Override public void close() {
            try { writer.flush(); } catch (Exception ignore) { /* best-effort */ }
            try { writer.close(); } catch (Exception ignore) { /* best-effort */ }
            try { reader.close(); } catch (Exception ignore) { /* best-effort */ }
        }
    }

    private static VectorSchemaRoot utf8Row(String field, String value) {
        Schema schema = new Schema(List.of(
                new Field(field, FieldType.notNullable(new ArrowType.Utf8()), null)));
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, Allocators.root());
        root.allocateNew();
        ((VarCharVector) root.getVector(field)).setSafe(0, value.getBytes(StandardCharsets.UTF_8));
        root.setRowCount(1);
        return root;
    }

    private static VectorSchemaRoot int64Row(String field, long value) {
        Schema schema = new Schema(List.of(
                new Field(field, FieldType.notNullable(new ArrowType.Int(64, true)), null)));
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, Allocators.root());
        root.allocateNew();
        ((BigIntVector) root.getVector(field)).setSafe(0, value);
        root.setRowCount(1);
        return root;
    }

    private static VectorSchemaRoot emptyRow() {
        VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(List.of()), Allocators.root());
        root.allocateNew();
        root.setRowCount(1);
        return root;
    }

    private static String resultString(byte[] framed) throws Exception {
        try (IpcStreamReader r = new IpcStreamReader(new ByteArrayInputStream(framed), Allocators.root())) {
            assertNotNull(r.readNextBatch());
            return r.root().getVector("result").getObject(0).toString();
        }
    }

    private static long firstLong(byte[] framed) throws Exception {
        try (IpcStreamReader r = new IpcStreamReader(new ByteArrayInputStream(framed), Allocators.root())) {
            assertNotNull(r.readNextBatch());
            return ((Number) r.root().getVector("n").getObject(0)).longValue();
        }
    }
}
