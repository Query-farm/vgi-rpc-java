// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import farm.query.vgirpc.AccessLogHook;
import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.ProducerState;
import farm.query.vgirpc.RpcConnection;
import farm.query.vgirpc.RpcServer;
import farm.query.vgirpc.RpcStream;
import farm.query.vgirpc.transport.RpcTransport;
import farm.query.vgirpc.wire.Allocators;
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
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * No payload value reaches any log, at the most verbose level there is.
 *
 * <p>A sentinel secret rides a unary argument and a stream argument that the producer keeps in
 * its state, so over HTTP it is sealed into every continuation token. With the library's
 * loggers at TRACE (see the test task in {@code vgirpc/build.gradle.kts}) and an access log
 * attached, neither the access log nor anything written to stdout/stderr may contain the
 * sentinel or any base64 alignment of it -- on HTTP and on the pipe transport.
 *
 * <p>Up to 0.30.0 the access log carried the request as base64 Arrow IPC ({@code
 * request_data}) and the <em>decrypted</em> stream state ({@code request_state} /
 * {@code response_state}) by default, which is how a VGI {@code catalog_attach}'s secret
 * options reached logs.
 */
final class NoPayloadInLogsTest {

    private static final String SECRET = "sk-live-NO-PAYLOAD-SENTINEL-7f3a9c";
    private static final ObjectMapper JSON = new ObjectMapper();

    static final Schema OUT_SCHEMA = new Schema(List.of(
            new Field("n", FieldType.nullable(new ArrowType.Int(64, true)), null)));

    /** Counts to {@code remaining}, holding the secret in its state the whole way. */
    public static final class SecretHoldingProducer extends ProducerState {
        public String apiKey;
        public long remaining;
        public long next;

        public SecretHoldingProducer() {}

        SecretHoldingProducer(String apiKey, long remaining) {
            this.apiKey = apiKey;
            this.remaining = remaining;
        }

        @Override public void produce(OutputCollector out, CallContext ctx) {
            if (remaining <= 0) {
                out.finish();
                return;
            }
            VectorSchemaRoot root = VectorSchemaRoot.create(OUT_SCHEMA, Allocators.root());
            root.allocateNew();
            ((BigIntVector) root.getVector(0)).setSafe(0, next++);
            root.setRowCount(1);
            out.emit(root);
            remaining--;
        }
    }

    public interface SecretService {
        long attach(String api_key, long n);

        RpcStream<SecretHoldingProducer> scan(String api_key, long n);
    }

    public static final class Impl implements SecretService {
        @Override public long attach(String api_key, long n) { return api_key.length() + n; }

        @Override public RpcStream<SecretHoldingProducer> scan(String api_key, long n) {
            return RpcStream.producer(OUT_SCHEMA, new SecretHoldingProducer(api_key, n));
        }
    }

    private final ByteArrayOutputStream accessLog = new ByteArrayOutputStream();
    private final ByteArrayOutputStream console = new ByteArrayOutputStream();
    private PrintStream savedOut;
    private PrintStream savedErr;

    @BeforeEach
    void captureConsole() {
        savedOut = System.out;
        savedErr = System.err;
        PrintStream tee = new PrintStream(console, true, StandardCharsets.UTF_8);
        System.setOut(tee);
        System.setErr(tee);
    }

    @AfterEach
    void restoreConsole() {
        System.setOut(savedOut);
        System.setErr(savedErr);
    }

    @Test
    @Timeout(60)
    void http_logs_neither_arguments_nor_stream_state() throws Exception {
        assertMostVerbose();
        RpcServer rpc = new RpcServer(SecretService.class, new Impl());
        rpc.setDispatchHook(AccessLogHook.builder(accessLog).build());
        HttpServer server = new HttpServer(rpc, HttpServer.Config.builder().prefix("/vgi").build());
        server.start();
        try (HttpRpcConnection conn = HttpRpcConnection.builder(
                "http://127.0.0.1:" + server.port() + "/vgi").build()) {
            exercise(conn.proxy(SecretService.class));
        } finally {
            server.stop();
        }

        List<JsonNode> records = records();
        // init + one record per continuation, and the continuations carried sealed state.
        assertTrue(records.stream().anyMatch(r -> r.path("request_state_bytes").asInt() > 0),
                "a continuation must report the size of the token it was sent");
        assertTrue(records.stream().anyMatch(r -> r.path("response_state_bytes").asInt() > 0),
                "a turn that mints a cursor must report its size");
        assertNoSecret();
    }

    @Test
    @Timeout(60)
    void pipe_logs_no_arguments() throws Exception {
        assertMostVerbose();
        RpcServer rpc = new RpcServer(SecretService.class, new Impl());
        rpc.setDispatchHook(AccessLogHook.builder(accessLog).build());

        PipedOutputStream clientOut = new PipedOutputStream();
        PipedInputStream serverIn = new PipedInputStream(clientOut, 1 << 16);
        PipedOutputStream serverOut = new PipedOutputStream();
        PipedInputStream clientIn = new PipedInputStream(serverOut, 1 << 16);
        Thread serving = new Thread(() -> rpc.serve(new Pipe(serverIn, serverOut)), "no-payload-pipe");
        serving.setDaemon(true);
        serving.start();
        Pipe clientSide = new Pipe(clientIn, clientOut);
        try (RpcConnection conn = new RpcConnection(clientSide)) {
            exercise(conn.proxy(SecretService.class));
        } finally {
            clientSide.close();
            serving.join(5000);
        }
        assertNoSecret();
    }

    /** One unary call and one multi-turn stream, both carrying the secret. */
    private void exercise(SecretService svc) {
        assertEquals(SECRET.length() + 3, svc.attach(SECRET, 3));
        List<Long> seen = new ArrayList<>();
        try (RpcStream<?> stream = svc.scan(SECRET, 4)) {
            while (true) {
                AnnotatedBatch batch;
                try {
                    batch = stream.tick();
                } catch (NoSuchElementException end) {
                    break;
                }
                BigIntVector v = (BigIntVector) batch.root().getVector("n");
                for (int i = 0; i < batch.root().getRowCount(); i++) seen.add(v.get(i));
            }
        }
        assertEquals(List.of(0L, 1L, 2L, 3L), seen);
    }

    /** The test is only as strong as the log level it runs at. */
    private static void assertMostVerbose() {
        assertTrue(LoggerFactory.getLogger(RpcServer.class).isTraceEnabled(),
                "the library's loggers must be at TRACE for this test to mean anything");
    }

    private List<JsonNode> records() throws Exception {
        List<JsonNode> out = new ArrayList<>();
        for (String line : accessLog.toString(StandardCharsets.UTF_8).split("\n")) {
            if (!line.isBlank()) out.add(JSON.readTree(line));
        }
        return out;
    }

    private void assertNoSecret() throws Exception {
        List<JsonNode> records = records();
        assertTrue(records.size() >= 2, "expected access records, got " + records.size());
        for (JsonNode rec : records) {
            for (String forbidden : List.of("request_data", "request_state", "response_state")) {
                assertFalse(rec.has(forbidden), forbidden + " in " + rec);
            }
        }
        JsonNode unary = records.stream().filter(r -> "attach".equals(r.path("method").asText()))
                .findFirst().orElseThrow();
        assertEquals("api_key", unary.get("request_fields").get(0).get("name").asText());
        assertEquals(1, unary.get("request_rows").asInt());

        String logged = accessLog.toString(StandardCharsets.UTF_8);
        String printed = console.toString(StandardCharsets.UTF_8);
        for (String needle : needles()) {
            assertFalse(logged.contains(needle), "access log contains the secret (" + needle + ")");
            assertFalse(printed.contains(needle), "console log contains the secret (" + needle + ")");
        }
    }

    /** The sentinel, and the stable core of its base64 at each of the three byte alignments. */
    private static List<String> needles() {
        List<String> out = new ArrayList<>(List.of(SECRET));
        byte[] raw = SECRET.getBytes(StandardCharsets.UTF_8);
        for (int pad = 0; pad < 3; pad++) {
            byte[] shifted = new byte[raw.length + pad];
            System.arraycopy(raw, 0, shifted, pad, raw.length);
            String b64 = Base64.getEncoder().encodeToString(shifted);
            // Drop the characters that depend on the neighbouring bytes at each end.
            out.add(b64.substring(4, b64.length() - 4));
        }
        return out;
    }

    private static final class Pipe implements RpcTransport {
        private final InputStream in;
        private final OutputStream out;

        Pipe(InputStream in, OutputStream out) {
            this.in = in;
            this.out = out;
        }

        @Override public InputStream reader() { return in; }

        @Override public OutputStream writer() { return out; }

        @Override public void close() {
            try { out.close(); } catch (Exception ignore) { /* best-effort */ }
            try { in.close(); } catch (Exception ignore) { /* best-effort */ }
        }
    }
}
