// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.ExchangeState;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.RpcStream;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Reverses the usual conformance direction: the native Java HTTP client sends
 * exact, explicitly typed Arrow batches to Python's strict client worker.
 */
@Timeout(30)
final class NativeClientConformanceTest {

    private static final long DICTIONARY_ID = 42;
    private static final DictionaryEncoding CATEGORY_ENCODING =
            new DictionaryEncoding(DICTIONARY_ID, false, new ArrowType.Int(16, true));
    private static final Field NULLABLE_FLOAT = new Field("nullable_float",
            FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null);
    private static final Field TAGS = new Field("tags", FieldType.nullable(new ArrowType.List()),
            List.of(new Field("item", FieldType.nullable(new ArrowType.Utf8()), null)));
    // A memory-format dictionary field stores indices. ArrowStreamWriter uses
    // the provider to put dictionary<string, int16> on the wire.
    private static final Field CATEGORY = new Field("category",
            new FieldType(true, CATEGORY_ENCODING.getIndexType(), CATEGORY_ENCODING), null);
    private static final Field EVENT_TIME = new Field("event_time",
            FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC")), null);
    private static final Field AMOUNT = new Field("amount",
            FieldType.nullable(new ArrowType.Decimal(18, 4, 128)), null);
    private static final Field NESTED = new Field("nested", FieldType.nullable(new ArrowType.Struct()),
            List.of(
                    new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
                    new Field("scores", FieldType.nullable(new ArrowType.List()),
                            List.of(new Field("item",
                                    FieldType.nullable(new ArrowType.Int(32, true)), null)))));
    private static final Schema MEMORY_SCHEMA =
            new Schema(List.of(NULLABLE_FLOAT, TAGS, CATEGORY, EVENT_TIME, AMOUNT, NESTED));

    interface ClientConformanceService {
        RpcStream<TypedEchoMarker> typed_exchange();
    }

    /** Compile-time stream-state marker; only the Python worker executes state. */
    static final class TypedEchoMarker extends ExchangeState {
        @Override public void exchange(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
            throw new UnsupportedOperationException();
        }
    }

    private static Process worker;
    private static HttpRpcConnection connection;
    private static ClientConformanceService service;

    @BeforeAll
    static void startPythonWorker() throws Exception {
        String python = findPython();
        Assumptions.assumeTrue(python != null,
                "Python client conformance worker is unavailable; set VGI_RPC_PYTHON");
        worker = new ProcessBuilder(python, "-m", "vgi_rpc.conformance.client_worker", "--http", "0")
                .redirectErrorStream(true)
                .start();
        BufferedReader output = new BufferedReader(
                new InputStreamReader(worker.getInputStream(), StandardCharsets.UTF_8));
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        List<String> startup = new ArrayList<>();
        Integer port = null;
        while (System.nanoTime() < deadline && port == null) {
            while (output.ready()) {
                String line = output.readLine();
                if (line == null) break;
                startup.add(line);
                if (line.startsWith("PORT:")) port = Integer.parseInt(line.substring(5));
            }
            if (port == null && !worker.isAlive()) break;
            if (port == null) Thread.sleep(25);
        }
        assertNotNull(port, "Python client worker did not start: " + startup);
        connection = HttpRpcConnection.builder("http://127.0.0.1:" + port).build();
        service = connection.proxy(ClientConformanceService.class);
    }

    @AfterAll
    static void stopPythonWorker() throws Exception {
        if (connection != null) connection.close();
        if (worker != null) {
            worker.destroy();
            if (!worker.waitFor(5, java.util.concurrent.TimeUnit.SECONDS)) worker.destroyForcibly();
        }
    }

    @Test
    void allNullOneRowRoundTripsWithExactSchema() throws Exception {
        try (InputBatch input = InputBatch.allNull();
             RpcStream<?> stream = service.typed_exchange()) {
            AnnotatedBatch result = stream.exchange(input.batch());
            assertEquals(1, result.root().getRowCount());
            for (var vector : result.root().getFieldVectors()) assertTrue(vector.isNull(0));
            assertNotNull(result.dictionaryProvider());
        }
    }

    @Test
    void zeroRowRoundTripsAsDataRatherThanEndOfStream() throws Exception {
        try (InputBatch input = InputBatch.zeroRows();
             RpcStream<?> stream = service.typed_exchange()) {
            AnnotatedBatch result = stream.exchange(input.batch());
            assertEquals(0, result.root().getRowCount());
            assertEquals(6, result.root().getFieldVectors().size());
        }
    }

    @Test
    void populatedComplexTypesAndDictionaryRoundTrip() throws Exception {
        try (InputBatch input = InputBatch.populated();
             RpcStream<?> stream = service.typed_exchange()) {
            AnnotatedBatch result = stream.exchange(input.batch());
            VectorSchemaRoot root = result.root();
            assertEquals(1, root.getRowCount());
            assertEquals(3.5d, ((Float8Vector) root.getVector("nullable_float")).get(0));

            List<?> tags = (List<?>) ((ListVector) root.getVector("tags")).getObject(0);
            assertEquals(java.util.Arrays.asList("alpha", null, "omega"),
                    tags.stream().map(v -> v == null ? null : v.toString()).toList());

            SmallIntVector category = (SmallIntVector) root.getVector("category");
            assertEquals(1, category.get(0));
            // Dictionary ids are IPC-local and Python may canonicalize the
            // caller's id while echoing; resolve through the returned field.
            long returnedDictionaryId = category.getField().getDictionary().getId();
            Dictionary dictionary = result.dictionaryProvider().lookup(returnedDictionaryId);
            assertNotNull(dictionary);
            assertEquals("green", ((VarCharVector) dictionary.getVector()).getObject(1).toString());

            assertEquals(1_700_000_000_123_456L,
                    ((TimeStampMicroTZVector) root.getVector("event_time")).get(0));
            assertEquals(new BigDecimal("1234.5678"),
                    ((DecimalVector) root.getVector("amount")).getObject(0));

            StructVector nested = (StructVector) root.getVector("nested");
            assertFalse(nested.isNull(0));
            assertEquals("node", ((VarCharVector) nested.getChild("name")).getObject(0).toString());
            List<?> scores = (List<?>) ((ListVector) nested.getChild("scores")).getObject(0);
            assertEquals(java.util.Arrays.asList(7, null, 9), scores);
        }
    }

    private static String findPython() throws IOException, InterruptedException {
        String configured = System.getenv("VGI_RPC_PYTHON");
        if (configured != null && !configured.isBlank()) return configured;
        Path cursor = Path.of("").toAbsolutePath();
        while (cursor != null) {
            Path candidate = cursor.resolveSibling("vgi-rpc/.venv/bin/python");
            if (Files.isExecutable(candidate)) return candidate.toString();
            cursor = cursor.getParent();
        }
        Process probe = new ProcessBuilder("python3", "-c",
                "import vgi_rpc.conformance.client_worker").start();
        return probe.waitFor() == 0 ? "python3" : null;
    }

    private static final class InputBatch implements AutoCloseable {
        private final VectorSchemaRoot root;
        private final DictionaryProvider.MapDictionaryProvider dictionaries;
        private final VarCharVector dictionaryValues;

        private InputBatch(int rows) {
            root = VectorSchemaRoot.create(MEMORY_SCHEMA, Allocators.root());
            root.allocateNew();
            dictionaryValues = new VarCharVector("category_dictionary", Allocators.root());
            dictionaryValues.allocateNew();
            dictionaryValues.setSafe(0, new Text("red"));
            dictionaryValues.setSafe(1, new Text("green"));
            dictionaryValues.setValueCount(2);
            dictionaries = new DictionaryProvider.MapDictionaryProvider(
                    new Dictionary(dictionaryValues, CATEGORY_ENCODING));
            root.setRowCount(rows);
        }

        static InputBatch allNull() {
            InputBatch batch = new InputBatch(1);
            batch.root.getFieldVectors().forEach(v -> v.setNull(0));
            return batch;
        }

        static InputBatch zeroRows() { return new InputBatch(0); }

        static InputBatch populated() {
            InputBatch batch = new InputBatch(1);
            ((Float8Vector) batch.root.getVector("nullable_float")).setSafe(0, 3.5d);

            ListVector tags = (ListVector) batch.root.getVector("tags");
            VarCharVector tagValues = (VarCharVector) tags.getDataVector();
            tags.startNewValue(0);
            tagValues.setSafe(0, new Text("alpha"));
            tagValues.setNull(1);
            tagValues.setSafe(2, new Text("omega"));
            tagValues.setValueCount(3);
            tags.endValue(0, 3);

            ((SmallIntVector) batch.root.getVector("category")).setSafe(0, (short) 1);
            ((TimeStampMicroTZVector) batch.root.getVector("event_time"))
                    .setSafe(0, 1_700_000_000_123_456L);
            ((DecimalVector) batch.root.getVector("amount"))
                    .setSafe(0, new BigDecimal("1234.5678"));

            StructVector nested = (StructVector) batch.root.getVector("nested");
            nested.setIndexDefined(0);
            ((VarCharVector) nested.getChild("name")).setSafe(0, new Text("node"));
            ListVector scores = (ListVector) nested.getChild("scores");
            IntVector scoreValues = (IntVector) scores.getDataVector();
            scores.startNewValue(0);
            scoreValues.setSafe(0, 7);
            scoreValues.setNull(1);
            scoreValues.setSafe(2, 9);
            scoreValues.setValueCount(3);
            scores.endValue(0, 3);
            batch.root.setRowCount(1);
            return batch;
        }

        AnnotatedBatch batch() { return new AnnotatedBatch(root, Map.of(), dictionaries, null); }

        @Override public void close() {
            root.close();
            dictionaryValues.close();
        }
    }
}
