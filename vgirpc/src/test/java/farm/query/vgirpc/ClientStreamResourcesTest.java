// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

import farm.query.vgirpc.transport.RpcTransport;
import farm.query.vgirpc.wire.Allocators;
import farm.query.vgirpc.wire.IpcStreamWriter;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.*;

final class ClientStreamResourcesTest {
    private static final Schema SCHEMA = new Schema(List.of(
            new Field("value", FieldType.notNullable(new ArrowType.Int(64, true)), null)));

    @Test void releasesBuffersAtEndOfStream() throws Exception {
        byte[] bytes = response();
        long before = Allocators.root().getAllocatedMemory();
        try (var stream = stream(bytes)) {
            assertEquals(42L, stream.tick().root().getVector(0).getObject(0));
            assertEquals(42L, stream.tick().root().getVector(0).getObject(0));
            assertThrows(NoSuchElementException.class, stream::tick);
        }
        assertEquals(before, Allocators.root().getAllocatedMemory());
    }

    @Test void releasesBuffersAfterEarlyCancellation() throws Exception {
        byte[] bytes = response();
        long before = Allocators.root().getAllocatedMemory();
        try (var stream = stream(bytes)) {
            stream.tick();
            stream.cancel();
            stream.cancel();
        }
        assertEquals(before, Allocators.root().getAllocatedMemory());
    }

    @Test void releasesBuffersWhenClosedBeforeTheFirstTick() throws Exception {
        byte[] bytes = response();
        long before = Allocators.root().getAllocatedMemory();
        try (var stream = stream(bytes)) {
            // close() drains the response, which also allocates reader buffers.
        }
        assertEquals(before, Allocators.root().getAllocatedMemory());
    }

    @Test void releasesBuffersAfterATruncatedResponse() throws Exception {
        byte[] bytes = response();
        bytes = Arrays.copyOf(bytes, bytes.length - 24);
        long before = Allocators.root().getAllocatedMemory();
        try (var stream = stream(bytes)) {
            stream.tick();
            assertThrows(RpcError.class, stream::tick);
        }
        assertEquals(before, Allocators.root().getAllocatedMemory());
    }

    @Test void releasesDictionaryBuffersAfterCancellation() throws Exception {
        var bytes = new ByteArrayOutputStream();
        try (var batch = new DictionaryBatch(); var writer = new IpcStreamWriter(bytes)) {
            writer.writeBatch(batch.root, Map.of(), batch.provider);
        }
        long before = Allocators.root().getAllocatedMemory();
        try (var stream = stream(bytes.toByteArray())) {
            var batch = stream.tick();
            assertEquals("value", batch.dictionaryProvider().lookup(7).getVector().getObject(0).toString());
            stream.cancel();
        }
        assertEquals(before, Allocators.root().getAllocatedMemory());
    }

    @Test void releasesExchangeDictionaryCopiesAfterTransportFailure() throws Exception {
        byte[] bytes = response();
        bytes = Arrays.copyOf(bytes, bytes.length - 24);
        try (var input = new DictionaryBatch()) {
            long before = Allocators.root().getAllocatedMemory();
            try (var stream = stream(bytes)) {
                var batch = new AnnotatedBatch(input.root, Map.of(), input.provider, null);
                stream.exchange(batch);
                assertThrows(RpcError.class, () -> stream.exchange(batch));
            }
            assertEquals(before, Allocators.root().getAllocatedMemory());
        }
    }

    @Test void writerReleasesDictionaryCopiesWhenWritingEosFails() throws Exception {
        var failed = new java.util.concurrent.atomic.AtomicBoolean();
        var sink = new OutputStream() {
            @Override public void write(int value) throws IOException {
                if (failed.get()) throw new IOException("closed transport");
            }
        };
        try (var input = new DictionaryBatch()) {
            long before = Allocators.root().getAllocatedMemory();
            var writer = new IpcStreamWriter(sink);
            writer.writeBatch(input.root, Map.of(), input.provider);
            failed.set(true);
            assertThrows(IOException.class, writer::close);
            writer.close();
            assertEquals(before, Allocators.root().getAllocatedMemory());
        }
    }

    private static final class DictionaryBatch implements AutoCloseable {
        final VectorSchemaRoot root;
        final DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();
        DictionaryBatch() {
            var encoding = new DictionaryEncoding(7, false, new ArrowType.Int(32, true));
            var schema = new Schema(List.of(new Field("value", new FieldType(false, encoding.getIndexType(), encoding), null)));
            root = VectorSchemaRoot.create(schema, Allocators.root());
            root.allocateNew();
            ((IntVector) root.getVector(0)).setSafe(0, 0);
            root.setRowCount(1);
            var dictionary = new VarCharVector("dictionary", Allocators.root());
            dictionary.allocateNew();
            dictionary.setSafe(0, "value".getBytes(StandardCharsets.UTF_8));
            dictionary.setValueCount(1);
            provider.put(new Dictionary(dictionary, encoding));
        }
        @Override public void close() {
            root.close();
            provider.close();
        }
    }

    private static byte[] response() throws Exception {
        var bytes = new ByteArrayOutputStream();
        try (var root = VectorSchemaRoot.create(SCHEMA, Allocators.root());
             var writer = new IpcStreamWriter(bytes)) {
            root.allocateNew();
            ((BigIntVector) root.getVector(0)).setSafe(0, 42);
            root.setRowCount(1);
            writer.writeSchema(SCHEMA);
            writer.writeBatch(root, Map.of());
            writer.writeBatch(root, Map.of());
        }
        return bytes.toByteArray();
    }

    private static ClientStreamSession<ProducerState> stream(byte[] response) {
        var input = new ByteArrayInputStream(response);
        var output = new ByteArrayOutputStream();
        RpcTransport transport = new RpcTransport() {
            @Override public InputStream reader() { return input; }
            @Override public OutputStream writer() { return output; }
            @Override public void close() {}
        };
        return new ClientStreamSession<>(transport, RpcStream.EMPTY_SCHEMA, SCHEMA, null, null);
    }
}
