// Copyright 2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0
package farm.query.vgirpc.wire;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowDictionaryBatch;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class IpcStreamReaderValidationTest {
    private static byte[] listBatch(boolean corrupt) throws Exception {
        var schema = new Schema(List.of(new Field("items", FieldType.nullable(new ArrowType.List()),
                List.of(new Field("item", FieldType.nullable(new ArrowType.Int(64, true)), List.of())))));
        try (var allocator = new RootAllocator(); var root = VectorSchemaRoot.create(schema, allocator)) {
            root.allocateNew();
            var list = (ListVector) root.getVector(0);
            list.startNewValue(0);
            var values = (BigIntVector) list.getDataVector();
            values.setSafe(0, 1); values.setSafe(1, 2);
            list.endValue(0, 2); root.setRowCount(1);
            // The IPC child node still declares two values. Stock VectorLoader
            // calls root.setRowCount after loading, which used to expand the
            // child to this bogus offset and mask the invalid wire shape.
            if (corrupt) list.getOffsetBuffer().setInt(4, 1 << 20);
            var bytes = new ByteArrayOutputStream();
            try (var writer = new IpcStreamWriter(bytes)) {
                writer.writeSchema(root.getSchema());
                writer.writeBatch(root, Map.of());
            }
            return bytes.toByteArray();
        }
    }

    @Test void rejectsListExtentBeyondDeclaredChildLengthBeforeLoading() throws Exception {
        byte[] malformed = listBatch(true);
        // The corrupt offset would require megabytes if the loader padded it.
        try (var allocator = new RootAllocator(64 * 1024);
             var reader = new IpcStreamReader(new ByteArrayInputStream(malformed), allocator)) {
            var error = assertThrows(IllegalArgumentException.class, reader::readNextBatch);
            assertTrue(error.getMessage().contains("Malformed Arrow IPC batch"), error.getMessage());
        }
    }

    @Test void validListKeepsItsChildLengthAndValues() throws Exception {
        try (var allocator = new RootAllocator();
             var reader = new IpcStreamReader(new ByteArrayInputStream(listBatch(false)), allocator)) {
            assertNotNull(reader.readNextBatch());
            assertEquals(List.of(1L, 2L), reader.root().getVector(0).getObject(0));
            assertNull(reader.readNextBatch());
        }
    }

    @Test void validNestedListDoesNotRequireItsChildToHaveADataBuffer() throws Exception {
        var child = new Field("item", FieldType.nullable(new ArrowType.Int(64, true)), List.of());
        var inner = new Field("item", FieldType.nullable(new ArrowType.List()), List.of(child));
        var schema = new Schema(List.of(new Field("items", FieldType.nullable(new ArrowType.List()), List.of(inner))));
        try (var allocator = new RootAllocator(); var root = VectorSchemaRoot.create(schema, allocator)) {
            root.allocateNew();
            var outer = (ListVector) root.getVector(0);
            var nested = (ListVector) outer.getDataVector();
            var values = (BigIntVector) nested.getDataVector();
            outer.startNewValue(0);
            nested.startNewValue(0);
            values.setSafe(0, 7);
            nested.endValue(0, 1);
            outer.endValue(0, 1);
            outer.setNull(1);
            outer.startNewValue(2); outer.endValue(2, 0);
            root.setRowCount(3);
            var bytes = new ByteArrayOutputStream();
            try (var writer = new IpcStreamWriter(bytes)) { writer.writeBatch(root, Map.of()); }
            try (var reader = new IpcStreamReader(new ByteArrayInputStream(bytes.toByteArray()), allocator)) {
                assertNotNull(reader.readNextBatch());
                assertEquals(List.of(List.of(7L)), reader.root().getVector(0).getObject(0));
                assertNull(reader.root().getVector(0).getObject(1));
                assertEquals(List.of(), reader.root().getVector(0).getObject(2));
            }
        }
    }

    @Test void validEmptyStringsAndNullsMayHaveAnEmptyDataBuffer() throws Exception {
        try (var allocator = new RootAllocator(); var vector = new VarCharVector("s", allocator)) {
            vector.allocateNew();
            vector.setSafe(0, new byte[0]); vector.setNull(1); vector.setValueCount(2);
            var root = new VectorSchemaRoot(List.of(vector.getField()), List.of(vector), 2);
            var bytes = new ByteArrayOutputStream();
            try (var writer = new IpcStreamWriter(bytes)) { writer.writeBatch(root, Map.of()); }
            try (var reader = new IpcStreamReader(new ByteArrayInputStream(bytes.toByteArray()), allocator)) {
                assertNotNull(reader.readNextBatch());
                assertEquals("", reader.root().getVector(0).getObject(0).toString());
                assertNull(reader.root().getVector(0).getObject(1));
            }
        }
    }

    @Test void validMapUsedByVgiCatalogMetadataRoundTrips() throws Exception {
        var entry = new Field("entries", FieldType.notNullable(new ArrowType.Struct()), List.of(
                new Field("key", FieldType.notNullable(new ArrowType.Utf8()), List.of()),
                new Field("value", FieldType.nullable(new ArrowType.Utf8()), List.of())));
        var field = new Field("metadata", FieldType.nullable(new ArrowType.Map(false)), List.of(entry));
        try (var allocator = new RootAllocator(); var root = VectorSchemaRoot.create(new Schema(List.of(field)), allocator)) {
            root.allocateNew();
            var map = (MapVector) root.getVector(0);
            var entries = (StructVector) map.getDataVector();
            map.startNewValue(0);
            entries.setIndexDefined(0);
            ((VarCharVector) entries.getChild("key")).setSafe(0, "format".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            ((VarCharVector) entries.getChild("value")).setSafe(0, "vgi".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            map.endValue(0, 1); root.setRowCount(1);
            var bytes = new ByteArrayOutputStream();
            try (var writer = new IpcStreamWriter(bytes)) { writer.writeBatch(root, Map.of()); }
            try (var reader = new IpcStreamReader(new ByteArrayInputStream(bytes.toByteArray()), allocator)) {
                assertNotNull(reader.readNextBatch());
                var decoded = (StructVector) ((MapVector) reader.root().getVector(0)).getDataVector();
                assertEquals("format", decoded.getChild("key").getObject(0).toString());
                assertEquals("vgi", decoded.getChild("value").getObject(0).toString());
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"root", "struct", "fixed-list"})
    void rejectsFieldNodeLengthsThatWouldBeOverwritten(String shape) throws Exception {
        var child = new Field("n", FieldType.nullable(new ArrowType.Int(64, true)), List.of());
        var field = switch (shape) {
            case "root" -> child;
            case "struct" -> new Field("s", FieldType.nullable(new ArrowType.Struct()), List.of(child));
            default -> new Field("a", FieldType.nullable(new ArrowType.FixedSizeList(2)), List.of(child));
        };
        byte[] wire;
        try (var allocator = new RootAllocator(); var root = VectorSchemaRoot.create(new Schema(List.of(field)), allocator)) {
            root.allocateNew();
            var vector = root.getVector(0);
            if (vector instanceof BigIntVector scalar) scalar.setSafe(0, 7);
            if (vector instanceof StructVector struct) {
                struct.setIndexDefined(0); ((BigIntVector) struct.getChild("n")).setSafe(0, 7);
            }
            if (vector instanceof FixedSizeListVector list) {
                list.setNotNull(0);
                ((BigIntVector) list.getDataVector()).setSafe(0, 7);
                ((BigIntVector) list.getDataVector()).setSafe(1, 8);
            }
            root.setRowCount(1);
            var bytes = new ByteArrayOutputStream();
            try (var batch = new VectorUnloader(root).getRecordBatch()) {
                var nodes = new ArrayList<>(batch.getNodes());
                int index = shape.equals("root") ? 0 : 1;
                nodes.set(index, new ArrowFieldNode(nodes.get(index).getLength() - 1, 0));
                try (var malformed = new ArrowRecordBatch(1, nodes, batch.getBuffers());
                     var writer = new IpcStreamWriter(bytes)) {
                    writer.writeSchema(root.getSchema());
                    writer.writeBatch(malformed, Map.of());
                }
            }
            wire = bytes.toByteArray();
        }
        try (var allocator = new RootAllocator();
             var reader = new IpcStreamReader(new ByteArrayInputStream(wire), allocator)) {
            var error = assertThrows(IllegalArgumentException.class, reader::readNextBatch);
            assertTrue(error.getMessage().contains("Malformed Arrow IPC batch"), error.getMessage());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void rejectsMalformedListDictionaryIncludingDeltaBatches(boolean delta) throws Exception {
        var encoding = new DictionaryEncoding(5, false, new ArrowType.Int(32, true));
        var child = new Field("item", FieldType.nullable(new ArrowType.Int(64, true)), List.of());
        var dictionaryField = new Field("dict", FieldType.nullable(new ArrowType.List()), List.of(child));
        var wireField = new Field("a", new FieldType(true, new ArrowType.List(), encoding), List.of(child));
        byte[] wire;
        try (var allocator = new RootAllocator();
             var root = VectorSchemaRoot.create(new Schema(List.of(dictionaryField)), allocator)) {
            root.allocateNew();
            var list = (ListVector) root.getVector(0);
            list.startNewValue(0);
            ((BigIntVector) list.getDataVector()).setSafe(0, 7);
            list.endValue(0, 1); root.setRowCount(1);
            var bytes = new ByteArrayOutputStream();
            var out = new WriteChannel(Channels.newChannel(bytes));
            MessageSerializer.serialize(out, new Schema(List.of(wireField)));
            if (delta) {
                try (var initial = new ArrowDictionaryBatch(5, new VectorUnloader(root).getRecordBatch(), false)) {
                    MessageSerializer.serialize(out, initial);
                }
            }
            list.getOffsetBuffer().setInt(4, 1 << 20);
            try (var malformed = new ArrowDictionaryBatch(5, new VectorUnloader(root).getRecordBatch(), delta)) {
                MessageSerializer.serialize(out, malformed);
            }
            ArrowStreamWriter.writeEndOfStream(out, IpcOption.DEFAULT);
            wire = bytes.toByteArray();
        }
        try (var allocator = new RootAllocator(64 * 1024);
             var reader = new IpcStreamReader(new ByteArrayInputStream(wire), allocator)) {
            var error = assertThrows(IllegalArgumentException.class, reader::readNextBatch);
            assertTrue(error.getMessage().contains("Malformed Arrow IPC dictionary batch"), error.getMessage());
        }
    }
}
