// Copyright 2026 Query Farm LLC - https://query.farm
package farm.query.vgirpc.wire;

import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgirpc.schema.ArrowField;
import farm.query.vgirpc.schema.ArrowFieldType;
import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.schema.Nullable;
import farm.query.vgirpc.schema.SchemaDerivation;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

public class RecordDictionaryEncodingTest {
    public record Contract(
            @ArrowField(ArrowFieldType.DICT_INT16_UTF8) String enforcement,
            @ArrowField(ArrowFieldType.DICT_INT16_UTF8) String validation,
            @Nullable @ArrowField(ArrowFieldType.DICT_INT16_UTF8) String optional,
            String plain) implements ArrowSerializableRecord {}

    @Test
    void annotatedRecordFieldsDeclareInt16Dictionaries() {
        var fields = SchemaDerivation.schemaForRecord(Contract.class).getFields();
        for (int i = 0; i < 3; i++) {
            var field = fields.get(i);
            assertNotNull(field.getDictionary());
            assertEquals(new ArrowType.Int(16, true), field.getDictionary().getIndexType());
            assertFalse(field.getDictionary().isOrdered());
            assertEquals(new ArrowType.Utf8(), field.getType());
            assertEquals(i == 2, field.isNullable());
        }
        assertNull(fields.get(3).getDictionary());
    }

    @Test
    void recordIpcWritesRealDictionariesWithDistinctValuesAndNulls() throws Exception {
        for (String optional : new String[] {null, "ALL_ROWS"}) {
            var value = new Contract("METADATA_ONLY", "UNVALIDATED", optional, "plain value");
            byte[] bytes = RecordCodec.serializeToBytes(value);
            try (var allocator = new RootAllocator();
                 var reader = new ArrowStreamReader(new ByteArrayInputStream(bytes), allocator)) {
                assertTrue(reader.loadNextBatch());
                var root = reader.getVectorSchemaRoot();
                assertEquals(1, root.getRowCount());
                var expected = new String[] {value.enforcement(), value.validation(), value.optional()};
                for (int i = 0; i < 3; i++) {
                    var field = root.getSchema().getFields().get(i);
                    assertNotNull(field.getDictionary());
                    assertEquals(new ArrowType.Int(16, true), field.getDictionary().getIndexType());
                    assertFalse(field.getDictionary().isOrdered());
                    var indices = assertInstanceOf(SmallIntVector.class, root.getVector(i));
                    var dictionary = assertInstanceOf(VarCharVector.class,
                            reader.getDictionaryVectors().get(field.getDictionary().getId()).getVector());
                    if (expected[i] == null) {
                        assertTrue(indices.isNull(0));
                    } else {
                        assertEquals(expected[i], new String(dictionary.get(indices.get(0)), StandardCharsets.UTF_8));
                    }
                }
                assertEquals("plain value", root.getVector(3).getObject(0).toString());
                assertFalse(reader.loadNextBatch());
            }
            assertEquals(value, RecordCodec.deserializeFromBytes(bytes, Contract.class));
        }
    }
}
