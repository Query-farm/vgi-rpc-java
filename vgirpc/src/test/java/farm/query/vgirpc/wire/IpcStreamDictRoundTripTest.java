// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.wire;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Reproduces the wire path that fails for dict-encoded enum columns
 * (filter_pushdown/enums.test).
 *
 * <p>Builds the exact VSR shape the worker emits for the echo TIO:
 * <pre>
 *   n: int32                                  (plain)
 *   c: dictionary&lt;values=string, indices=int8&gt;  (memory-format VSR carries
 *                                              the int8 index vector here)
 * </pre>
 * with a 3-entry dictionary {"red", "green", "blue"}. Writes through
 * {@link IpcStreamWriter}, reads back via {@link IpcStreamReader}, asserts
 * the round-trip produces 4 rows with the right values.</p>
 */
public class IpcStreamDictRoundTripTest {

    @Test
    public void echoStyleDictColumn_roundTripsCleanly() throws Exception {
        try (RootAllocator alloc = new RootAllocator()) {
            long dictId = 0;
            DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();

            // Build the dictionary: {"red", "green", "blue"}
            VarCharVector dictVec = new VarCharVector("dict", alloc);
            dictVec.allocateNew();
            dictVec.setSafe(0, new Text("red"));
            dictVec.setSafe(1, new Text("green"));
            dictVec.setSafe(2, new Text("blue"));
            dictVec.setValueCount(3);
            DictionaryEncoding enc = new DictionaryEncoding(dictId, false, new ArrowType.Int(8, true));
            provider.put(new Dictionary(dictVec, enc));

            // Memory-format Field for the dict-encoded column: type = int8, encoding = enc.
            Field cField = new Field("c", new FieldType(true, enc.getIndexType(), enc), null);
            Field nField = new Field("n", new FieldType(true, new ArrowType.Int(32, true), null), null);
            Schema schema = new Schema(List.of(nField, cField));

            // Build a 4-row VSR: rows = [(1,'red'), (2,'green'), (3,NULL), (4,'blue')]
            IntVector nVec = (IntVector) nField.createVector(alloc);
            TinyIntVector cIdx = (TinyIntVector) cField.createVector(alloc);
            nVec.allocateNew(); cIdx.allocateNew();
            nVec.setSafe(0, 1); cIdx.setSafe(0, (byte) 0);
            nVec.setSafe(1, 2); cIdx.setSafe(1, (byte) 1);
            nVec.setSafe(2, 3); cIdx.setNull(2);
            nVec.setSafe(3, 4); cIdx.setSafe(3, (byte) 2);
            nVec.setValueCount(4); cIdx.setValueCount(4);

            VectorSchemaRoot vsr = new VectorSchemaRoot(List.of(nField, cField),
                    List.of(nVec, cIdx), 4);

            // Write via IpcStreamWriter — exact path the worker uses.
            ByteArrayOutputStream sink = new ByteArrayOutputStream();
            try (IpcStreamWriter w = new IpcStreamWriter(sink)) {
                w.writeBatch(vsr, null, provider);
            }
            vsr.close();
            dictVec.close();

            byte[] wire = sink.toByteArray();
            System.out.println("wire bytes: " + wire.length);

            // Read back via IpcStreamReader.
            try (RootAllocator readAlloc = new RootAllocator();
                 IpcStreamReader r = new IpcStreamReader(new ByteArrayInputStream(wire), readAlloc)) {
                Schema wireSchema = r.wireSchema();
                System.out.println("read schema: " + wireSchema);
                assertEquals(2, wireSchema.getFields().size());
                assertEquals("n", wireSchema.getFields().get(0).getName());
                assertEquals("c", wireSchema.getFields().get(1).getName());
                assertNotNull(wireSchema.getFields().get(1).getDictionary(),
                        "c should still carry DictionaryEncoding on the wire schema");

                java.util.Map<String, String> meta = r.readNextBatch();
                assertNotNull(meta, "first batch should be present");
                VectorSchemaRoot root = r.root();
                assertEquals(4, root.getRowCount(), "expected 4 rows back");

                // Check c column resolves through the dict
                DictionaryProvider rp = r.dictionaryProvider();
                assertNotNull(rp.lookup(dictId), "reader must have dict 0");
                Dictionary readDict = rp.lookup(dictId);
                VarCharVector readDictVec = (VarCharVector) readDict.getVector();
                assertEquals(3, readDictVec.getValueCount(), "dict should have 3 values");
                assertEquals("red", new String(readDictVec.get(0)));
                assertEquals("green", new String(readDictVec.get(1)));
                assertEquals("blue", new String(readDictVec.get(2)));

                FieldVector cBack = root.getVector("c");
                System.out.println("c back: " + cBack.getClass().getSimpleName() + " field=" + cBack.getField());
                TinyIntVector cIdxBack = (TinyIntVector) cBack;
                assertEquals(0, cIdxBack.get(0));
                assertEquals(1, cIdxBack.get(1));
                assertEquals(true, cIdxBack.isNull(2));
                assertEquals(2, cIdxBack.get(3));
            }
        }
    }
}
