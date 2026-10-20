// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

import farm.query.vgirpc.marshal.Marshalling;
import farm.query.vgirpc.wire.Allocators;
import farm.query.vgirpc.wire.IpcStreamReader;
import farm.query.vgirpc.wire.IpcStreamWriter;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FloatingPointVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link OutputCollector#emit} reconciles a batch to the declared output schema
 * the way the reference's {@code emit} does: select the declared columns by
 * name, then cast what still differs with a value-checked cast.
 *
 * <p>Each case is one the reference was probed on (pyarrow 25.0.1, its
 * {@code select}-then-{@code cast} on a batch whose schema differs from
 * {@code output_schema}); the comment on each states pyarrow's outcome.
 * {@link OutputReconcilerCastTest} holds the value-by-value cases for strings,
 * decimals and containers.
 */
final class OutputReconcilerTest {

    private static final ArrowType I8 = new ArrowType.Int(8, true);
    private static final ArrowType I16 = new ArrowType.Int(16, true);
    private static final ArrowType I32 = new ArrowType.Int(32, true);
    private static final ArrowType I64 = new ArrowType.Int(64, true);
    private static final ArrowType U8 = new ArrowType.Int(8, false);
    private static final ArrowType U32 = new ArrowType.Int(32, false);
    private static final ArrowType F32 = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
    private static final ArrowType F64 = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);

    private static Field f(String name, ArrowType t) { return new Field(name, FieldType.nullable(t), null); }

    private static Field dict(String name, long id, ArrowType index) {
        return new Field(name, new FieldType(true, new ArrowType.Utf8(),
                new DictionaryEncoding(id, false, (ArrowType.Int) index)), null);
    }

    private static Schema schema(Field... fields) { return new Schema(List.of(fields)); }

    private static VectorSchemaRoot root(int rows, Field field, Consumer<FieldVector> fill) {
        VectorSchemaRoot r = VectorSchemaRoot.create(schema(field), Allocators.root());
        r.allocateNew();
        fill.accept(r.getVector(0));
        r.getVector(0).setValueCount(rows);
        r.setRowCount(rows);
        return r;
    }

    private static VectorSchemaRoot ints(ArrowType t, long... vs) {
        return root(vs.length, f("v", t), v -> {
            for (int i = 0; i < vs.length; i++) ((BaseIntVector) v).setWithPossibleTruncate(i, vs[i]);
        });
    }

    private static VectorSchemaRoot floats(ArrowType t, double... vs) {
        return root(vs.length, f("v", t), v -> {
            for (int i = 0; i < vs.length; i++) ((FloatingPointVector) v).setWithPossibleTruncate(i, vs[i]);
        });
    }

    private static VectorSchemaRoot strings(String name, ArrowType t, String... vs) {
        return root(vs.length, f(name, t), v -> {
            for (int i = 0; i < vs.length; i++) {
                byte[] b = vs[i].getBytes(StandardCharsets.UTF_8);
                if (v instanceof VarCharVector x) x.setSafe(i, b);
                else if (v instanceof LargeVarCharVector x) x.setSafe(i, b);
                else ((VarBinaryVector) v).setSafe(i, b);
            }
        });
    }

    /** Emit through a collector declaring {@code target}; return the entry. */
    private static OutputCollector.Entry emit(Schema target, VectorSchemaRoot root, DictionaryProvider p) {
        OutputCollector out = new OutputCollector(target, "test", true);
        out.emit(root, null, p);
        OutputCollector.Entry e = out.dataEntry();
        assertEquals(target, e.root().getSchema(), "the batch must carry the declared schema");
        return e;
    }

    private static List<Object> values(OutputCollector.Entry e, int col) {
        List<Object> got = new ArrayList<>();
        FieldVector v = e.root().getVector(col);
        Field field = e.root().getSchema().getFields().get(col);
        for (int r = 0; r < e.root().getRowCount(); r++) {
            if (field.getDictionary() != null) {
                Dictionary d = e.dictionaryProvider().lookup(field.getDictionary().getId());
                got.add(v.isNull(r) ? null
                        : d.getVector().getObject((int) ((BaseIntVector) v).getValueAsLong(r)).toString());
            } else {
                Object o = v.getObject(r);
                got.add(o instanceof org.apache.arrow.vector.util.Text t ? t.toString() : o);
            }
        }
        return got;
    }

    private static List<Object> emitValues(Schema target, VectorSchemaRoot root) {
        OutputCollector.Entry e = emit(target, root, null);
        try (VectorSchemaRoot ignored = e.root()) {
            return values(e, 0);
        }
    }

    private static IllegalArgumentException refused(Schema target, VectorSchemaRoot root) {
        OutputCollector out = new OutputCollector(target, "test", true);
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> out.emit(root));
        assertEquals(0, out.entries().size(), "a refused batch must not be buffered");
        return e;
    }

    // --- integers ----------------------------------------------------------

    @Test void int32ToInt64() {  // pyarrow: OK
        assertEquals(List.of(1L, 2L), emitValues(schema(f("v", I64)), ints(I32, 1, 2)));
    }

    @Test void int64ToInt32WhenTheValuesFit() {  // pyarrow: OK
        assertEquals(List.of(1, 2), emitValues(schema(f("v", I32)), ints(I64, 1, 2)));
    }

    @Test void int64ToInt32Overflow() {  // pyarrow: ArrowInvalid "Integer value 1099511627776 not in range"
        assertTrue(refused(schema(f("v", I32)), ints(I64, 1L << 40)).getMessage().contains("not in range"));
    }

    @Test void negativeToUnsigned() {  // pyarrow: ArrowInvalid "Integer value -1 not in range: 0 to 255"
        assertTrue(refused(schema(f("v", U8)), ints(I64, -1)).getMessage().contains("0 to 255"));
    }

    @Test void uint32ToInt64() {  // pyarrow: OK
        VectorSchemaRoot r = root(1, f("v", U32), v -> ((UInt4Vector) v).setSafe(0, 7));
        assertEquals(List.of(7L), emitValues(schema(f("v", I64)), r));
    }

    @Test void boolToInt64() {  // pyarrow: OK
        VectorSchemaRoot r = root(2, f("v", ArrowType.Bool.INSTANCE), v -> {
            ((BitVector) v).setSafe(0, 1);
            ((BitVector) v).setSafe(1, 0);
        });
        assertEquals(List.of(1L, 0L), emitValues(schema(f("v", I64)), r));
    }

    // --- floating point ----------------------------------------------------

    @Test void int64ToFloat64() {  // pyarrow: OK
        assertEquals(List.of(3.0), emitValues(schema(f("v", F64)), ints(I64, 3)));
    }

    @Test void int64ToFloat64Imprecise() {  // pyarrow: ArrowInvalid "... not in range: -9007199254740992 to ..."
        refused(schema(f("v", F64)), ints(I64, (1L << 53) + 1));
    }

    @Test void float64ToInt64Exact() {  // pyarrow: OK
        assertEquals(List.of(3L), emitValues(schema(f("v", I64)), floats(F64, 3.0)));
    }

    @Test void float64ToInt64Truncating() {  // pyarrow: ArrowInvalid "Float value 3.500000 was truncated"
        assertTrue(refused(schema(f("v", I64)), floats(F64, 3.5)).getMessage().contains("truncated"));
    }

    @Test void float32ToFloat64AndBack() {  // pyarrow: OK both ways
        assertEquals(List.of(1.5), emitValues(schema(f("v", F64)), floats(F32, 1.5)));
        assertEquals(List.of(0.1f), emitValues(schema(f("v", F32)), floats(F64, 0.1)));
    }

    // --- strings and binary ------------------------------------------------

    @Test void stringToLargeStringAndBack() {  // pyarrow: OK both ways
        assertEquals(List.of("a"), emitValues(schema(f("s", ArrowType.LargeUtf8.INSTANCE)),
                strings("s", ArrowType.Utf8.INSTANCE, "a")));
        assertEquals(List.of("a"), emitValues(schema(f("s", ArrowType.Utf8.INSTANCE)),
                strings("s", ArrowType.LargeUtf8.INSTANCE, "a")));
    }

    @Test void stringToBinary() {  // pyarrow: OK
        OutputCollector.Entry e = emit(schema(f("s", ArrowType.Binary.INSTANCE)),
                strings("s", ArrowType.Utf8.INSTANCE, "a"), null);
        try (VectorSchemaRoot r = e.root()) {
            assertEquals("a", new String(((VarBinaryVector) r.getVector(0)).get(0), StandardCharsets.UTF_8));
        }
    }

    @Test void invalidUtf8BinaryToString() {  // pyarrow: ArrowInvalid "Invalid UTF8 payload"
        VectorSchemaRoot r = root(1, f("s", ArrowType.Binary.INSTANCE),
                v -> ((VarBinaryVector) v).setSafe(0, new byte[] {(byte) 0xff}));
        assertTrue(refused(schema(f("s", ArrowType.Utf8.INSTANCE)), r).getMessage().contains("UTF-8"));
    }

    @Test void int64ToString() {  // pyarrow: OK ('5'); OutputReconcilerCastTest has the formatting cases
        assertEquals(List.of("5"), emitValues(schema(f("v", ArrowType.Utf8.INSTANCE)), ints(I64, 5)));
    }

    @Test void stringToInt64() {  // pyarrow: OK (parses '5'); 'x' fails to parse
        assertEquals(List.of(5L), emitValues(schema(f("v", I64)), strings("v", ArrowType.Utf8.INSTANCE, "5")));
        assertTrue(refused(schema(f("v", I64)), strings("v", ArrowType.Utf8.INSTANCE, "x"))
                .getMessage().contains("Failed to parse string: 'x' as a scalar of type int64"));
    }

    // --- temporal / null ---------------------------------------------------

    @Test void timestampMillisToMicros() {  // pyarrow: OK
        ArrowType ms = new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
        VectorSchemaRoot r = root(1, f("t", ms), v -> ((TimeStampVector) v).setSafe(0, 1));
        OutputCollector.Entry e = emit(schema(f("t", new ArrowType.Timestamp(TimeUnit.MICROSECOND, null))), r, null);
        try (VectorSchemaRoot out = e.root()) {
            assertEquals(1000L, ((TimeStampVector) out.getVector(0)).get(0));
        }
    }

    @Test void timestampMicrosToMillisLossy() {  // pyarrow: ArrowInvalid "would lose data: 1001"
        ArrowType us = new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
        VectorSchemaRoot r = root(1, f("t", us), v -> ((TimeStampVector) v).setSafe(0, 1001));
        assertTrue(refused(schema(f("t", new ArrowType.Timestamp(TimeUnit.MILLISECOND, null))), r)
                .getMessage().contains("lose data"));
    }

    @Test void timestampNaiveToZoned() {  // pyarrow: OK
        ArrowType naive = new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
        VectorSchemaRoot r = root(1, f("t", naive), v -> ((TimeStampVector) v).setSafe(0, 1));
        OutputCollector.Entry e = emit(schema(f("t", new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC"))), r, null);
        try (VectorSchemaRoot out = e.root()) {
            assertEquals(1L, ((TimeStampVector) out.getVector(0)).get(0));
        }
    }

    @Test void date32ToDate64() {  // pyarrow: OK
        VectorSchemaRoot r = root(1, f("d", new ArrowType.Date(DateUnit.DAY)), v -> ((DateDayVector) v).setSafe(0, 1));
        OutputCollector.Entry e = emit(schema(f("d", new ArrowType.Date(DateUnit.MILLISECOND))), r, null);
        try (VectorSchemaRoot out = e.root()) {
            assertEquals(86_400_000L, ((org.apache.arrow.vector.DateMilliVector) out.getVector(0)).get(0));
        }
    }

    @Test void nullTypedToInt64() {  // pyarrow: OK (all null)
        VectorSchemaRoot r = root(1, f("v", ArrowType.Null.INSTANCE), v -> {});
        assertEquals(java.util.Collections.singletonList(null), emitValues(schema(f("v", I64)), r));
    }

    // --- names -------------------------------------------------------------

    @Test void reordersColumnsByName() {  // pyarrow: OK (select)
        VectorSchemaRoot r = VectorSchemaRoot.create(schema(f("b", I64), f("a", I64)), Allocators.root());
        r.allocateNew();
        ((BaseIntVector) r.getVector(0)).setWithPossibleTruncate(0, 1);
        ((BaseIntVector) r.getVector(1)).setWithPossibleTruncate(0, 2);
        r.setRowCount(1);
        OutputCollector.Entry e = emit(schema(f("a", I64), f("b", I64)), r, null);
        try (VectorSchemaRoot out = e.root()) {
            assertEquals(2L, ((BaseIntVector) out.getVector("a")).getValueAsLong(0));
            assertEquals(1L, ((BaseIntVector) out.getVector("b")).getValueAsLong(0));
        }
    }

    @Test void dropsUndeclaredColumns() {  // pyarrow: OK (select)
        VectorSchemaRoot r = VectorSchemaRoot.create(schema(f("a", I64), f("x", I64)), Allocators.root());
        r.allocateNew();
        ((BaseIntVector) r.getVector(0)).setWithPossibleTruncate(0, 1);
        r.setRowCount(1);
        assertEquals(List.of(1L), emitValues(schema(f("a", I64)), r));
    }

    @Test void refusesAMissingColumn() {  // pyarrow: ValueError "missing column(s)"
        assertTrue(refused(schema(f("a", I64)), ints(I64, 1)).getMessage().contains("missing"));
    }

    @Test void listItemNameIsNotSignificant() {
        // Arrow Java names a list item "$data$", pyarrow "item"; the reference's cast renames freely.
        Field declaredItem = new Field("item", FieldType.notNullable(I64), null);
        Schema target = schema(new Field("l", FieldType.nullable(ArrowType.List.INSTANCE), List.of(declaredItem)));
        VectorSchemaRoot r = VectorSchemaRoot.create(schema(new Field("l", FieldType.nullable(ArrowType.List.INSTANCE),
                List.of(new Field("$data$", FieldType.nullable(I64), null)))), Allocators.root());
        ListVector lv = (ListVector) r.getVector(0);
        UnionListWriter w = lv.getWriter();
        w.setPosition(0);
        w.startList();
        w.bigInt().writeBigInt(9);
        w.endList();
        lv.setValueCount(1);
        r.setRowCount(1);
        OutputCollector.Entry e = emit(target, r, null);
        try (VectorSchemaRoot out = e.root()) {
            assertEquals(List.of(9L), ((ListVector) out.getVector(0)).getObject(0));
        }
    }

    @Test void unionTypeIdsAreNotSignificant() {
        // Arrow Java states a union's type ids as MinorType ordinals ([24, 3] for
        // utf8 + smallint); the writer canonicalises them to [0, 1] on the way out.
        List<Field> kids = List.of(f("str", ArrowType.Utf8.INSTANCE), f("small", I16));
        Field stated = new Field("u", FieldType.nullable(
                new ArrowType.Union(org.apache.arrow.vector.types.UnionMode.Sparse, new int[] {24, 3})), kids);
        Field declared = new Field("u", FieldType.nullable(
                new ArrowType.Union(org.apache.arrow.vector.types.UnionMode.Sparse, new int[] {0, 1})), kids);
        Field wrapped = new Field("s", FieldType.nullable(ArrowType.Struct.INSTANCE), List.of(stated));
        Field wrappedDeclared = new Field("s", FieldType.nullable(ArrowType.Struct.INSTANCE), List.of(declared));
        VectorSchemaRoot r = VectorSchemaRoot.create(schema(stated, wrapped), Allocators.root());
        r.setRowCount(0);
        OutputCollector.Entry e = emit(schema(declared, wrappedDeclared), r, null);
        e.root().close();
    }

    @Test void listOfInt32ToListOfInt64() {  // pyarrow: OK
        Schema target = schema(new Field("l", FieldType.nullable(ArrowType.List.INSTANCE),
                List.of(new Field("item", FieldType.nullable(I64), null))));
        VectorSchemaRoot r = VectorSchemaRoot.create(schema(new Field("l", FieldType.nullable(ArrowType.List.INSTANCE),
                List.of(new Field("item", FieldType.nullable(I32), null)))), Allocators.root());
        ListVector lv = (ListVector) r.getVector(0);
        UnionListWriter w = lv.getWriter();
        w.setPosition(0);
        w.startList();
        w.integer().writeInt(9);
        w.endList();
        lv.setValueCount(1);
        r.setRowCount(1);
        OutputCollector.Entry e = emit(target, r, null);
        try (VectorSchemaRoot out = e.root()) {
            assertEquals(List.of(9L), ((ListVector) out.getVector(0)).getObject(0));
        }
    }

    // --- dictionaries ------------------------------------------------------

    @Test void dictionaryToItsValueType() {  // pyarrow: OK
        try (Marshalling.EncodedRow row = Marshalling.encodeRowForWire(schema(dict("s", 0, I16)),
                Map.of("s", "a"), Allocators.root())) {
            // The collector takes the root; the encoded row keeps the dictionary alive.
            VectorSchemaRoot handed = new VectorSchemaRoot(row.root().getSchema(),
                    new ArrayList<>(row.root().getFieldVectors()), 1);
            OutputCollector out = new OutputCollector(schema(f("s", ArrowType.Utf8.INSTANCE)), "test", true);
            out.emit(handed, null, row.provider());
            OutputCollector.Entry e = out.dataEntry();
            try (VectorSchemaRoot r = e.root()) {
                assertEquals(schema(f("s", ArrowType.Utf8.INSTANCE)), r.getSchema());
                assertEquals("a", new String(((VarCharVector) r.getVector(0)).get(0), StandardCharsets.UTF_8));
            }
        }
    }

    @Test void plainStringToDictionary() {  // pyarrow: OK
        Schema target = schema(dict("s", 0, I16));
        OutputCollector.Entry e = emit(target, strings("s", ArrowType.Utf8.INSTANCE, "a", "b", "a"), null);
        try (VectorSchemaRoot r = e.root()) {
            assertEquals(List.of("a", "b", "a"), values(e, 0));
            assertEquals(2, e.dictionaryProvider().lookup(0).getVector().getValueCount(), "distinct values only");
            // And the batch must survive the wire.
            assertEquals(List.of("a", "b", "a"), roundTrip(r, e.dictionaryProvider()));
        }
    }

    @Test void dictionaryToADifferentIndexWidth() {  // pyarrow: OK (dict32 -> dict16)
        VarCharVector dv = new VarCharVector("d", Allocators.root());
        dv.allocateNew();
        dv.setSafe(0, "a".getBytes(StandardCharsets.UTF_8));
        dv.setSafe(1, "b".getBytes(StandardCharsets.UTF_8));
        dv.setValueCount(2);
        Dictionary d32 = new Dictionary(dv, new DictionaryEncoding(0L, false, (ArrowType.Int) I32));
        try (VarCharVector plain = new VarCharVector("s", Allocators.root())) {
            plain.allocateNew();
            plain.setSafe(0, "b".getBytes(StandardCharsets.UTF_8));
            plain.setSafe(1, "a".getBytes(StandardCharsets.UTF_8));
            plain.setValueCount(2);
            FieldVector idx = (FieldVector) DictionaryEncoder.encode(plain, d32);
            VectorSchemaRoot r = new VectorSchemaRoot(List.of(idx.getField()), List.of(idx), 2);
            DictionaryProvider.MapDictionaryProvider p = new DictionaryProvider.MapDictionaryProvider();
            p.put(d32);
            OutputCollector.Entry e = emit(schema(dict("s", 0, I16)), r, p);
            try (VectorSchemaRoot out = e.root()) {
                assertEquals(List.of("b", "a"), values(e, 0));
            }
        } finally {
            dv.close();
        }
    }

    // --- nullability -------------------------------------------------------

    @Test void refusesNullsInANonNullableField() {  // pyarrow: ValueError "with null values to non-nullable"
        Schema target = schema(new Field("v", FieldType.notNullable(I64), null));
        VectorSchemaRoot r = root(1, f("v", I32), v -> ((org.apache.arrow.vector.IntVector) v).setNull(0));
        assertTrue(refused(target, r).getMessage().contains("non-nullable"));
    }

    private static List<Object> roundTrip(VectorSchemaRoot root, DictionaryProvider p) {
        try {
            ByteArrayOutputStream sink = new ByteArrayOutputStream();
            try (IpcStreamWriter w = new IpcStreamWriter(sink)) {
                w.writeBatch(root, null, p);
            }
            List<Object> got = new ArrayList<>();
            try (IpcStreamReader rd = new IpcStreamReader(new ByteArrayInputStream(sink.toByteArray()), Allocators.root())) {
                rd.readNextBatch();
                VectorSchemaRoot back = rd.root();
                for (int i = 0; i < back.getRowCount(); i++) {
                    got.add(Marshalling.readScalar(back.getVector(0), i, back.getSchema().getFields().get(0)) == null
                            ? null : String.valueOf(rd.resolveDictValue(0, (int) ((BaseIntVector) back.getVector(0)).getValueAsLong(i))));
                }
            }
            return got;
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }
}
