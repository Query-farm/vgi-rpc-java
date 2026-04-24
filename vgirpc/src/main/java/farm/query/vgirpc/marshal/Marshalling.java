// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.marshal;

import farm.query.vgirpc.schema.ArrowSerializableRecord;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.MapWriter;
import org.apache.arrow.vector.holders.NullableVarCharHolder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.lang.reflect.RecordComponent;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Convert between Java values (primitives, Strings, byte[], Optionals, Lists,
 * Maps, enums, {@link ArrowSerializableRecord}s) and Arrow {@link VectorSchemaRoot}
 * row vectors. Mirrors the marshalling Python performs on the params/result
 * batches.
 */
public final class Marshalling {

    private Marshalling() {}

    /** Build a single-row {@link VectorSchemaRoot} from a parameter map matching {@code schema}. */
    public static VectorSchemaRoot encodeRow(Schema schema, Map<String, Object> values, BufferAllocator alloc) {
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, alloc);
        root.allocateNew();
        for (int i = 0; i < schema.getFields().size(); i++) {
            Field f = schema.getFields().get(i);
            FieldVector v = root.getVector(i);
            Object val = values.get(f.getName());
            if (val instanceof Optional<?> opt) val = opt.orElse(null);
            writeScalar(v, 0, f, val);
        }
        root.setRowCount(1);
        return root;
    }

    /** Read a single-row {@link VectorSchemaRoot} into an ordered map keyed by field name. */
    public static Map<String, Object> decodeRow(VectorSchemaRoot root) {
        return decodeRow(root, null);
    }

    /** Read a single-row {@link VectorSchemaRoot} with optional dictionary resolution. */
    public static Map<String, Object> decodeRow(VectorSchemaRoot root,
            org.apache.arrow.vector.dictionary.DictionaryProvider provider) {
        return decodeRow(root, provider, null);
    }

    /**
     * Read a single-row root with optional dictionary resolution and the originating
     * wire-side schema. {@code wireSchema} is used to detect dict-encoded fields when
     * the root has been rewritten to materialise indices as integer vectors.
     */
    public static Map<String, Object> decodeRow(VectorSchemaRoot root,
            org.apache.arrow.vector.dictionary.DictionaryProvider provider,
            Schema wireSchema) {
        Map<String, Object> result = new LinkedHashMap<>();
        Schema rootSchema = root.getSchema();
        for (int i = 0; i < rootSchema.getFields().size(); i++) {
            Field rootField = rootSchema.getFields().get(i);
            Field schemaField = wireSchema != null ? wireSchema.getFields().get(i) : rootField;
            FieldVector v = root.getVector(i);
            result.put(schemaField.getName(), readWithDict(v, 0, schemaField, provider));
        }
        return result;
    }

    private static Object readWithDict(FieldVector v, int row, Field f,
            org.apache.arrow.vector.dictionary.DictionaryProvider provider) {
        org.apache.arrow.vector.types.pojo.DictionaryEncoding de = f.getDictionary();
        if (de != null && provider != null && !v.isNull(row)) {
            int idx = readIntIndex(v, row);
            org.apache.arrow.vector.dictionary.Dictionary d = provider.lookup(de.getId());
            if (d != null) {
                FieldVector dv = d.getVector();
                if (dv instanceof VarCharVector vc) {
                    return new String(vc.get(idx), java.nio.charset.StandardCharsets.UTF_8);
                }
            }
        }
        return readScalar(v, row, f);
    }

    private static int readIntIndex(FieldVector v, int row) {
        if (v instanceof TinyIntVector tv) return tv.get(row);
        if (v instanceof SmallIntVector sv) return sv.get(row);
        if (v instanceof IntVector iv) return iv.get(row);
        if (v instanceof BigIntVector bv) return (int) bv.get(row);
        throw new IllegalStateException("unsupported dict index vector: " + v.getClass());
    }

    /** Write {@code value} into vector {@code v} at {@code row} according to field {@code f}. */
    public static void writeScalar(FieldVector v, int row, Field f, Object value) {
        if (value == null) {
            v.setNull(row);
            return;
        }
        ArrowType type = f.getType();
        switch (type.getTypeID()) {
            case Int -> {
                ArrowType.Int it = (ArrowType.Int) type;
                long lv = ((Number) value).longValue();
                switch (it.getBitWidth()) {
                    case 8 -> ((TinyIntVector) v).setSafe(row, (int) lv);
                    case 16 -> ((SmallIntVector) v).setSafe(row, (int) lv);
                    case 32 -> ((IntVector) v).setSafe(row, (int) lv);
                    case 64 -> ((BigIntVector) v).setSafe(row, lv);
                    default -> throw new IllegalArgumentException("unsupported int width: " + it.getBitWidth());
                }
            }
            case FloatingPoint -> {
                ArrowType.FloatingPoint fp = (ArrowType.FloatingPoint) type;
                double dv = ((Number) value).doubleValue();
                switch (fp.getPrecision()) {
                    case SINGLE -> ((Float4Vector) v).setSafe(row, (float) dv);
                    case DOUBLE -> ((Float8Vector) v).setSafe(row, dv);
                    default -> throw new IllegalArgumentException("unsupported FP precision: " + fp.getPrecision());
                }
            }
            case Bool -> ((BitVector) v).setSafe(row, ((Boolean) value) ? 1 : 0);
            case Utf8 -> {
                String s = (value instanceof Enum<?> e) ? e.name() : value.toString();
                ((VarCharVector) v).setSafe(row, s.getBytes(StandardCharsets.UTF_8));
            }
            case Binary -> {
                byte[] bytes;
                if (value instanceof byte[] b) bytes = b;
                else if (value instanceof ArrowSerializableRecord r) bytes = RecordCodec.serializeToBytes(r);
                else throw new IllegalArgumentException("expected byte[] for Binary field, got " + value.getClass());
                ((VarBinaryVector) v).setSafe(row, bytes);
            }
            case List -> writeList(v, row, f, (List<?>) value);
            case Map -> writeMap(v, row, f, (Map<?, ?>) value);
            case Struct -> writeStruct(v, row, f, value);
            default -> throw new IllegalArgumentException("unsupported Arrow type: " + type);
        }
    }

    private static void writeList(FieldVector v, int row, Field f, List<?> values) {
        ListVector lv = (ListVector) v;
        UnionListWriter w = lv.getWriter();
        w.setPosition(row);
        w.startList();
        Field child = f.getChildren().get(0);
        for (Object e : values) {
            writeListElement(w, child, e);
        }
        w.endList();
        lv.setLastSet(row);
    }

    private static void writeListElement(ListWriter w, Field child, Object e) {
        if (e == null) { w.writeNull(); return; }
        ArrowType t = child.getType();
        switch (t.getTypeID()) {
            case Int -> {
                int bits = ((ArrowType.Int) t).getBitWidth();
                long lv = ((Number) e).longValue();
                switch (bits) {
                    case 8 -> w.tinyInt().writeTinyInt((byte) lv);
                    case 16 -> w.smallInt().writeSmallInt((short) lv);
                    case 32 -> w.integer().writeInt((int) lv);
                    default -> w.bigInt().writeBigInt(lv);
                }
            }
            case FloatingPoint -> {
                double dv = ((Number) e).doubleValue();
                switch (((ArrowType.FloatingPoint) t).getPrecision()) {
                    case SINGLE -> w.float4().writeFloat4((float) dv);
                    default -> w.float8().writeFloat8(dv);
                }
            }
            case Bool -> w.bit().writeBit(((Boolean) e) ? 1 : 0);
            case Utf8 -> {
                String s = (e instanceof Enum<?> en) ? en.name() : e.toString();
                byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
                try (var buf = farm.query.vgirpc.wire.Allocators.root().buffer(bytes.length)) {
                    buf.setBytes(0, bytes);
                    w.varChar().writeVarChar(0, bytes.length, buf);
                }
            }
            case Binary -> {
                byte[] bytes = (byte[]) e;
                try (var buf = farm.query.vgirpc.wire.Allocators.root().buffer(bytes.length)) {
                    buf.setBytes(0, bytes);
                    w.varBinary().writeVarBinary(0, bytes.length, buf);
                }
            }
            case List -> {
                ListWriter inner = w.list();
                inner.startList();
                Field grand = child.getChildren().get(0);
                for (Object x : (List<?>) e) writeListElement(inner, grand, x);
                inner.endList();
            }
            case Struct -> {
                org.apache.arrow.vector.complex.writer.BaseWriter.StructWriter sw = w.struct();
                sw.start();
                Map<String, Object> fields;
                if (e instanceof Map<?, ?> m) {
                    //noinspection unchecked
                    fields = (Map<String, Object>) m;
                } else if (e instanceof ArrowSerializableRecord r) {
                    fields = RecordCodec.toRowMap(r);
                } else {
                    throw new IllegalArgumentException("struct list element must be record or Map: " + e.getClass());
                }
                for (Field f : child.getChildren()) {
                    writeStructField(sw, f, fields.get(f.getName()));
                }
                sw.end();
            }
            default -> throw new IllegalArgumentException("unsupported list element type: " + t);
        }
    }

    private static void writeStructField(
            org.apache.arrow.vector.complex.writer.BaseWriter.StructWriter sw,
            Field f, Object v) {
        String name = f.getName();
        ArrowType t = f.getType();
        if (v == null) {
            // leave unset → null bit
            return;
        }
        switch (t.getTypeID()) {
            case Int -> {
                int bits = ((ArrowType.Int) t).getBitWidth();
                long lv = ((Number) v).longValue();
                switch (bits) {
                    case 8 -> sw.tinyInt(name).writeTinyInt((byte) lv);
                    case 16 -> sw.smallInt(name).writeSmallInt((short) lv);
                    case 32 -> sw.integer(name).writeInt((int) lv);
                    default -> sw.bigInt(name).writeBigInt(lv);
                }
            }
            case FloatingPoint -> {
                double dv = ((Number) v).doubleValue();
                switch (((ArrowType.FloatingPoint) t).getPrecision()) {
                    case SINGLE -> sw.float4(name).writeFloat4((float) dv);
                    default -> sw.float8(name).writeFloat8(dv);
                }
            }
            case Bool -> sw.bit(name).writeBit(((Boolean) v) ? 1 : 0);
            case Utf8 -> {
                String s = (v instanceof Enum<?> en) ? en.name() : v.toString();
                byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
                try (var buf = farm.query.vgirpc.wire.Allocators.root().buffer(bytes.length)) {
                    buf.setBytes(0, bytes);
                    sw.varChar(name).writeVarChar(0, bytes.length, buf);
                }
            }
            case Binary -> {
                byte[] bytes = (byte[]) v;
                try (var buf = farm.query.vgirpc.wire.Allocators.root().buffer(bytes.length)) {
                    buf.setBytes(0, bytes);
                    sw.varBinary(name).writeVarBinary(0, bytes.length, buf);
                }
            }
            case Struct -> {
                org.apache.arrow.vector.complex.writer.BaseWriter.StructWriter inner = sw.struct(name);
                inner.start();
                Map<String, Object> nested;
                if (v instanceof Map<?, ?> m) { //noinspection unchecked
                    nested = (Map<String, Object>) m;
                } else if (v instanceof ArrowSerializableRecord r) { nested = RecordCodec.toRowMap(r);
                } else throw new IllegalArgumentException("struct field value must be record or Map: " + v.getClass());
                for (Field c : f.getChildren()) writeStructField(inner, c, nested.get(c.getName()));
                inner.end();
            }
            case List -> {
                ListWriter lw = sw.list(name);
                lw.startList();
                Field child = f.getChildren().get(0);
                for (Object e : (List<?>) v) writeListElement(lw, child, e);
                lw.endList();
            }
            default -> throw new IllegalArgumentException("unsupported struct field type: " + t);
        }
    }

    private static void writeMap(FieldVector v, int row, Field f, Map<?, ?> values) {
        MapVector mv = (MapVector) v;
        UnionMapWriter w = mv.getWriter();
        w.setPosition(row);
        w.startMap();
        Field entries = f.getChildren().get(0);
        Field keyField = entries.getChildren().get(0);
        Field valField = entries.getChildren().get(1);
        for (Map.Entry<?, ?> e : values.entrySet()) {
            w.startEntry();
            writeMapKey(w, keyField, e.getKey());
            writeMapValue(w, valField, e.getValue());
            w.endEntry();
        }
        w.endMap();
    }

    private static void writeMapKey(MapWriter w, Field f, Object key) {
        switch (f.getType().getTypeID()) {
            case Utf8 -> {
                byte[] bytes = ((String) key).getBytes(StandardCharsets.UTF_8);
                try (var buf = farm.query.vgirpc.wire.Allocators.root().buffer(bytes.length)) {
                    buf.setBytes(0, bytes);
                    w.key().varChar().writeVarChar(0, bytes.length, buf);
                }
            }
            case Int -> w.key().bigInt().writeBigInt(((Number) key).longValue());
            default -> throw new IllegalArgumentException("unsupported map key type: " + f.getType());
        }
    }

    private static void writeMapValue(MapWriter w, Field f, Object value) {
        switch (f.getType().getTypeID()) {
            case Int -> w.value().bigInt().writeBigInt(((Number) value).longValue());
            case Utf8 -> {
                byte[] bytes = ((String) value).getBytes(StandardCharsets.UTF_8);
                try (var buf = farm.query.vgirpc.wire.Allocators.root().buffer(bytes.length)) {
                    buf.setBytes(0, bytes);
                    w.value().varChar().writeVarChar(0, bytes.length, buf);
                }
            }
            case FloatingPoint -> w.value().float8().writeFloat8(((Number) value).doubleValue());
            case Bool -> w.value().bit().writeBit(((Boolean) value) ? 1 : 0);
            default -> throw new IllegalArgumentException("unsupported map value type: " + f.getType());
        }
    }

    private static void writeStruct(FieldVector v, int row, Field f, Object value) {
        StructVector sv = (StructVector) v;
        sv.setIndexDefined(row);
        // Expect a Map<String,Object> or an ArrowSerializableRecord
        Map<String, Object> fields;
        if (value instanceof Map<?, ?> m) {
            //noinspection unchecked
            fields = (Map<String, Object>) m;
        } else if (value instanceof ArrowSerializableRecord r) {
            fields = RecordCodec.toRowMap(r);
        } else {
            throw new IllegalArgumentException("struct value must be a record or Map: " + value.getClass());
        }
        for (int i = 0; i < f.getChildren().size(); i++) {
            Field child = f.getChildren().get(i);
            FieldVector cv = sv.getChildrenFromFields().get(i);
            writeScalar(cv, row, child, fields.get(child.getName()));
        }
    }

    /**
     * Cast a batch to a target schema by element-wise copy, allowing int→long,
     * float→double, and other Arrow-compatible widenings. Returns the same root
     * when the source schema already matches. Caller owns the returned root.
     */
    public static VectorSchemaRoot castRoot(VectorSchemaRoot source, Schema target, BufferAllocator alloc) {
        if (source.getSchema().equals(target)) return source;
        if (source.getSchema().getFields().size() != target.getFields().size()) {
            throw new IllegalArgumentException(
                    "Input schema mismatch: expected " + target.getFields().size()
                            + " fields, got " + source.getSchema().getFields().size());
        }
        int rows = source.getRowCount();
        VectorSchemaRoot out = VectorSchemaRoot.create(target, alloc);
        out.allocateNew();
        for (int i = 0; i < target.getFields().size(); i++) {
            Field tf = target.getFields().get(i);
            Field sf = source.getSchema().getFields().get(i);
            if (!tf.getName().equals(sf.getName())) {
                out.close();
                throw new IllegalArgumentException("Input schema mismatch: expected field '"
                        + tf.getName() + "', got '" + sf.getName() + "'");
            }
            FieldVector sv = source.getVector(i);
            FieldVector tv = out.getVector(i);
            copyCast(sv, sf, tv, tf, rows);
        }
        out.setRowCount(rows);
        return out;
    }

    private static void copyCast(FieldVector sv, Field sf, FieldVector tv, Field tf, int rows) {
        ArrowType st = sf.getType();
        ArrowType tt = tf.getType();
        if (st.equals(tt)) {
            // Same type; transfer via copyFromSafe
            for (int r = 0; r < rows; r++) tv.copyFromSafe(r, r, sv);
            return;
        }
        // Numeric widening cases
        if (st instanceof ArrowType.Int && tt instanceof ArrowType.FloatingPoint) {
            for (int r = 0; r < rows; r++) {
                if (sv.isNull(r)) { tv.setNull(r); continue; }
                double d = asLong(sv, r);
                if (tt instanceof ArrowType.FloatingPoint fp && fp.getPrecision() == org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE) {
                    ((Float4Vector) tv).setSafe(r, (float) d);
                } else {
                    ((Float8Vector) tv).setSafe(r, d);
                }
            }
            return;
        }
        if (st instanceof ArrowType.FloatingPoint && tt instanceof ArrowType.FloatingPoint) {
            for (int r = 0; r < rows; r++) {
                if (sv.isNull(r)) { tv.setNull(r); continue; }
                double d = asDouble(sv, r);
                if (tt instanceof ArrowType.FloatingPoint fp && fp.getPrecision() == org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE) {
                    ((Float4Vector) tv).setSafe(r, (float) d);
                } else {
                    ((Float8Vector) tv).setSafe(r, d);
                }
            }
            return;
        }
        if (st instanceof ArrowType.Int && tt instanceof ArrowType.Int) {
            for (int r = 0; r < rows; r++) {
                if (sv.isNull(r)) { tv.setNull(r); continue; }
                long v = asLong(sv, r);
                int bits = ((ArrowType.Int) tt).getBitWidth();
                switch (bits) {
                    case 8 -> ((TinyIntVector) tv).setSafe(r, (int) v);
                    case 16 -> ((SmallIntVector) tv).setSafe(r, (int) v);
                    case 32 -> ((IntVector) tv).setSafe(r, (int) v);
                    default -> ((BigIntVector) tv).setSafe(r, v);
                }
            }
            return;
        }
        throw new IllegalArgumentException("Cannot cast field '" + tf.getName() + "' from " + st + " to " + tt);
    }

    private static long asLong(FieldVector v, int r) {
        if (v instanceof TinyIntVector t) return t.get(r);
        if (v instanceof SmallIntVector s) return s.get(r);
        if (v instanceof IntVector i) return i.get(r);
        if (v instanceof BigIntVector b) return b.get(r);
        throw new IllegalStateException("not an int vector: " + v.getClass());
    }

    private static double asDouble(FieldVector v, int r) {
        if (v instanceof Float4Vector f) return f.get(r);
        if (v instanceof Float8Vector d) return d.get(r);
        throw new IllegalStateException("not a float vector: " + v.getClass());
    }

    public static Object readScalar(FieldVector v, int row, Field f) {
        if (v.isNull(row)) return null;
        ArrowType t = f.getType();
        return switch (t.getTypeID()) {
            case Int -> {
                int width = ((ArrowType.Int) t).getBitWidth();
                yield switch (width) {
                    case 8 -> (long) ((TinyIntVector) v).get(row);
                    case 16 -> (long) ((SmallIntVector) v).get(row);
                    case 32 -> (long) ((IntVector) v).get(row);
                    case 64 -> ((BigIntVector) v).get(row);
                    default -> throw new IllegalArgumentException("int width: " + width);
                };
            }
            case FloatingPoint -> switch (((ArrowType.FloatingPoint) t).getPrecision()) {
                case SINGLE -> (double) ((Float4Vector) v).get(row);
                case DOUBLE -> ((Float8Vector) v).get(row);
                default -> throw new IllegalArgumentException("FP precision");
            };
            case Bool -> ((BitVector) v).get(row) == 1;
            case Utf8 -> new String(((VarCharVector) v).get(row), StandardCharsets.UTF_8);
            case Binary -> ((VarBinaryVector) v).get(row);
            case List -> readList((ListVector) v, row, f);
            case Map -> readMap((MapVector) v, row, f);
            case Struct -> readStruct((StructVector) v, row, f);
            default -> throw new IllegalArgumentException("unsupported Arrow type: " + t);
        };
    }

    private static List<Object> readList(ListVector lv, int row, Field f) {
        Field child = f.getChildren().get(0);
        FieldVector dv = lv.getDataVector();
        int start = lv.getOffsetBuffer().getInt((long) row * 4);
        int end = lv.getOffsetBuffer().getInt(((long) row + 1) * 4);
        List<Object> out = new ArrayList<>(end - start);
        for (int i = start; i < end; i++) {
            out.add(readScalar(dv, i, child));
        }
        return out;
    }

    private static Map<Object, Object> readMap(MapVector mv, int row, Field f) {
        Field entries = f.getChildren().get(0);
        Field keyField = entries.getChildren().get(0);
        Field valField = entries.getChildren().get(1);
        StructVector entriesVec = (StructVector) mv.getDataVector();
        FieldVector keyVec = entriesVec.getChildrenFromFields().get(0);
        FieldVector valVec = entriesVec.getChildrenFromFields().get(1);
        int start = mv.getOffsetBuffer().getInt((long) row * 4);
        int end = mv.getOffsetBuffer().getInt(((long) row + 1) * 4);
        Map<Object, Object> out = new LinkedHashMap<>(end - start);
        for (int i = start; i < end; i++) {
            out.put(readScalar(keyVec, i, keyField), readScalar(valVec, i, valField));
        }
        return out;
    }

    private static Map<String, Object> readStruct(StructVector sv, int row, Field f) {
        Map<String, Object> out = new LinkedHashMap<>();
        for (int i = 0; i < f.getChildren().size(); i++) {
            Field child = f.getChildren().get(i);
            FieldVector cv = sv.getChildrenFromFields().get(i);
            out.put(child.getName(), readScalar(cv, row, child));
        }
        return out;
    }
}
