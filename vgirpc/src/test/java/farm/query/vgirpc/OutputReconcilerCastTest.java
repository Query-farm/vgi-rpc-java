// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.FloatingPointVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The casts {@link OutputCollector#emit} performs when a batch's types differ
 * from the declared output schema, against the reference's outcomes.
 *
 * <p>Every expectation here is pyarrow 25.0.1's ({@code Array.cast}, safe),
 * probed value by value; an {@code ok(...)} carries pyarrow's result verbatim
 * and a {@code refused(...)} carries a fragment of its error. The one place
 * this port deliberately departs from the reference is marked: a declared
 * struct child the batch lacks, which pyarrow fills with nulls.
 */
final class OutputReconcilerCastTest {

    // --- types ---------------------------------------------------------------

    static final ArrowType I8 = new ArrowType.Int(8, true);
    static final ArrowType I16 = new ArrowType.Int(16, true);
    static final ArrowType I32 = new ArrowType.Int(32, true);
    static final ArrowType I64 = new ArrowType.Int(64, true);
    static final ArrowType U8 = new ArrowType.Int(8, false);
    static final ArrowType U64 = new ArrowType.Int(64, false);
    static final ArrowType F32 = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
    static final ArrowType F64 = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
    static final ArrowType STR = ArrowType.Utf8.INSTANCE;
    static final ArrowType LSTR = ArrowType.LargeUtf8.INSTANCE;
    static final ArrowType BIN = ArrowType.Binary.INSTANCE;
    static final ArrowType BOOL = ArrowType.Bool.INSTANCE;

    static ArrowType dec(int p, int s) { return new ArrowType.Decimal(p, s, 128); }
    static ArrowType dec256(int p, int s) { return new ArrowType.Decimal(p, s, 256); }

    static Field f(String name, ArrowType t, Field... kids) {
        return new Field(name, FieldType.nullable(t), List.of(kids));
    }

    static Field col(ArrowType t, Field... kids) { return f("v", t, kids); }

    static Field list(Field item) { return col(ArrowType.List.INSTANCE, item); }

    static Field item(ArrowType t, Field... kids) { return f("item", t, kids); }

    static Field map(ArrowType k, ArrowType v) {
        Field entries = new Field("entries", FieldType.notNullable(ArrowType.Struct.INSTANCE),
                List.of(new Field("key", FieldType.notNullable(k), null), f("value", v)));
        return col(new ArrowType.Map(false), entries);
    }

    static BigDecimal d(String s) { return new BigDecimal(s); }

    // --- harness -------------------------------------------------------------

    /** Emit {@code values} typed {@code source} against a schema declaring {@code target}; return the values. */
    static List<Object> cast(Field source, Field target, Object... values) {
        VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(List.of(source)), Allocators.root());
        FieldVector v = root.getVector(0);
        v.allocateNew();
        for (int i = 0; i < values.length; i++) put(v, i, values[i]);
        v.setValueCount(values.length);
        root.setRowCount(values.length);
        OutputCollector out = new OutputCollector(new Schema(List.of(target)), "test", true);
        out.emit(root);
        try (VectorSchemaRoot r = out.dataEntry().root()) {
            assertEquals(new Schema(List.of(target)), r.getSchema());
            List<Object> got = new ArrayList<>();
            for (int i = 0; i < r.getRowCount(); i++) got.add(get(r.getVector(0), i));
            return got;
        }
    }

    static void ok(Field source, Field target, List<?> expected, Object... values) {
        assertEquals(expected, cast(source, target, values), source.getType() + " -> " + target.getType());
    }

    static void ok(ArrowType source, ArrowType target, List<?> expected, Object... values) {
        ok(col(source), col(target), expected, values);
    }

    static void refused(Field source, Field target, String fragment, Object... values) {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> cast(source, target, values),
                source.getType() + " -> " + target.getType() + " " + Arrays.toString(values));
        assertTrue(e.getMessage().contains(fragment), "expected '" + fragment + "' in: " + e.getMessage());
    }

    static void refused(ArrowType source, ArrowType target, String fragment, Object... values) {
        refused(col(source), col(target), fragment, values);
    }

    /** Write a Java value into row {@code i}, recursing through containers. */
    static void put(FieldVector v, int i, Object val) {
        if (val == null) {
            if (v instanceof ListVector l) l.setNull(i);
            else if (v instanceof LargeListVector l) l.setNull(i);
            else if (v instanceof FixedSizeListVector l) l.setNull(i);
            else if (v instanceof StructVector s) s.setNull(i);
            else if (v instanceof org.apache.arrow.vector.BaseFixedWidthVector x) x.setNull(i);
            else if (v instanceof org.apache.arrow.vector.BaseVariableWidthVector x) x.setNull(i);
            else ((org.apache.arrow.vector.BaseLargeVariableWidthVector) v).setNull(i);
            return;
        }
        if (v instanceof FixedSizeListVector l) {
            List<?> xs = (List<?>) val;
            l.setNotNull(i);
            for (int j = 0; j < xs.size(); j++) put(l.getDataVector(), i * l.getListSize() + j, xs.get(j));
            l.getDataVector().setValueCount((i + 1) * l.getListSize());
            return;
        }
        if (v instanceof ListVector || v instanceof LargeListVector) {
            List<?> xs = (List<?>) val;
            FieldVector data = v instanceof ListVector l ? l.getDataVector() : ((LargeListVector) v).getDataVector();
            long start = v instanceof ListVector l ? l.startNewValue(i) : ((LargeListVector) v).startNewValue(i);
            for (int j = 0; j < xs.size(); j++) {
                Object x = xs.get(j);
                if (v instanceof MapVector) {
                    Object[] kv = (Object[]) x;
                    StructVector e = (StructVector) data;
                    e.setIndexDefined((int) start + j);
                    put(e.getChildrenFromFields().get(0), (int) start + j, kv[0]);
                    put(e.getChildrenFromFields().get(1), (int) start + j, kv[1]);
                } else {
                    put(data, (int) start + j, x);
                }
            }
            data.setValueCount((int) start + xs.size());
            if (v instanceof ListVector l) l.endValue(i, xs.size());
            else ((LargeListVector) v).endValue(i, xs.size());
            return;
        }
        if (v instanceof StructVector s) {
            s.setIndexDefined(i);
            Map<?, ?> m = (Map<?, ?>) val;
            for (FieldVector c : s.getChildrenFromFields()) {
                put(c, i, m.get(c.getName()));
                c.setValueCount(Math.max(c.getValueCount(), i + 1));
            }
            return;
        }
        if (v instanceof BitVector b) b.setSafe(i, ((Boolean) val) ? 1 : 0);
        else if (v instanceof BaseIntVector b) b.setWithPossibleTruncate(i, val instanceof BigInteger bi
                ? bi.longValue() : ((Number) val).longValue());
        else if (v instanceof FloatingPointVector fp) fp.setWithPossibleTruncate(i, ((Number) val).doubleValue());
        else if (v instanceof DecimalVector dv) dv.setSafe(i, (BigDecimal) val);
        else if (v instanceof Decimal256Vector dv) dv.setSafe(i, (BigDecimal) val);
        else if (v instanceof VarCharVector x) x.setSafe(i, bytes(val));
        else if (v instanceof LargeVarCharVector x) x.setSafe(i, bytes(val));
        else if (v instanceof VarBinaryVector x) x.setSafe(i, bytes(val));
        else if (v instanceof LargeVarBinaryVector x) x.setSafe(i, bytes(val));
        else throw new IllegalStateException("no writer for " + v.getClass());
    }

    static byte[] bytes(Object val) {
        return val instanceof byte[] b ? b : ((String) val).getBytes(StandardCharsets.UTF_8);
    }

    /** Read row {@code i} back as plain Java values (pyarrow's to_pylist shapes). */
    static Object get(FieldVector v, int i) {
        if (v.isNull(i)) return null;
        if (v instanceof FixedSizeListVector l) {
            List<Object> xs = new ArrayList<>();
            for (int j = 0; j < l.getListSize(); j++) xs.add(get(l.getDataVector(), i * l.getListSize() + j));
            return xs;
        }
        if (v instanceof ListVector || v instanceof LargeListVector) {
            long s;
            long e;
            FieldVector data;
            if (v instanceof LargeListVector l) {
                s = l.getOffsetBuffer().getLong((long) i * 8);
                e = l.getOffsetBuffer().getLong((long) (i + 1) * 8);
                data = l.getDataVector();
            } else {
                ListVector l = (ListVector) v;
                s = l.getOffsetBuffer().getInt((long) i * 4);
                e = l.getOffsetBuffer().getInt((long) (i + 1) * 4);
                data = l.getDataVector();
            }
            List<Object> xs = new ArrayList<>();
            for (long j = s; j < e; j++) {
                if (v instanceof MapVector) {
                    StructVector entry = (StructVector) data;
                    xs.add(List.of(get(entry.getChildrenFromFields().get(0), (int) j),
                            get(entry.getChildrenFromFields().get(1), (int) j)));
                } else {
                    xs.add(get(data, (int) j));
                }
            }
            return xs;
        }
        if (v instanceof StructVector s) {
            Map<String, Object> m = new LinkedHashMap<>();
            for (FieldVector c : s.getChildrenFromFields()) m.put(c.getName(), get(c, i));
            return m;
        }
        if (v instanceof BitVector b) return b.get(i) != 0;
        if (v instanceof UInt8Vector u) return new BigInteger(Long.toUnsignedString(u.get(i)));
        if (v instanceof BaseIntVector b) return b.getValueAsLong(i);
        if (v instanceof Float4Vector fv) return (double) fv.get(i);
        if (v instanceof FloatingPointVector fp) return fp.getValueAsDouble(i);
        if (v instanceof DecimalVector dv) return dv.getObject(i);
        if (v instanceof Decimal256Vector dv) return dv.getObject(i);
        if (v instanceof VarCharVector x) return new String(x.get(i), StandardCharsets.UTF_8);
        if (v instanceof LargeVarCharVector x) return new String(x.get(i), StandardCharsets.UTF_8);
        if (v instanceof VarBinaryVector x) return "b:" + new String(x.get(i), StandardCharsets.UTF_8);
        if (v instanceof LargeVarBinaryVector x) return "b:" + new String(x.get(i), StandardCharsets.UTF_8);
        throw new IllegalStateException("no reader for " + v.getClass());
    }

    static Map<String, Object> struct(Object... kv) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) m.put((String) kv[i], kv[i + 1]);
        return m;
    }

    // --- numbers -> string ---------------------------------------------------

    @Test void intToString() {
        ok(I64, STR, List.of("5", "-3", "0", "4611686018427387904"), 5L, -3L, 0L, 1L << 62);
        ok(U64, STR, List.of("18446744073709551615"), -1L);
        ok(I8, LSTR, List.of("-128"), -128L);
    }

    @Test void doubleToString() {
        ok(F64, STR, List.of("1.5", "0.1", "3", "-0", "1e+20", "1e+21", "1e-7", "123456789.125",
                        "0.3333333333333333", "nan", "inf", "-inf", "1e+300", "5e-324", "100", "1e+16", "1e+15", "123456"),
                1.5, 0.1, 3.0, -0.0, 1e20, 1e21, 1e-7, 123456789.125, 1.0 / 3, Double.NaN,
                Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 1e300, 5e-324, 100.0, 1e16, 1e15, 123456.0);
    }

    /** Python's {@code 10.0 ** k}: the double nearest 10^k (Math.pow is not guaranteed to be). */
    static double pow10(int k) { return Double.parseDouble("1e" + k); }

    @Test void doubleToStringNotationBoundaries() {
        List<Object> powers = new ArrayList<>();
        for (int k = 0; k < 23; k++) powers.add(pow10(k));
        for (int k = 0; k < 23; k++) powers.add(1.5 * pow10(k));
        powers.addAll(List.of(123456789012345.0, 1234567890123456.0, 12345678901234567.0, 0x1p53, 0x1p60));
        ok(F64, STR, List.of("1", "10", "100", "1000", "10000", "100000", "1000000", "10000000", "100000000",
                        "1000000000", "1e+10", "1e+11", "1e+12", "1e+13", "1e+14", "1e+15", "1e+16", "1e+17", "1e+18",
                        "1e+19", "1e+20", "1e+21", "1e+22", "1.5", "15", "150", "1500", "15000", "150000", "1500000",
                        "15000000", "150000000", "1500000000", "1.5e+10", "1.5e+11", "1.5e+12", "1.5e+13", "1.5e+14",
                        "1.5e+15", "1.5e+16", "1.5e+17", "1.5e+18", "1.5e+19", "1.5e+20", "1.5e+21", "1.5e+22",
                        "1.23456789012345e+14", "1.234567890123456e+15", "1.2345678901234568e+16",
                        "9.007199254740992e+15", "1.152921504606847e+18"),
                powers.toArray());
        List<Object> small = new ArrayList<>();
        for (int k = 1; k < 10; k++) small.add(pow10(-k));
        for (int k = 1; k < 10; k++) small.add(1.5 * pow10(-k));
        small.addAll(List.of(0.000123, 0.0001234, 1.25e-5));
        ok(F64, STR, List.of("0.1", "0.01", "0.001", "0.0001", "0.00001", "0.000001", "1e-7", "1e-8", "1e-9",
                        "0.15000000000000002", "0.015", "0.0015", "0.00015000000000000001", "0.000015000000000000002",
                        "0.0000015", "1.5e-7", "1.5000000000000002e-8", "1.5000000000000002e-9", "0.000123",
                        "0.0001234", "0.0000125"),
                small.toArray());
        ok(F64, STR, List.of("-1.5", "-1e+20", "-0.0001", "-123"), -1.5, -1e20, -0.0001, -123.0);
    }

    @Test void floatToString() {
        ok(F32, STR, List.of("0.1", "1.5", "3", "1e+20", "16777216", "0.33333334"),
                0.1f, 1.5f, 3.0f, 1e20f, 16777217.0f, 1.0f / 3);
        ok(F32, STR, List.of("1", "10", "1000000000", "1e+10", "1e+11", "0.000015", "0.0001", "3.4028235e+38", "1e-45"),
                1f, 10f, 1e9f, 1e10f, 1e11f, 1.5e-5f, 0.0001f, Float.MAX_VALUE, Float.MIN_VALUE);
    }

    @Test void boolToString() {
        ok(BOOL, STR, List.of("true", "false"), true, false);
        ok(BOOL, LSTR, List.of("true"), true);
    }

    // --- string -> numbers ---------------------------------------------------

    @Test void stringToInt() {
        ok(STR, I64, List.of(5L, -5L, 16L, 16L, 255L, Long.MAX_VALUE, Long.MIN_VALUE, 7L, 0L, Long.MAX_VALUE, -1L),
                "5", "-5", "0x10", "0X10", "0xff", "9223372036854775807", "-9223372036854775808", "007", "-0",
                "0x7fffffffffffffff", "0xffffffffffffffff");
        for (String s : List.of("+5", " 5", "5 ", "5.0", "1e3", "", "99999999999999999999", "9223372036854775808",
                "١", "-0x10", "0x", "0xg", "0x-1", "0b1", "0o7", "--1", "-")) {
            refused(STR, I64, "Failed to parse string: '" + s + "' as a scalar of type int64", s);
        }
        ok(STR, U8, List.of(255L, 0L, 255L), "255", "0", "0xff");
        for (String s : List.of("256", "-1", "0x100", "-0")) refused(STR, U8, "as a scalar of type uint8", s);
        ok(STR, I8, List.of(-128L, -1L, -128L), "-128", "0xff", "0x80");
        refused(STR, I8, "as a scalar of type int8", "128");
        ok(STR, U64, List.of(new BigInteger("18446744073709551615"), new BigInteger("18446744073709551615")),
                "18446744073709551615", "0xffffffffffffffff");
        ok(LSTR, I32, List.of(12L), "12");
        ok(BIN, I64, List.of(5L), "5");
    }

    @Test void stringToDouble() {
        ok(STR, F64, List.of(1.5, -1.5, 1.5, 1000.0, 1000.0, 0.5, 5.0, Double.NaN, Double.NaN, Double.POSITIVE_INFINITY,
                        Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY,
                        Double.POSITIVE_INFINITY, 0.0, 3.4e39, 0.1, Double.NaN, Double.POSITIVE_INFINITY,
                        Double.POSITIVE_INFINITY, Double.NaN, 1500.0, 0.0015, Double.NaN, Double.POSITIVE_INFINITY,
                        Double.NEGATIVE_INFINITY, 1000.0, 0.5),
                "1.5", "-1.5", "+1.5", "1e3", "1E3", ".5", "5.", "nan", "NaN", "inf", "-inf", "Infinity", "INF",
                "1e400", "1e-400", "3.4e39", "0.1", "-nan", "+inf", "iNf", "+nan", "1.5e+3", "1.5e-3", "NAN",
                "infinity", "-Infinity", "1.e3", "00.5");
        for (String s : List.of(" 1.5", "0x1p3", "1_000", "", "abc", "1e", "e3", ".", "-", "+", "1.5f", "1d", "inff",
                ".e3", "1e+", "1..2")) {
            refused(STR, F64, "Failed to parse string: '" + s + "' as a scalar of type double", s);
        }
        ok(STR, F32, List.of(Double.POSITIVE_INFINITY, 0.10000000149011612), "3.4e39", "0.1");
        ok(LSTR, F64, List.of(1.5), "1.5");
    }

    @Test void stringToBool() {
        ok(STR, BOOL, List.of(true, false, true, false, true, true, true, false),
                "true", "false", "1", "0", "True", "TRUE", "tRuE", "FALSE");
        for (String s : List.of("yes", "t", "1.0", "", "2")) refused(STR, BOOL, "Failed to parse value: " + s, s);
    }

    @Test void numbersAndBooleans() {
        ok(BOOL, F64, List.of(1.0), true);
        ok(F64, BOOL, List.of(true, false), 2.0, 0.0);
        ok(I64, BOOL, List.of(true, false), 2L, 0L);
        refused(I64, BIN, "no exact cast", 5L);              // pyarrow: Unsupported cast from int64 to binary
        refused(F64, I64, "was truncated converting to int64", 1e19);
    }

    // --- decimals ------------------------------------------------------------

    @Test void decimalToDecimal() {
        ok(dec(10, 2), dec(12, 2), List.of(d("1.23"), d("-99999999.99")), d("1.23"), d("-99999999.99"));
        ok(dec(12, 2), dec(5, 2), List.of(d("123.45")), d("123.45"));
        refused(dec(12, 2), dec(5, 2), "Decimal value does not fit in precision 5", d("1234.56"));
        ok(dec(10, 2), dec(12, 4), List.of(d("1.2300")), d("1.23"));
        refused(dec(10, 2), dec(10, 4), "Decimal value does not fit in precision 10", d("12345678.91"));
        ok(dec(12, 4), dec(12, 2), List.of(d("1.23")), d("1.2300"));
        refused(dec(12, 4), dec(12, 2), "Rescaling Decimal value would cause data loss", d("1.2345"));
        ok(dec(10, 2), dec256(40, 2), List.of(d("1.23")), d("1.23"));
        ok(dec256(40, 2), dec(10, 2), List.of(d("1.23")), d("1.23"));
    }

    @Test void intToDecimal() {
        ok(I32, dec(12, 2), List.of(d("7.00"), d("-7.00")), 7L, -7L);
        refused(I64, dec(5, 2), "It should be at least 21", 1000L);
        ok(I64, dec(38, 0), List.of(d("4611686018427387904")), 1L << 62);
        ok(U64, dec(20, 0), List.of(d("18446744073709551615")), -1L);
        ok(I8, dec(3, 0), List.of(d("5")), 5L);
        refused(I8, dec(2, 0), "It should be at least 3", 5L);
        ok(U8, dec(3, 0), List.of(d("5")), 5L);
        ok(I16, dec(5, 0), List.of(d("5")), 5L);
        ok(I64, dec(19, 0), List.of(d("5")), 5L);
        ok(I64, dec(20, 1), List.of(d("5.0")), 5L);
        ok(I64, dec256(40, 2), List.of(d("5.00")), 5L);
        refused(BOOL, dec(10, 2), "no exact cast", true);   // pyarrow: Unsupported cast from bool to decimal128
    }

    @Test void decimalToInt() {
        ok(dec(10, 2), I64, List.of(12L, -12L), d("12.00"), d("-12.00"));
        refused(dec(10, 2), I64, "Rescaling Decimal value would cause data loss", d("12.34"));
        refused(dec(10, 0), I8, "Integer value out of bounds", d("300"));
        refused(dec(10, 0), U8, "Integer value out of bounds", d("-1"));
        refused(dec(10, 0), BOOL, "no exact cast", d("1"));  // pyarrow: Unsupported cast to bool
    }

    @Test void decimalToReal() {
        BigDecimal[] decs = {d("0.10"), d("0.70"), d("1.10"), d("1.23"), d("3.33"), d("123456.78"), d("0.01"), d("99.99")};
        ok(dec(10, 2), F32, List.of(0.09999999403953552, 0.699999988079071, 1.100000023841858, 1.2300000190734863,
                3.3299999237060547, 123456.7734375, 0.009999999776482582, 99.98999786376953), (Object[]) decs);
        ok(dec(10, 2), F64, List.of(0.1, 0.7000000000000001, 1.1, 1.23, 3.33, 123456.78, 0.01, 99.99000000000001),
                (Object[]) decs);
        ok(dec(10, 2), F64, List.of(1.25, 0.1), d("1.25"), d("0.10"));
        ok(dec(38, 18), F64, List.of(0.12345678901234569), d("0.123456789012345678"));
        ok(dec(22, 2), F64, List.of(1.2345678901234567e+19), d("12345678901234567890.12"));
        ok(dec(38, 9), F64, List.of(9.876543210987655e+28), d("98765432109876543210987654321.123456789"));
        ok(dec(22, 2), F32, List.of(1.2345679395506094e+19), d("12345678901234567890.12"));
    }

    @Test void realToDecimal() {
        ok(F64, dec(10, 2), List.of(d("1.25"), d("0.10"), d("1.00"), d("1.23"), d("1.24"), d("1.24"), d("1.25"),
                        d("-1.24"), d("0.12"), d("0.38"), d("2.50"), d("3.50"), d("-2.50"), d("0.00"), d("0.00"),
                        d("0.01"), d("99999999.99")),
                1.25, 0.1, 1.005, 1.234, 1.236, 1.235, 1.245, -1.236, 0.125, 0.375, 2.5, 3.5, -2.5, 1e-3, 0.004999,
                0.005, 99999999.99);
        ok(F64, dec(10, 0), List.of(d("2"), d("4"), d("-2"), d("0"), d("2")), 2.5, 3.5, -2.5, 0.5, 1.5);
        ok(F32, dec(10, 2), List.of(d("1.24")), 1.236f);
        ok(F64, dec(38, 10), List.of(d("100000000000000000000.0000000000")), 1e20);
        refused(F64, dec(3, 2), "Cannot convert 12.500000 to Decimal128(precision = 3, scale = 2): overflow", 12.5);
        refused(F64, dec(10, 2), "Cannot convert 99999999.995000 to Decimal128(precision = 10, scale = 2): overflow",
                99999999.995);
        refused(F64, dec(10, 2), "Cannot convert nan to Decimal128", Double.NaN);
        refused(F64, dec(10, 2), "Cannot convert inf to Decimal128", Double.POSITIVE_INFINITY);
    }

    @Test void stringToDecimal() {
        ok(STR, dec(10, 2), List.of(d("1.20"), d("1.20"), d("0.50"), d("5.00"), d("-0.50"), d("100.00"), d("0.01"),
                        d("1.00"), d("1.00"), d("12.30"), d("1.23"), d("100.00")),
                "1.2", "+1.2", ".5", "5.", "-.5", "1E2", "1e-2", "1.", "0.1e1", "00012.30", "1.2300", "1e2");
        refused(STR, dec(10, 2), "Rescaling Decimal value would cause data loss", "-3.456");
        refused(STR, dec(10, 2), "Rescaling Decimal value would cause data loss", "1.234");
        refused(STR, dec(10, 2), "Rescaling Decimal value would cause data loss", "12e-3");
        refused(STR, dec(10, 2), "Decimal value does not fit in precision 10", "123456789012");
        for (String s : List.of(" 1.2", "abc", "1e2.5", "--1", "-", "1,2")) {
            refused(STR, dec(10, 2), "The string '" + s + "' is not a valid decimal128 number", s);
        }
    }

    @Test void decimalToString() {
        ok(dec(10, 2), STR, List.of("1.20", "-0.05", "100.00", "-1234.50", "0.00", "-0.01"),
                d("1.20"), d("-0.05"), d("100.00"), d("-1234.50"), d("0.00"), d("-0.01"));
        ok(dec(5, 0), STR, List.of("100"), d("100"));
        ok(dec(38, 0), STR, List.of("12345678901234567890123456789012345678"),
                d("12345678901234567890123456789012345678"));
        ok(dec(20, 18), STR, List.of("1E-18"), d("0.000000000000000001"));
    }

    // --- containers ----------------------------------------------------------

    @Test void listElementCasts() {
        ok(list(item(I32)), list(item(I64)), Arrays.asList(List.of(1L, 2L), null, List.of()), List.of(1, 2), null, List.of());
        refused(list(item(I64)), list(item(I32)), "not in range: -2147483648 to 2147483647", List.of(1L << 40));
        ok(list(item(STR)), list(item(I64)), List.of(List.of(1L, 2L)), List.of("1", "2"));
        ok(list(item(ArrowType.List.INSTANCE, item(I32))), list(item(ArrowType.List.INSTANCE, item(I64))),
                List.of(List.of(List.of(1L))), List.of(List.of(1)));
    }

    @Test void listKindCasts() {
        Field large = col(ArrowType.LargeList.INSTANCE, item(I64));
        ok(list(item(I64)), large, List.of(List.of(1L)), List.of(1L));
        ok(large, list(item(I64)), List.of(List.of(1L)), List.of(1L));
        Field fsl32 = col(new ArrowType.FixedSizeList(2), item(I32));
        ok(fsl32, col(new ArrowType.FixedSizeList(2), item(I64)), List.of(List.of(1L, 2L)), List.of(1, 2));
        ok(fsl32, list(item(I64)), List.of(List.of(1L, 2L)), List.of(1, 2));
        ok(list(item(I32)), fsl32, List.of(List.of(1L, 2L)), List.of(1, 2));
        refused(list(item(I32)), fsl32, "if the lists are all the expected size", List.of(1, 2, 3));
        refused(fsl32, col(new ArrowType.FixedSizeList(3), item(I32)), "Size of FixedSizeList is not the same",
                List.of(1, 2));
    }

    @Test void mapCasts() {
        ok(map(STR, I32), map(STR, I64), List.of(List.of(List.of("a", 1L))), java.util.Collections.singletonList((Object) new Object[] {"a", 1}));
        ok(map(I32, STR), map(I64, STR), List.of(List.of(List.of(1L, "a"))), java.util.Collections.singletonList((Object) new Object[] {1, "a"}));
    }

    @Test void structChildCasts() {
        Field sxy = col(ArrowType.Struct.INSTANCE, f("x", I32), f("y", STR));
        ok(sxy, col(ArrowType.Struct.INSTANCE, f("x", I64), f("y", STR)), List.of(struct("x", 1L, "y", "a")),
                struct("x", 1, "y", "a"));
        Field s2 = col(ArrowType.Struct.INSTANCE, f("x", I64), f("y", I64));
        ok(s2, col(ArrowType.Struct.INSTANCE, f("y", I64), f("x", I64)), List.of(struct("y", 2L, "x", 1L)),
                struct("x", 1L, "y", 2L));                    // reordered: matched by name
        ok(s2, col(ArrowType.Struct.INSTANCE, f("x", I64)), List.of(struct("x", 1L)), struct("x", 1L, "y", 2L));
        ok(list(item(ArrowType.Struct.INSTANCE, f("x", I32))), list(item(ArrowType.Struct.INSTANCE, f("x", I64))),
                List.of(List.of(struct("x", 1L))), List.of(struct("x", 1)));
    }

    /**
     * DEPARTS FROM THE REFERENCE, deliberately. pyarrow casts
     * {@code struct<x>} to {@code struct<y>} by filling {@code y} with nulls
     * ({@code [{'y': None}]}), and adds a declared-but-absent child the same
     * way: the data under the old name is silently gone. Refused here.
     */
    @Test void structChildTheBatchLacksIsRefusedRatherThanNulled() {
        Field sx = col(ArrowType.Struct.INSTANCE, f("x", I64));
        refused(sx, col(ArrowType.Struct.INSTANCE, f("y", I64)), "has no child 'y'", struct("x", 1L));
        refused(sx, col(ArrowType.Struct.INSTANCE, f("x", I64), f("y", I64)), "has no child 'y'", struct("x", 1L));
    }

    /** pyarrow checks nullability at the top level only; nested non-nullable declarations are not enforced. */
    @Test void nestedNullabilityIsNotEnforced() {
        Field notNullItem = list(new Field("item", FieldType.notNullable(I64), null));
        ok(list(item(I32)), notNullItem, List.of(Arrays.asList(1L, null)), Arrays.asList(1, null));
        Field notNullChild = col(ArrowType.Struct.INSTANCE, new Field("x", FieldType.notNullable(I64), null));
        ok(col(ArrowType.Struct.INSTANCE, f("x", I32)), notNullChild, Arrays.asList(struct("x", 1L), null),
                struct("x", 1), null);
    }

    // --- temporal (OutputReconcilerTemporalTest has the full pyarrow oracle) ---

    @Test void stringToTemporal() {
        ArrowType ts = new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.SECOND, null);
        ArrowType date = new ArrowType.Date(org.apache.arrow.vector.types.DateUnit.DAY);
        // pyarrow: [datetime(1970, 1, 1, 0, 0, 1)] and [date(1970, 1, 2)]
        VectorSchemaRoot r1 = VectorSchemaRoot.create(new Schema(List.of(col(STR))), Allocators.root());
        put(r1.getVector(0), 0, "1970-01-01 00:00:01");
        r1.getVector(0).setValueCount(1);
        r1.setRowCount(1);
        OutputCollector out = new OutputCollector(new Schema(List.of(col(ts))), "test", true);
        out.emit(r1);
        try (VectorSchemaRoot r = out.dataEntry().root()) {
            assertEquals(1L, ((org.apache.arrow.vector.TimeStampVector) r.getVector(0)).get(0));
        }
        VectorSchemaRoot r2 = VectorSchemaRoot.create(new Schema(List.of(col(STR))), Allocators.root());
        put(r2.getVector(0), 0, "1970-01-02");
        r2.getVector(0).setValueCount(1);
        r2.setRowCount(1);
        OutputCollector out2 = new OutputCollector(new Schema(List.of(col(date))), "test", true);
        out2.emit(r2);
        try (VectorSchemaRoot r = out2.dataEntry().root()) {
            assertEquals(1, ((org.apache.arrow.vector.DateDayVector) r.getVector(0)).get(0));
        }
    }
}
