// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.BaseLargeVariableWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FloatingPointVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
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

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Reconcile an emitted data batch to the stream's declared output schema, as
 * the reference's {@code OutputCollector.emit} does: select the declared
 * columns by name, then cast what still differs.
 *
 * <p>The reference casts with pyarrow's safe cast. This mirrors its outcome for
 * the conversions it shares with Arrow Java, value-checked the same way:
 * <ul>
 *   <li>nullability / field-metadata only: the vectors are relabelled, unchanged;</li>
 *   <li>integer to integer (any width, signed or unsigned), refused when a value
 *       is out of the target's range;</li>
 *   <li>integer to floating point, refused beyond the float's exact-integer range;
 *       floating point to floating point; floating point to integer, refused on a
 *       fractional or out-of-range value; boolean to integer;</li>
 *   <li>among string, large string, binary and large binary (binary to a string
 *       type is refused on invalid UTF-8);</li>
 *   <li>timestamp unit and timezone (refused when a coarser unit would lose data),
 *       date32 / date64 (refused when date64 is not a whole day);</li>
 *   <li>null-typed to any type (all null);</li>
 *   <li>dictionary-encoded to its value type (or anything that casts to from it),
 *       plain string / binary to a dictionary, and dictionary to a dictionary
 *       with a different encoding.</li>
 * </ul>
 * Anything else -- a missing column, a nested container whose children differ
 * beyond nullability, decimals, numbers to or from strings -- is refused with an
 * {@link IllegalArgumentException} naming the field, rather than sending a
 * stream whose schema is not the declared one. Nulls in a field the output
 * schema declares non-nullable are refused too, as pyarrow refuses them.
 */
final class OutputReconciler {

    private OutputReconciler() {}

    /** The reconciled root (caller-owned) and the dictionaries its encoded fields reference. */
    record Result(VectorSchemaRoot root, DictionaryProvider provider) {}

    /**
     * Reconcile {@code root} to {@code target}. Takes ownership of {@code root}:
     * on success its surviving vectors move into the returned root and the rest
     * are closed; on failure everything is closed before the exception escapes.
     */
    static Result reconcile(VectorSchemaRoot root, DictionaryProvider provider, Schema target) {
        if (target == null || root.getSchema().equals(target)) return new Result(root, provider);
        BufferAllocator alloc = Allocators.root();
        List<Field> targetFields = target.getFields();
        List<Field> sourceFields = root.getSchema().getFields();
        List<FieldVector> sourceVectors = root.getFieldVectors();
        int rows = root.getRowCount();

        List<FieldVector> out = new ArrayList<>(targetFields.size());
        Set<FieldVector> kept = java.util.Collections.newSetFromMap(new IdentityHashMap<>());
        Map<Long, DictBuilder> newDicts = new LinkedHashMap<>();
        List<FieldVector> extra = new ArrayList<>();
        try {
            int[] selection = select(sourceFields, targetFields);
            for (int i = 0; i < targetFields.size(); i++) {
                Field tf = targetFields.get(i);
                Field sf = sourceFields.get(selection[i]);
                FieldVector sv = sourceVectors.get(selection[i]);
                if (sameLayout(sf, tf)) {
                    out.add(sv);
                    kept.add(sv);
                } else {
                    out.add(castColumn(sv, sf, tf, provider, rows, alloc, newDicts));
                }
            }
            String nullIn = nullInNonNullable(targetFields, out);
            if (nullIn != null) {
                throw new IllegalArgumentException("emitted batch has null values in field '" + nullIn
                        + "', which the output schema declares non-nullable");
            }
            DictionaryProvider merged = provider;
            if (!newDicts.isEmpty()) {
                Map<Long, Dictionary> built = new HashMap<>();
                for (Map.Entry<Long, DictBuilder> e : newDicts.entrySet()) {
                    FieldVector dv = e.getValue().build(alloc);
                    extra.add(dv);
                    built.put(e.getKey(), new Dictionary(dv, e.getValue().encoding));
                }
                merged = new LayeredProvider(built, provider);
            }
            for (FieldVector v : sourceVectors) if (!kept.contains(v)) v.close();
            return new Result(new OwningRoot(target, out, rows, extra), merged);
        } catch (RuntimeException e) {
            for (FieldVector v : out) if (!kept.contains(v)) v.close();
            for (FieldVector v : extra) v.close();
            root.close();
            throw e instanceof IllegalArgumentException ? e : new IllegalArgumentException(
                    "cannot reconcile the emitted batch to the output schema: " + e.getMessage(), e);
        }
    }

    // ------------------------------------------------------------------
    // Column selection and layout comparison
    // ------------------------------------------------------------------

    /** Index of the source column for each target column: by position when the names line up, else by name. */
    private static int[] select(List<Field> source, List<Field> target) {
        int[] sel = new int[target.size()];
        boolean positional = source.size() == target.size();
        for (int i = 0; positional && i < target.size(); i++) {
            positional = source.get(i).getName().equals(target.get(i).getName());
        }
        if (positional) {
            for (int i = 0; i < sel.length; i++) sel[i] = i;
            return sel;
        }
        Map<String, Integer> byName = new HashMap<>();
        for (int i = source.size() - 1; i >= 0; i--) byName.put(source.get(i).getName(), i);
        List<String> missing = new ArrayList<>();
        for (int i = 0; i < target.size(); i++) {
            Integer idx = byName.get(target.get(i).getName());
            if (idx == null) missing.add(target.get(i).getName());
            else sel[i] = idx;
        }
        if (!missing.isEmpty()) {
            throw new IllegalArgumentException("emitted batch is missing column(s) required by the output schema: "
                    + "batch has " + names(source) + ", output schema needs " + names(target)
                    + " (missing " + missing + ")");
        }
        return sel;
    }

    private static List<String> names(List<Field> fields) {
        List<String> n = new ArrayList<>(fields.size());
        for (Field f : fields) n.add(f.getName());
        return n;
    }

    /**
     * Whether {@code s}'s vector can carry {@code t} unchanged: same name, type
     * and children, ignoring nullability and field metadata. Two
     * dictionary-encoded fields compare by encoding alone, because a dictionary
     * field's own type may be stated as either the index type (Arrow Java's
     * memory form) or the value type (the declared form) and both serialise
     * identically from the dictionary the batch carries.
     */
    private static boolean sameLayout(Field s, Field t) {
        if (!s.getName().equals(t.getName())) return false;
        DictionaryEncoding se = s.getDictionary();
        DictionaryEncoding te = t.getDictionary();
        if (se != null || te != null) return se != null && te != null && sameEncoding(se, te);
        if (!s.getType().equals(t.getType()) && !sameUnion(s.getType(), t.getType())) return false;
        return sameChildren(s, t);
    }

    /**
     * Unions of the same mode are the same layout whatever type ids they state.
     * Arrow Java reports a union's type ids as {@code Types.MinorType} ordinals
     * (apache/arrow-java#1308) -- {@code [24, 3]} for a union a peer declared
     * {@code [0, 1]} -- and the writer rewrites every union to the canonical
     * child-positional ids on the way out ({@code UnionTypeIdWorkaround}), so
     * the stated ids never reach the wire.
     */
    private static boolean sameUnion(ArrowType s, ArrowType t) {
        return s instanceof ArrowType.Union su && t instanceof ArrowType.Union tu && su.getMode() == tu.getMode();
    }

    /**
     * Children compared by {@link #sameLayout}, except that the names of a list's
     * item and of a map's entries are not significant: Arrow Java calls a list
     * item {@code $data$} where Arrow C++ and pyarrow say {@code item}, and the
     * reference's cast renames them freely.
     */
    private static boolean sameChildren(Field s, Field t) {
        if (s.getChildren().size() != t.getChildren().size()) return false;
        boolean positional = s.getType() instanceof ArrowType.List || s.getType() instanceof ArrowType.LargeList
                || s.getType() instanceof ArrowType.FixedSizeList || s.getType() instanceof ArrowType.ListView
                || s.getType() instanceof ArrowType.LargeListView || s.getType() instanceof ArrowType.Map;
        for (int i = 0; i < s.getChildren().size(); i++) {
            Field sc = s.getChildren().get(i);
            Field tc = t.getChildren().get(i);
            if (positional && !sc.getName().equals(tc.getName())) {
                sc = new Field(tc.getName(), sc.getFieldType(), sc.getChildren());
            }
            if (s.getType() instanceof ArrowType.Map && !sameChildrenOfEntries(sc, tc)) return false;
            if (!(s.getType() instanceof ArrowType.Map) && !sameLayout(sc, tc)) return false;
        }
        return true;
    }

    /** A map's entries struct: key and value compared positionally, names not significant. */
    private static boolean sameChildrenOfEntries(Field s, Field t) {
        if (!s.getType().equals(t.getType()) || s.getChildren().size() != t.getChildren().size()) return false;
        for (int i = 0; i < s.getChildren().size(); i++) {
            Field sc = s.getChildren().get(i);
            Field tc = t.getChildren().get(i);
            if (!sameLayout(new Field(tc.getName(), sc.getFieldType(), sc.getChildren()), tc)) return false;
        }
        return true;
    }

    private static boolean sameEncoding(DictionaryEncoding a, DictionaryEncoding b) {
        return a.getId() == b.getId() && a.isOrdered() == b.isOrdered()
                && Objects.equals(indexType(a), indexType(b));
    }

    private static ArrowType.Int indexType(DictionaryEncoding e) {
        return e.getIndexType() != null ? e.getIndexType() : new ArrowType.Int(32, true);
    }

    /** The first declared-non-nullable field (dotted path) whose vector holds a null, or {@code null}. */
    private static String nullInNonNullable(List<Field> declared, List<FieldVector> vectors) {
        for (int i = 0; i < declared.size(); i++) {
            Field f = declared.get(i);
            FieldVector v = vectors.get(i);
            if (!f.isNullable() && v.getNullCount() > 0) return f.getName();
            if (f.getDictionary() != null) continue;  // an index vector has no children to check
            String child = nullInNonNullable(f.getChildren(), v.getChildrenFromFields());
            if (child != null) return f.getName() + "." + child;
        }
        return null;
    }

    // ------------------------------------------------------------------
    // Casts
    // ------------------------------------------------------------------

    /** Cast one column into a new vector laid out for {@code tf}. Never closes {@code sv}. */
    private static FieldVector castColumn(FieldVector sv, Field sf, Field tf, DictionaryProvider provider,
                                          int rows, BufferAllocator alloc, Map<Long, DictBuilder> newDicts) {
        DictionaryEncoding se = sf.getDictionary();
        DictionaryEncoding te = tf.getDictionary();
        if (se != null) {
            // Decode, then cast the values to whatever the target wants (re-encoding if it is a dictionary).
            Dictionary dict = provider != null ? provider.lookup(se.getId()) : null;
            if (dict == null) {
                throw refuse(tf, sf, "the batch carries no dictionary with id " + se.getId());
            }
            try (ValueVector decoded = DictionaryEncoder.decode(sv, dict, alloc)) {
                FieldVector dv = (FieldVector) decoded;
                return castColumn(dv, dv.getField(), tf, provider, rows, alloc, newDicts);
            }
        }
        if (te != null) return encode(sv, sf, tf, rows, alloc, newDicts);

        if (!sf.getChildren().isEmpty() || !tf.getChildren().isEmpty()) {
            throw refuse(tf, sf, "nested types are reconciled only when they differ in nullability");
        }
        FieldVector tv = tf.createVector(alloc);
        try {
            tv.allocateNew();
            if (sf.getType().equals(tf.getType())) {
                for (int r = 0; r < rows; r++) tv.copyFromSafe(r, r, sv);  // e.g. a decoded dictionary
            } else {
                castValues(sv, sf, tv, tf, rows);
            }
            tv.setValueCount(rows);
            return tv;
        } catch (RuntimeException e) {
            tv.close();
            throw e;
        }
    }

    private static void castValues(FieldVector sv, Field sf, FieldVector tv, Field tf, int rows) {
        ArrowType st = sf.getType();
        ArrowType tt = tf.getType();
        if (st instanceof ArrowType.Null) {
            for (int r = 0; r < rows; r++) setNull(tv, r);
            return;
        }
        if (st instanceof ArrowType.Int si && tt instanceof ArrowType.Int ti) {
            BaseIntVector src = (BaseIntVector) sv;
            BaseIntVector dst = (BaseIntVector) tv;
            for (int r = 0; r < rows; r++) {
                if (sv.isNull(r)) { setNull(tv, r); continue; }
                long v = src.getValueAsLong(r);
                boolean huge = sv instanceof UInt8Vector && v < 0;  // an unsigned value >= 2^63
                if (!fits(v, huge, ti)) {
                    throw refuse(tf, sf, "integer value " + (huge ? Long.toUnsignedString(v) : Long.toString(v))
                            + " is not in range: " + range(ti));
                }
                dst.setWithPossibleTruncate(r, v);
            }
            return;
        }
        if (st instanceof ArrowType.Bool && tt instanceof ArrowType.Int) {
            for (int r = 0; r < rows; r++) {
                if (sv.isNull(r)) { setNull(tv, r); continue; }
                ((BaseIntVector) tv).setWithPossibleTruncate(r, ((BitVector) sv).get(r));
            }
            return;
        }
        if (st instanceof ArrowType.Int && tt instanceof ArrowType.FloatingPoint tfp) {
            long limit = tfp.getPrecision() == FloatingPointPrecision.DOUBLE ? 1L << 53
                    : tfp.getPrecision() == FloatingPointPrecision.SINGLE ? 1L << 24 : 1L << 11;
            BaseIntVector src = (BaseIntVector) sv;
            for (int r = 0; r < rows; r++) {
                if (sv.isNull(r)) { setNull(tv, r); continue; }
                long v = src.getValueAsLong(r);
                boolean huge = sv instanceof UInt8Vector && v < 0;
                if (huge || v > limit || v < -limit) {
                    throw refuse(tf, sf, "integer value " + (huge ? Long.toUnsignedString(v) : Long.toString(v))
                            + " is not exactly representable: range is -" + limit + " to " + limit);
                }
                ((FloatingPointVector) tv).setWithPossibleTruncate(r, (double) v);
            }
            return;
        }
        if (st instanceof ArrowType.FloatingPoint && tt instanceof ArrowType.FloatingPoint) {
            for (int r = 0; r < rows; r++) {
                if (sv.isNull(r)) { setNull(tv, r); continue; }
                ((FloatingPointVector) tv).setWithPossibleTruncate(r, ((FloatingPointVector) sv).getValueAsDouble(r));
            }
            return;
        }
        if (st instanceof ArrowType.FloatingPoint && tt instanceof ArrowType.Int ti) {
            for (int r = 0; r < rows; r++) {
                if (sv.isNull(r)) { setNull(tv, r); continue; }
                double d = ((FloatingPointVector) sv).getValueAsDouble(r);
                if (d != Math.rint(d) || Double.isInfinite(d)) {
                    throw refuse(tf, sf, "float value " + d + " would be truncated");
                }
                if (!fitsDouble(d, ti)) {
                    throw refuse(tf, sf, "float value " + d + " is not in range: " + range(ti));
                }
                long v = (!ti.getIsSigned() && ti.getBitWidth() == 64 && d >= 0x1p63)
                        ? (long) (d - 0x1p63) ^ Long.MIN_VALUE : (long) d;
                ((BaseIntVector) tv).setWithPossibleTruncate(r, v);
            }
            return;
        }
        if (isBytes(st) && isBytes(tt)) {
            boolean checkUtf8 = !isString(st) && isString(tt);
            for (int r = 0; r < rows; r++) {
                if (sv.isNull(r)) { setNull(tv, r); continue; }
                byte[] b = getBytes(sv, r);
                if (checkUtf8 && !validUtf8(b)) throw refuse(tf, sf, "row " + r + " is not valid UTF-8");
                setBytes(tv, r, b);
            }
            return;
        }
        if (st instanceof ArrowType.Timestamp sts && tt instanceof ArrowType.Timestamp tts) {
            long from = perSecond(sts.getUnit());
            long to = perSecond(tts.getUnit());
            for (int r = 0; r < rows; r++) {
                if (sv.isNull(r)) { setNull(tv, r); continue; }
                long v = ((TimeStampVector) sv).get(r);
                long cast;
                if (to >= from) {
                    cast = Math.multiplyExact(v, to / from);
                } else {
                    long div = from / to;
                    if (v % div != 0) throw refuse(tf, sf, "casting would lose data: " + v);
                    cast = v / div;
                }
                ((TimeStampVector) tv).setSafe(r, cast);
            }
            return;
        }
        if (st instanceof ArrowType.Date sd && tt instanceof ArrowType.Date td) {
            final long msPerDay = 86_400_000L;
            for (int r = 0; r < rows; r++) {
                if (sv.isNull(r)) { setNull(tv, r); continue; }
                long ms = sd.getUnit() == DateUnit.DAY ? ((DateDayVector) sv).get(r) * msPerDay
                        : ((DateMilliVector) sv).get(r);
                if (td.getUnit() == DateUnit.DAY) {
                    if (ms % msPerDay != 0) throw refuse(tf, sf, "casting would lose data: " + ms);
                    ((DateDayVector) tv).setSafe(r, Math.toIntExact(ms / msPerDay));
                } else {
                    ((DateMilliVector) tv).setSafe(r, ms);
                }
            }
            return;
        }
        throw refuse(tf, sf, "no exact cast is available");
    }

    /** Encode a plain column into the target's dictionary, collecting the dictionary values per id. */
    private static FieldVector encode(FieldVector sv, Field sf, Field tf, int rows, BufferAllocator alloc,
                                      Map<Long, DictBuilder> newDicts) {
        DictionaryEncoding te = tf.getDictionary();
        ArrowType.Int it = indexType(te);
        ArrowType valueType = tf.getType();
        if (valueType.equals(it) || !isBytes(valueType)) {
            throw refuse(tf, sf, "only string and binary values are dictionary-encoded here");
        }
        if (!isBytes(sf.getType())) throw refuse(tf, sf, "no exact cast is available to the dictionary's values");
        DictBuilder dict = newDicts.computeIfAbsent(te.getId(), id -> new DictBuilder(te, valueType, tf.getName()));
        if (!dict.valueType.equals(valueType)) {
            throw refuse(tf, sf, "dictionary " + te.getId() + " is declared with two value types");
        }
        boolean checkUtf8 = !isString(sf.getType()) && isString(valueType);
        FieldVector idx = new Field(tf.getName(), new FieldType(true, it, te, tf.getMetadata()), null)
                .createVector(alloc);
        try {
            idx.allocateNew();
            BaseIntVector iv = (BaseIntVector) idx;
            for (int r = 0; r < rows; r++) {
                if (sv.isNull(r)) { setNull(idx, r); continue; }
                byte[] b = getBytes(sv, r);
                if (checkUtf8 && !validUtf8(b)) throw refuse(tf, sf, "row " + r + " is not valid UTF-8");
                int code = dict.indexOf(b);
                if (!fits(code, false, it)) {
                    throw refuse(tf, sf, "the dictionary outgrew its " + it + " index (" + (code + 1) + " values)");
                }
                iv.setWithPossibleTruncate(r, code);
            }
            idx.setValueCount(rows);
            return idx;
        } catch (RuntimeException e) {
            idx.close();
            throw e;
        }
    }

    private static IllegalArgumentException refuse(Field tf, Field sf, String why) {
        return new IllegalArgumentException("cannot cast field '" + tf.getName() + "' of the emitted batch from "
                + describe(sf) + " to the output schema's " + describe(tf) + ": " + why);
    }

    private static String describe(Field f) {
        return f.getDictionary() != null ? "dictionary<" + f.getType() + ", " + indexType(f.getDictionary()) + ">"
                : f.getType().toString();
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private static boolean fits(long v, boolean hugeUnsigned, ArrowType.Int t) {
        int bits = t.getBitWidth();
        if (t.getIsSigned()) {
            if (hugeUnsigned) return false;
            if (bits == 64) return true;
            long max = (1L << (bits - 1)) - 1;
            return v >= -max - 1 && v <= max;
        }
        if (hugeUnsigned) return bits == 64;
        if (v < 0) return false;
        return bits == 64 || v <= (1L << bits) - 1;
    }

    private static boolean fitsDouble(double d, ArrowType.Int t) {
        int bits = t.getBitWidth();
        if (t.getIsSigned()) return d >= -Math.pow(2, bits - 1) && d < Math.pow(2, bits - 1);
        return d >= 0 && d < Math.pow(2, bits);
    }

    private static String range(ArrowType.Int t) {
        int bits = t.getBitWidth();
        if (t.getIsSigned()) {
            return bits == 64 ? Long.MIN_VALUE + " to " + Long.MAX_VALUE
                    : -(1L << (bits - 1)) + " to " + ((1L << (bits - 1)) - 1);
        }
        return bits == 64 ? "0 to 18446744073709551615" : "0 to " + ((1L << bits) - 1);
    }

    private static long perSecond(TimeUnit u) {
        return switch (u) {
            case SECOND -> 1L;
            case MILLISECOND -> 1_000L;
            case MICROSECOND -> 1_000_000L;
            case NANOSECOND -> 1_000_000_000L;
        };
    }

    private static boolean isString(ArrowType t) {
        return t instanceof ArrowType.Utf8 || t instanceof ArrowType.LargeUtf8;
    }

    private static boolean isBytes(ArrowType t) {
        return isString(t) || t instanceof ArrowType.Binary || t instanceof ArrowType.LargeBinary;
    }

    private static byte[] getBytes(FieldVector v, int r) {
        if (v instanceof VarCharVector x) return x.get(r);
        if (v instanceof LargeVarCharVector x) return x.get(r);
        if (v instanceof VarBinaryVector x) return x.get(r);
        if (v instanceof LargeVarBinaryVector x) return x.get(r);
        throw new IllegalArgumentException("not a string or binary vector: " + v.getClass().getSimpleName());
    }

    private static void setBytes(FieldVector v, int r, byte[] b) {
        if (v instanceof VarCharVector x) x.setSafe(r, b);
        else if (v instanceof LargeVarCharVector x) x.setSafe(r, b);
        else if (v instanceof VarBinaryVector x) x.setSafe(r, b);
        else if (v instanceof LargeVarBinaryVector x) x.setSafe(r, b);
        else throw new IllegalArgumentException("not a string or binary vector: " + v.getClass().getSimpleName());
    }

    private static void setNull(FieldVector v, int r) {
        if (v instanceof BaseFixedWidthVector x) x.setNull(r);
        else if (v instanceof BaseVariableWidthVector x) x.setNull(r);
        else if (v instanceof BaseLargeVariableWidthVector x) x.setNull(r);
        else if (!(v instanceof NullVector)) {
            throw new IllegalArgumentException("cannot set a null in " + v.getClass().getSimpleName());
        }
    }

    private static boolean validUtf8(byte[] b) {
        try {
            StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(b));
            return true;
        } catch (CharacterCodingException e) {
            return false;
        }
    }

    /** Distinct values for one dictionary id, in first-seen order. */
    private static final class DictBuilder {
        final DictionaryEncoding encoding;
        final ArrowType valueType;
        final String name;
        final Map<ByteBuffer, Integer> codes = new LinkedHashMap<>();
        final List<byte[]> values = new ArrayList<>();

        DictBuilder(DictionaryEncoding encoding, ArrowType valueType, String name) {
            this.encoding = encoding;
            this.valueType = valueType;
            this.name = name;
        }

        int indexOf(byte[] b) {
            Integer code = codes.get(ByteBuffer.wrap(b));
            if (code != null) return code;
            int next = values.size();
            codes.put(ByteBuffer.wrap(b), next);
            values.add(b);
            return next;
        }

        FieldVector build(BufferAllocator alloc) {
            FieldVector v = new Field(name, FieldType.notNullable(valueType), null).createVector(alloc);
            try {
                v.allocateNew();
                for (int i = 0; i < values.size(); i++) setBytes(v, i, values.get(i));
                v.setValueCount(values.size());
                return v;
            } catch (RuntimeException e) {
                v.close();
                throw e;
            }
        }
    }

    /** Dictionaries built here first, then the handler's own. */
    private static final class LayeredProvider implements DictionaryProvider {
        private final Map<Long, Dictionary> built;
        private final DictionaryProvider base;

        LayeredProvider(Map<Long, Dictionary> built, DictionaryProvider base) {
            this.built = built;
            this.base = base;
        }

        @Override
        public Dictionary lookup(long id) {
            Dictionary d = built.get(id);
            return d != null || base == null ? d : base.lookup(id);
        }

        @Override
        public Set<Long> getDictionaryIds() {
            Set<Long> ids = new HashSet<>(built.keySet());
            if (base != null) ids.addAll(base.getDictionaryIds());
            return ids;
        }
    }

    /** A root that also closes the dictionary vectors built for it. */
    private static final class OwningRoot extends VectorSchemaRoot {
        private final List<FieldVector> extra;

        OwningRoot(Schema schema, List<FieldVector> vectors, int rows, List<FieldVector> extra) {
            super(schema, vectors, rows);
            this.extra = extra;
        }

        @Override
        public void close() {
            try {
                super.close();
            } finally {
                for (FieldVector v : extra) v.close();
            }
        }
    }
}
