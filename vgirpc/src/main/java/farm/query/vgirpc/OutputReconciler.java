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
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
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

import java.math.BigDecimal;
import java.math.BigInteger;
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
 * <p>The reference casts with pyarrow's safe cast (Arrow C++'s cast kernels).
 * This reproduces its outcome -- the values it produces and the cases it
 * refuses -- for the conversions probed against pyarrow 25.0.1 (see
 * {@code OutputReconcilerTest}):
 * <ul>
 *   <li>nullability / field-metadata only: the vectors are relabelled, unchanged;</li>
 *   <li>integers, floating point and booleans among themselves, value-checked
 *       (out of range, truncating, beyond a float's exact-integer range);</li>
 *   <li>to and from strings: numbers and booleans formatted and parsed exactly
 *       as Arrow C++ does ({@link ArrowCasts}); among string, large string,
 *       binary and large binary (UTF-8 validated into a string type);</li>
 *   <li>decimals: rescale, precision, integer, floating-point and string
 *       conversions with Arrow's own refusals ({@link ArrowCasts});</li>
 *   <li>timestamp unit and timezone, date32 / date64, null-typed to anything;</li>
 *   <li>containers recursively: list, large list and fixed-size list among
 *       themselves (a list to a fixed-size list only when every list has that
 *       size), map keys and values, struct children matched by name with
 *       undeclared children dropped;</li>
 *   <li>dictionary-encoded to its value type (or anything that casts from it),
 *       plain string / binary to a dictionary, and dictionary to a dictionary
 *       with a different encoding.</li>
 * </ul>
 * Refused, with an {@link IllegalArgumentException} naming the field, rather
 * than sending a stream whose schema is not the declared one: everything the
 * reference refuses; a missing column; and one case the reference accepts by
 * corrupting data -- a declared struct child the batch does not have, which
 * pyarrow fills with nulls. Nulls in a top-level field the output schema
 * declares non-nullable are refused too, as pyarrow refuses them (it does not
 * check nested fields, and neither does this).
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

    /**
     * The first declared-non-nullable top-level field whose vector holds a
     * null, or {@code null}. Top level only, as the reference's cast checks:
     * pyarrow casts {@code [[1, None]]} to {@code list<item: int64 not null>}
     * and a null struct over a non-nullable child without complaint.
     */
    private static String nullInNonNullable(List<Field> declared, List<FieldVector> vectors) {
        for (int i = 0; i < declared.size(); i++) {
            if (!declared.get(i).isNullable() && vectors.get(i).getNullCount() > 0) return declared.get(i).getName();
        }
        return null;
    }

    // ------------------------------------------------------------------
    // Casts
    // ------------------------------------------------------------------

    /** Cast one top-level column into a new vector laid out for {@code tf}. Never closes {@code sv}. */
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

        FieldVector tv = tf.createVector(alloc);
        try {
            tv.allocateNew();
            int[] identity = new int[rows];
            for (int r = 0; r < rows; r++) identity[r] = r;
            fill(sv, sf, tv, tf, identity);
            return tv;
        } catch (RuntimeException e) {
            tv.close();
            throw e;
        }
    }

    /**
     * Write {@code src.length} rows into {@code tv}: row {@code i} is the cast of
     * source row {@code src[i]}, or null where {@code src[i] < 0}. Recurses
     * through list, large list, fixed-size list, map and struct, so an element
     * or child is cast exactly as a top-level column would be.
     */
    private static void fill(FieldVector sv, Field sf, FieldVector tv, Field tf, int[] src) {
        int n = src.length;
        if (sf.getDictionary() != null || tf.getDictionary() != null) {
            if (sf.getDictionary() == null || tf.getDictionary() == null
                    || !sameEncoding(sf.getDictionary(), tf.getDictionary())) {
                throw refuse(tf, sf, "dictionaries nested inside a container are not re-encoded");
            }
        }
        if (sf.getDictionary() != null || sameLayout(new Field(tf.getName(), sf.getFieldType(), sf.getChildren()), tf)) {
            for (int i = 0; i < n; i++) {
                if (src[i] < 0) setNull(tv, i);
                else tv.copyFromSafe(src[i], i, sv);
            }
            tv.setValueCount(n);
            return;
        }
        ArrowType st = sf.getType();
        ArrowType tt = tf.getType();
        if (isListLike(tt)) {
            fillList(sv, sf, tv, tf, src);
        } else if (tt instanceof ArrowType.Struct) {
            if (!(st instanceof ArrowType.Struct)) throw refuse(tf, sf, "no exact cast is available");
            fillStruct(sv, sf, tv, tf, src, false);
        } else {
            if (!sf.getChildren().isEmpty() || st instanceof ArrowType.Union) {
                throw refuse(tf, sf, "no exact cast is available");
            }
            castValues(sv, sf, tv, tf, src);
            tv.setValueCount(n);
        }
    }

    private static boolean isListLike(ArrowType t) {
        return t instanceof ArrowType.List || t instanceof ArrowType.LargeList
                || t instanceof ArrowType.FixedSizeList || t instanceof ArrowType.Map;
    }

    /** List / large list / fixed-size list / map: rebuild the offsets, then cast the elements. */
    private static void fillList(FieldVector sv, Field sf, FieldVector tv, Field tf, int[] src) {
        ArrowType st = sf.getType();
        ArrowType tt = tf.getType();
        boolean sourceIsMap = st instanceof ArrowType.Map;
        boolean targetIsMap = tt instanceof ArrowType.Map;
        if (!isListLike(st) || sourceIsMap != targetIsMap) throw refuse(tf, sf, "no exact cast is available");
        int fixedSize = tt instanceof ArrowType.FixedSizeList f ? f.getListSize() : -1;
        if (fixedSize >= 0 && st instanceof ArrowType.FixedSizeList sfl && sfl.getListSize() != fixedSize) {
            throw refuse(tf, sf, "Size of FixedSizeList is not the same");
        }
        int n = src.length;
        List<Integer> elements = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            int r = src[i];
            boolean isNull = r < 0 || sv.isNull(r);
            long start = 0;
            long end = 0;
            if (!isNull) {
                if (sv instanceof LargeListVector l) {
                    start = l.getOffsetBuffer().getLong((long) r * 8);
                    end = l.getOffsetBuffer().getLong((long) (r + 1) * 8);
                } else if (sv instanceof ListVector l) {   // includes MapVector
                    start = l.getOffsetBuffer().getInt((long) r * 4);
                    end = l.getOffsetBuffer().getInt((long) (r + 1) * 4);
                } else {
                    int size = ((FixedSizeListVector) sv).getListSize();
                    start = (long) r * size;
                    end = start + size;
                }
            }
            if (fixedSize >= 0) {
                if (!isNull && end - start != fixedSize) {
                    throw refuse(tf, sf, "ListType can only be casted to FixedSizeListType if the lists are all the "
                            + "expected size.");
                }
                FixedSizeListVector t = (FixedSizeListVector) tv;
                if (isNull) {
                    t.setNull(i);
                    for (int j = 0; j < fixedSize; j++) elements.add(-1);
                } else {
                    t.setNotNull(i);
                    for (long j = start; j < end; j++) elements.add(Math.toIntExact(j));
                }
                continue;
            }
            long len = end - start;
            if (isNull) {
                setNull(tv, i);
            } else if (tv instanceof LargeListVector t) {
                t.startNewValue(i);
                t.endValue(i, len);
            } else {
                ListVector t = (ListVector) tv;
                t.startNewValue(i);
                t.endValue(i, Math.toIntExact(len));
            }
            for (long j = start; j < end; j++) elements.add(Math.toIntExact(j));
        }
        int[] childSrc = new int[elements.size()];
        for (int j = 0; j < childSrc.length; j++) childSrc[j] = elements.get(j);
        FieldVector sChild = dataVector(sv);
        FieldVector tChild = dataVector(tv);
        Field sChildField = sf.getChildren().get(0);
        Field tChildField = tf.getChildren().get(0);
        if (targetIsMap) {
            fillStruct(sChild, sChildField, tChild, tChildField, childSrc, true);
        } else {
            fill(sChild, sChildField, tChild, tChildField, childSrc);
        }
        tv.setValueCount(n);
    }

    private static FieldVector dataVector(FieldVector v) {
        if (v instanceof LargeListVector l) return l.getDataVector();
        if (v instanceof ListVector l) return l.getDataVector();
        return ((FixedSizeListVector) v).getDataVector();
    }

    /**
     * Struct: each declared child is cast from the source child of the same
     * name (a map's entries match key and value by position). Undeclared
     * source children are dropped, as the reference drops them. A declared
     * child the source lacks -- renamed or new -- is refused: pyarrow fills it
     * with nulls, silently discarding the data under the old name.
     */
    private static void fillStruct(FieldVector sv, Field sf, FieldVector tv, Field tf, int[] src, boolean positional) {
        StructVector s = (StructVector) sv;
        StructVector t = (StructVector) tv;
        List<Field> sKids = sf.getChildren();
        List<Field> tKids = tf.getChildren();
        int n = src.length;
        int[] childSrc = new int[n];
        for (int i = 0; i < n; i++) {
            int r = src[i];
            boolean isNull = r < 0 || s.isNull(r);
            childSrc[i] = isNull ? -1 : r;
            if (isNull) t.setNull(i);
            else t.setIndexDefined(i);
        }
        for (int j = 0; j < tKids.size(); j++) {
            Field tc = tKids.get(j);
            int k = -1;
            if (positional) {
                if (sKids.size() != tKids.size()) throw refuse(tf, sf, "map entries differ in shape");
                k = j;
            } else {
                for (int m = 0; m < sKids.size(); m++) {
                    if (sKids.get(m).getName().equals(tc.getName())) { k = m; break; }
                }
                if (k < 0) {
                    throw refuse(tf, sf, "the emitted struct has no child '" + tc.getName() + "' (it has "
                            + names(sKids) + "); the reference would silently fill it with nulls");
                }
            }
            fill(s.getChildrenFromFields().get(k), sKids.get(k), t.getChildrenFromFields().get(j), tc, childSrc);
        }
        t.setValueCount(n);
    }

    private static void castValues(FieldVector sv, Field sf, FieldVector tv, Field tf, int[] src) {
        ArrowType st = sf.getType();
        ArrowType tt = tf.getType();
        int n = src.length;
        if (st instanceof ArrowType.Null) {
            for (int i = 0; i < n; i++) setNull(tv, i);
            return;
        }
        Cast cast = scalarCast(sv, sf, tv, tf);
        for (int i = 0; i < n; i++) {
            int r = src[i];
            if (r < 0 || sv.isNull(r)) { setNull(tv, i); continue; }
            try {
                cast.apply(r, i);
            } catch (ArithmeticException | IllegalArgumentException e) {
                throw refuse(tf, sf, e.getMessage());
            }
        }
    }

    /** One value: read source row {@code r}, write target row {@code i}. */
    @FunctionalInterface
    private interface Cast {
        void apply(int r, int i);
    }

    private static Cast scalarCast(FieldVector sv, Field sf, FieldVector tv, Field tf) {
        ArrowType st = sf.getType();
        ArrowType tt = tf.getType();

        // --- to and from strings / binary ----------------------------------
        if (isBytes(st) && isBytes(tt)) {
            boolean checkUtf8 = !isString(st) && isString(tt);
            return (r, i) -> {
                byte[] b = getBytes(sv, r);
                if (checkUtf8 && !validUtf8(b)) throw new IllegalArgumentException("row " + r + " is not valid UTF-8");
                setBytes(tv, i, b);
            };
        }
        if (isString(tt)) {
            if (st instanceof ArrowType.Int si) {
                return (r, i) -> setString(tv, i, ArrowCasts.formatInt(((BaseIntVector) sv).getValueAsLong(r), si));
            }
            if (st instanceof ArrowType.FloatingPoint fp) {
                return (r, i) -> setString(tv, i, fp.getPrecision() == FloatingPointPrecision.SINGLE
                        ? ArrowCasts.formatFloat((float) ((FloatingPointVector) sv).getValueAsDouble(r))
                        : ArrowCasts.formatDouble(((FloatingPointVector) sv).getValueAsDouble(r)));
            }
            if (st instanceof ArrowType.Bool) {
                return (r, i) -> setString(tv, i, ((BitVector) sv).get(r) != 0 ? "true" : "false");
            }
            if (st instanceof ArrowType.Decimal) {
                return (r, i) -> setString(tv, i, ArrowCasts.decimalToString(unscaled(sv, r), scaleOf(sf)));
            }
        }
        if (isBytes(st)) {
            if (tt instanceof ArrowType.Int ti) {
                return (r, i) -> ((BaseIntVector) tv).setWithPossibleTruncate(i, ArrowCasts.parseInt(string(sv, r), ti));
            }
            if (tt instanceof ArrowType.FloatingPoint fp) {
                return (r, i) -> {
                    String s = string(sv, r);
                    ((FloatingPointVector) tv).setWithPossibleTruncate(i,
                            fp.getPrecision() == FloatingPointPrecision.SINGLE ? ArrowCasts.parseFloat(s)
                                    : ArrowCasts.parseDouble(s, fp.getPrecision() == FloatingPointPrecision.DOUBLE
                                            ? "double" : "halffloat"));
                };
            }
            if (tt instanceof ArrowType.Bool) {
                return (r, i) -> ((BitVector) tv).setSafe(i, ArrowCasts.parseBool(string(sv, r)) ? 1 : 0);
            }
            if (tt instanceof ArrowType.Decimal td) {
                return (r, i) -> setDecimal(tv, i, ArrowCasts.parseDecimal(string(sv, r), td.getPrecision(),
                        td.getScale(), td.getBitWidth()));
            }
        }

        // --- decimals -------------------------------------------------------
        if (st instanceof ArrowType.Decimal sd && tt instanceof ArrowType.Decimal td) {
            return (r, i) -> setDecimal(tv, i, ArrowCasts.decimalToDecimal(unscaled(sv, r), sd.getScale(),
                    td.getPrecision(), td.getScale()));
        }
        if (st instanceof ArrowType.Int si && tt instanceof ArrowType.Decimal td) {
            return (r, i) -> setDecimal(tv, i, ArrowCasts.intToDecimal(((BaseIntVector) sv).getValueAsLong(r), si,
                    td.getPrecision(), td.getScale()));
        }
        if (st instanceof ArrowType.Decimal sd && tt instanceof ArrowType.Int ti) {
            return (r, i) -> ((BaseIntVector) tv).setWithPossibleTruncate(i,
                    ArrowCasts.decimalToInt(unscaled(sv, r), sd.getScale(), ti).longValue());
        }
        if (st instanceof ArrowType.Decimal sd && tt instanceof ArrowType.FloatingPoint fp) {
            return (r, i) -> ((FloatingPointVector) tv).setWithPossibleTruncate(i,
                    fp.getPrecision() == FloatingPointPrecision.DOUBLE
                            ? ArrowCasts.decimalToDouble(unscaled(sv, r), sd.getScale(), sd.getBitWidth())
                            : ArrowCasts.decimalToFloat(unscaled(sv, r), sd.getScale(), sd.getBitWidth()));
        }
        if (st instanceof ArrowType.FloatingPoint fp && tt instanceof ArrowType.Decimal td) {
            return (r, i) -> {
                double d = ((FloatingPointVector) sv).getValueAsDouble(r);
                setDecimal(tv, i, fp.getPrecision() == FloatingPointPrecision.SINGLE
                        ? ArrowCasts.floatToDecimal((float) d, td.getPrecision(), td.getScale(), td.getBitWidth())
                        : ArrowCasts.doubleToDecimal(d, td.getPrecision(), td.getScale(), td.getBitWidth()));
            };
        }

        // --- numbers and booleans -------------------------------------------
        if (st instanceof ArrowType.Int && tt instanceof ArrowType.Int ti) {
            return (r, i) -> {
                long v = ((BaseIntVector) sv).getValueAsLong(r);
                boolean huge = sv instanceof UInt8Vector && v < 0;  // an unsigned value >= 2^63
                if (!fits(v, huge, ti)) {
                    throw new IllegalArgumentException("Integer value "
                            + (huge ? Long.toUnsignedString(v) : Long.toString(v)) + " not in range: " + range(ti));
                }
                ((BaseIntVector) tv).setWithPossibleTruncate(i, v);
            };
        }
        if (st instanceof ArrowType.Bool && tt instanceof ArrowType.Int) {
            return (r, i) -> ((BaseIntVector) tv).setWithPossibleTruncate(i, ((BitVector) sv).get(r));
        }
        if (st instanceof ArrowType.Bool && tt instanceof ArrowType.FloatingPoint) {
            return (r, i) -> ((FloatingPointVector) tv).setWithPossibleTruncate(i, ((BitVector) sv).get(r));
        }
        if (st instanceof ArrowType.Int && tt instanceof ArrowType.Bool) {
            return (r, i) -> ((BitVector) tv).setSafe(i, ((BaseIntVector) sv).getValueAsLong(r) != 0 ? 1 : 0);
        }
        if (st instanceof ArrowType.FloatingPoint && tt instanceof ArrowType.Bool) {
            return (r, i) -> ((BitVector) tv).setSafe(i, ((FloatingPointVector) sv).getValueAsDouble(r) != 0 ? 1 : 0);
        }
        if (st instanceof ArrowType.Int && tt instanceof ArrowType.FloatingPoint tfp) {
            long limit = tfp.getPrecision() == FloatingPointPrecision.DOUBLE ? 1L << 53
                    : tfp.getPrecision() == FloatingPointPrecision.SINGLE ? 1L << 24 : 1L << 11;
            return (r, i) -> {
                long v = ((BaseIntVector) sv).getValueAsLong(r);
                boolean huge = sv instanceof UInt8Vector && v < 0;
                if (huge || v > limit || v < -limit) {
                    throw new IllegalArgumentException("Integer value "
                            + (huge ? Long.toUnsignedString(v) : Long.toString(v)) + " not in range: -" + limit
                            + " to " + limit);
                }
                ((FloatingPointVector) tv).setWithPossibleTruncate(i, (double) v);
            };
        }
        if (st instanceof ArrowType.FloatingPoint && tt instanceof ArrowType.FloatingPoint) {
            return (r, i) -> ((FloatingPointVector) tv).setWithPossibleTruncate(i,
                    ((FloatingPointVector) sv).getValueAsDouble(r));
        }
        if (st instanceof ArrowType.FloatingPoint && tt instanceof ArrowType.Int ti) {
            return (r, i) -> {
                double d = ((FloatingPointVector) sv).getValueAsDouble(r);
                if (d != Math.rint(d) || Double.isInfinite(d) || !fitsDouble(d, ti)) {
                    throw new IllegalArgumentException("Float value " + String.format(java.util.Locale.ROOT, "%f", d)
                            + " was truncated converting to " + ArrowCasts.intName(ti));
                }
                long v = (!ti.getIsSigned() && ti.getBitWidth() == 64 && d >= 0x1p63)
                        ? (long) (d - 0x1p63) ^ Long.MIN_VALUE : (long) d;
                ((BaseIntVector) tv).setWithPossibleTruncate(i, v);
            };
        }

        // --- temporal -------------------------------------------------------
        if (st instanceof ArrowType.Timestamp sts && tt instanceof ArrowType.Timestamp tts) {
            long from = perSecond(sts.getUnit());
            long to = perSecond(tts.getUnit());
            return (r, i) -> {
                long v = ((TimeStampVector) sv).get(r);
                long cast;
                if (to >= from) {
                    cast = Math.multiplyExact(v, to / from);
                } else {
                    long div = from / to;
                    if (v % div != 0) throw new IllegalArgumentException("Casting would lose data: " + v);
                    cast = v / div;
                }
                ((TimeStampVector) tv).setSafe(i, cast);
            };
        }
        if (st instanceof ArrowType.Date sd && tt instanceof ArrowType.Date td) {
            final long msPerDay = 86_400_000L;
            return (r, i) -> {
                long ms = sd.getUnit() == DateUnit.DAY ? ((DateDayVector) sv).get(r) * msPerDay
                        : ((DateMilliVector) sv).get(r);
                if (td.getUnit() == DateUnit.DAY) {
                    if (ms % msPerDay != 0) throw new IllegalArgumentException("Casting would lose data: " + ms);
                    ((DateDayVector) tv).setSafe(i, Math.toIntExact(ms / msPerDay));
                } else {
                    ((DateMilliVector) tv).setSafe(i, ms);
                }
            };
        }
        throw refuse(tf, sf, "no exact cast is available");
    }

    private static BigInteger unscaled(FieldVector v, int r) {
        if (v instanceof DecimalVector d) return d.getObject(r).unscaledValue();
        return ((Decimal256Vector) v).getObject(r).unscaledValue();
    }

    private static int scaleOf(Field f) { return ((ArrowType.Decimal) f.getType()).getScale(); }

    private static void setDecimal(FieldVector v, int i, BigInteger unscaled) {
        if (v instanceof DecimalVector d) d.setSafe(i, new BigDecimal(unscaled, d.getScale()));
        else ((Decimal256Vector) v).setSafe(i, new BigDecimal(unscaled, ((Decimal256Vector) v).getScale()));
    }

    private static String string(FieldVector v, int r) {
        return new String(getBytes(v, r), StandardCharsets.UTF_8);
    }

    private static void setString(FieldVector v, int i, String s) {
        setBytes(v, i, s.getBytes(StandardCharsets.UTF_8));
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
        if (v instanceof LargeListVector x) x.setNull(r);
        else if (v instanceof ListVector x) x.setNull(r);
        else if (v instanceof FixedSizeListVector x) x.setNull(r);
        else if (v instanceof StructVector x) x.setNull(r);
        else if (v instanceof BaseFixedWidthVector x) x.setNull(r);
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
