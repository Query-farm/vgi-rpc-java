// Copyright 2026 Query Farm LLC - https://query.farm
package farm.query.vgirpc.wire;

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

/**
 * Workaround for apache/arrow-java#1308.
 *
 * <p>arrow-java's {@code UnionVector.getField()} derives a union's declared type
 * ids from {@code Types.MinorType} ordinals rather than the union's actual wire
 * type ids. When a spec-canonical union (type ids {@code [0, 1]}, produced by
 * Arrow C++/pyarrow/DuckDB) is read and re-serialized, the emitted schema
 * declares type ids like {@code [24, 3]} (the MinorType ordinals for Utf8 and
 * SmallInt) while the type-id buffer still holds {@code [0, 1]}. The result is a
 * self-inconsistent union that strict Arrow consumers reject with
 * "Union value at position 0 has invalid type id 0".
 *
 * <p>The emitted schema is taken from the {@link VectorSchemaRoot}'s per-vector
 * {@code getField()} (see {@link IpcStreamWriter}), so we correct it there: any
 * Union field whose declared type ids are not the canonical child-positional
 * {@code [0 .. childCount-1]} — the codes a spec-compliant producer writes into
 * the buffer, and the codes arrow-java loads verbatim — is rewritten to match.
 *
 * <p><b>Self-disabling:</b> a field already carrying canonical type ids is left
 * untouched and, when no field needs fixing, the original root is returned
 * unchanged (no allocation). Once arrow-java#1308 is fixed and {@code getField()}
 * reports the real type ids, this becomes a no-op and the single call site in
 * {@link IpcStreamWriter#writeBatch(VectorSchemaRoot, java.util.Map,
 * org.apache.arrow.vector.dictionary.DictionaryProvider)} plus this class can be
 * deleted.
 */
final class UnionTypeIdWorkaround {
    private UnionTypeIdWorkaround() {}

    /**
     * Return {@code root} with every Union field (at any nesting depth) carrying
     * canonical child-positional type ids. Returns {@code root} unchanged when
     * nothing needs correcting. The returned root shares {@code root}'s vectors
     * (and therefore its buffers), so unloading it writes identical data.
     */
    static VectorSchemaRoot canonicalize(VectorSchemaRoot root) {
        List<Field> fields = root.getSchema().getFields();
        List<Field> fixed = null;
        for (int i = 0; i < fields.size(); i++) {
            Field original = fields.get(i);
            Field corrected = fixField(original);
            if (corrected != original) {
                if (fixed == null) {
                    fixed = new ArrayList<>(fields);
                }
                fixed.set(i, corrected);
            }
        }
        if (fixed == null) {
            return root; // nothing to fix — arrow-java already correct, or no unions
        }
        // Re-wrap the SAME vectors under corrected fields; the writer takes its
        // schema from the root, while the data buffers are unchanged.
        return new VectorSchemaRoot(fixed, root.getFieldVectors(), root.getRowCount());
    }

    /** Recursively correct a field's type (and children); returns the same instance when unchanged. */
    private static Field fixField(Field field) {
        ArrowType type = field.getType();
        List<Field> children = field.getChildren();

        List<Field> fixedChildren = null;
        for (int i = 0; i < children.size(); i++) {
            Field c = children.get(i);
            Field fc = fixField(c);
            if (fc != c) {
                if (fixedChildren == null) {
                    fixedChildren = new ArrayList<>(children);
                }
                fixedChildren.set(i, fc);
            }
        }

        ArrowType fixedType = type;
        if (type instanceof ArrowType.Union) {
            ArrowType.Union u = (ArrowType.Union) type;
            int n = children.size();
            int[] canonical = new int[n];
            for (int i = 0; i < n; i++) {
                canonical[i] = i;
            }
            int[] declared = u.getTypeIds();
            if (declared == null || !java.util.Arrays.equals(declared, canonical)) {
                fixedType = new ArrowType.Union(u.getMode(), canonical);
            }
        }

        if (fixedType == type && fixedChildren == null) {
            return field; // unchanged
        }
        FieldType ft = new FieldType(field.isNullable(), fixedType, field.getDictionary(), field.getMetadata());
        return new Field(field.getName(), ft, fixedChildren != null ? fixedChildren : children);
    }
}
