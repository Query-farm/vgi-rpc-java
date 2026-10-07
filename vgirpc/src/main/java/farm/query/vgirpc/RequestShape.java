// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.ArrayList;
import java.util.List;

/**
 * The shape of a request batch -- its parameter names, their Arrow types and the row count --
 * and never its values.
 *
 * <p>This is everything an access record says about a request (spec §4.3, {@code
 * request_fields} / {@code request_rows}). The framework cannot know which parameters are
 * secret: a VGI {@code catalog_attach} carries API keys and passwords in its options. So no
 * value, encoded, truncated or hashed, reaches a log at any level, and nothing can turn that
 * back on. A digest is excluded too: a hash of a request whose other fields are known is a
 * brute-force oracle for a short secret.
 *
 * @param fields one entry per request parameter, in schema order
 * @param rows   the batch's row count: 1, or 0 for a zero-parameter method
 */
public record RequestShape(List<FieldShape> fields, long rows) {

    /**
     * One request parameter: its name and its Arrow type rendered as text.
     *
     * @param name the parameter name
     * @param type the Arrow type, as Arrow Java renders it; not compared across ports
     */
    public record FieldShape(String name, String type) {}

    /** Defensive copy, so a hook cannot see a list the dispatcher still mutates. */
    public RequestShape {
        fields = List.copyOf(fields);
    }

    /**
     * Describe a request batch.
     *
     * <p>Reads only the schema and the row count; the vectors are never touched.
     *
     * @param root the request batch, before it is drained
     * @return its shape
     */
    public static RequestShape of(VectorSchemaRoot root) {
        List<FieldShape> out = new ArrayList<>();
        for (Field f : root.getSchema().getFields()) {
            out.add(new FieldShape(f.getName(), f.getType().toString()));
        }
        return new RequestShape(out, root.getRowCount());
    }
}
