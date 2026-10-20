// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

import farm.query.vgirpc.log.Level;
import farm.query.vgirpc.wire.Allocators;
import farm.query.vgirpc.wire.Wire;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class OutputCollectorTest {
    private static final Schema SCHEMA = new Schema(List.of(new Field(
            "value", FieldType.notNullable(new ArrowType.Int(64, true)), null)));

    @Test
    void rejectsASecondDataBatchButStillAllowsControlEntries() {
        OutputCollector out = new OutputCollector(SCHEMA, "test", true);
        VectorSchemaRoot first = oneRow(1);
        VectorSchemaRoot second = oneRow(2);
        try {
            out.emit(first);
            RpcError error = assertThrows(RpcError.class, () -> out.emit(second));
            assertEquals("ProtocolError", error.errorType());
            assertEquals("ProtocolError",
                    Wire.errorFromMetadata(Wire.errorMetadata(error, "test", true)).errorType());

            out.clientLog(Level.INFO, "after data");
            assertEquals(2, out.entries().size(), "one data batch plus one control batch");
        } finally {
            for (OutputCollector.Entry entry : out.entries()) entry.root().close();
            second.close();
        }
    }

    private static final Schema NULLABLE = new Schema(List.of(new Field(
            "value", FieldType.nullable(new ArrowType.Int(64, true)), null)));

    @Test
    void labelsANullabilityOnlyDifferentRootWithTheOutputSchema() {
        OutputCollector out = new OutputCollector(SCHEMA, "test", true);
        VectorSchemaRoot root = VectorSchemaRoot.create(NULLABLE, Allocators.root());
        root.allocateNew();
        ((BigIntVector) root.getVector(0)).setSafe(0, 42);
        root.setRowCount(1);
        out.emit(root);
        try (VectorSchemaRoot data = out.dataEntry().root()) {
            assertEquals(SCHEMA, data.getSchema(), "the reference casts an emitted batch to output_schema");
            assertEquals(42L, ((BigIntVector) data.getVector(0)).get(0));
        }
    }

    @Test
    void refusesNullsInAFieldTheOutputSchemaDeclaresNonNullable() {
        OutputCollector out = new OutputCollector(SCHEMA, "test", true);
        VectorSchemaRoot root = VectorSchemaRoot.create(NULLABLE, Allocators.root());
        root.allocateNew();
        ((BigIntVector) root.getVector(0)).setNull(0);
        root.setRowCount(1);
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> out.emit(root));
        assertTrue(e.getMessage().contains("'value'"), e.getMessage());
        assertEquals(0, out.entries().size());
    }

    @Test
    void leavesARootThatDiffersInTypeUnchanged() {
        Schema int32 = new Schema(List.of(new Field(
                "value", FieldType.nullable(new ArrowType.Int(32, true)), null)));
        OutputCollector out = new OutputCollector(SCHEMA, "test", true);
        VectorSchemaRoot root = VectorSchemaRoot.create(int32, Allocators.root());
        root.allocateNew();
        root.setRowCount(0);
        out.emit(root);
        try (VectorSchemaRoot data = out.dataEntry().root()) {
            assertEquals(int32, data.getSchema());
        }
    }

    private static VectorSchemaRoot oneRow(long value) {
        VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, Allocators.root());
        root.allocateNew();
        ((BigIntVector) root.getVector(0)).setSafe(0, value);
        root.setRowCount(1);
        return root;
    }
}
