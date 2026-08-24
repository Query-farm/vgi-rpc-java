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
                    Wire.errorFromMetadata(Wire.errorMetadata(error, "test")).errorType());

            out.clientLog(Level.INFO, "after data");
            assertEquals(2, out.entries().size(), "one data batch plus one control batch");
        } finally {
            for (OutputCollector.Entry entry : out.entries()) entry.root().close();
            second.close();
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
