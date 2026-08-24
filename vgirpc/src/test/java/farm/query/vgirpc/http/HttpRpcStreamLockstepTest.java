// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.ProducerState;
import farm.query.vgirpc.RpcError;
import farm.query.vgirpc.wire.Allocators;
import farm.query.vgirpc.wire.IpcStreamWriter;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class HttpRpcStreamLockstepTest {
    private static final Schema SCHEMA = new Schema(List.of(new Field(
            "value", FieldType.notNullable(new ArrowType.Int(64, true)), null)));

    @Test
    void clientRejectsTwoDataBatchesInOneHttpTurn() throws Exception {
        byte[] response = twoBatchResponse();
        try (HttpRpcConnection connection = HttpRpcConnection.builder("http://127.0.0.1:1/vgi").build();
             HttpRpcStream<ProducerState> stream = new HttpRpcStream<>(connection, "bad_producer",
                     new ByteArrayInputStream(response), null)) {
            AnnotatedBatch first = stream.tick();
            assertEquals(1L, ((BigIntVector) first.root().getVector(0)).get(0));

            RpcError error = assertThrows(RpcError.class, stream::tick);
            assertEquals("ProtocolError", error.errorType());
        }
    }

    private static byte[] twoBatchResponse() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (IpcStreamWriter writer = new IpcStreamWriter(out);
             VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, Allocators.root())) {
            root.allocateNew();
            BigIntVector values = (BigIntVector) root.getVector(0);
            values.setSafe(0, 1);
            root.setRowCount(1);
            writer.writeBatch(root, null);
            values.setSafe(0, 2);
            writer.writeBatch(root, null);
        }
        return out.toByteArray();
    }
}
