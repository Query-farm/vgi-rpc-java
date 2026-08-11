// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.wire;

import farm.query.vgirpc.RpcError;
import farm.query.vgirpc.SessionLostError;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The error batch a server writes when a handler raises. */
final class ErrorMetadataTest {

    private static final Schema RESULT = new Schema(List.of(
            new Field("result", FieldType.nullable(new ArrowType.Binary()), null)));

    /** {@code errorKind()} is documented to return null when no category
     *  applies, which is every server-thrown {@link RpcError} built without
     *  one. Putting that null in the metadata map throws in the flatbuffer
     *  key/value writer, so the error batch never reaches the wire and the
     *  caller hangs waiting for a response instead of seeing the error. */
    @Test
    void nullErrorKindIsOmittedRatherThanWritten() {
        Map<String, String> md = Wire.errorMetadata(
                new RpcError("ValueError", "boom", ""), "server-1");
        assertFalse(md.containsKey(Metadata.ERROR_KIND));
        assertEquals("server-1", md.get(Metadata.SERVER_ID));
    }

    @Test
    void declaredErrorKindIsCarried() {
        Map<String, String> md = Wire.errorMetadata(new SessionLostError("gone"), "server-1");
        assertEquals(SessionLostError.ERROR_KIND, md.get(Metadata.ERROR_KIND));
    }

    /** The end the hang was at: the batch has to serialize. */
    @Test
    void errorBatchWithoutKindSerializes() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (IpcStreamWriter w = new IpcStreamWriter(out)) {
            w.writeSchema(RESULT);
            Wire.writeZeroBatch(w, RESULT,
                    Wire.errorMetadata(new RpcError("ValueError", "boom", ""), "server-1"));
        }
        assertTrue(out.size() > 0);
    }
}
