// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.wire;

import farm.query.vgirpc.RpcError;
import farm.query.vgirpc.VersionError;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Covers the public framing helpers an intermediary (proxy/router/gateway) needs. */
final class WireIntermediaryTest {

    private static final Schema PARAMS = new Schema(List.of(
            new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("count", FieldType.nullable(new ArrowType.Int(64, true)), null)));

    private static final Schema ENVELOPE = new Schema(List.of(
            new Field("result", FieldType.nullable(new ArrowType.Binary()), null)));

    @Test
    void requestRoundTripsMethodAndKwargs() throws Exception {
        Map<String, Object> kwargs = new LinkedHashMap<>();
        kwargs.put("name", "widget");
        kwargs.put("count", 7L);

        byte[] framed = Wire.writeRequest("bind", PARAMS, kwargs, null);
        Wire.Request parsed = Wire.readRequest(framed);

        assertEquals("bind", parsed.method());
        assertEquals("widget", parsed.kwargs().get("name").toString());
        assertEquals(7L, ((Number) parsed.kwargs().get("count")).longValue());
        assertNull(parsed.protocolVersion());
    }

    @Test
    void protocolVersionIsStampedAndRecoverable() throws Exception {
        byte[] framed = Wire.writeRequest("bind", PARAMS, Map.of("name", "x", "count", 1L), "2.1.0");

        assertEquals("2.1.0", Wire.readRequest(framed).protocolVersion());
        assertEquals("2.1.0", Wire.findProtocolVersion(framed));
    }

    @Test
    void findProtocolVersionIsNullWhenAbsentOrUnparseable() throws Exception {
        byte[] framed = Wire.writeRequest("bind", PARAMS, Map.of("name", "x", "count", 1L), null);
        assertNull(Wire.findProtocolVersion(framed));
        assertNull(Wire.findProtocolVersion(new byte[] {1, 2, 3}));
    }

    @Test
    void emptyParamsSchemaFramesAsOneRowBatch() throws Exception {
        Schema empty = new Schema(List.of());
        byte[] framed = Wire.writeRequest("ping", empty, Map.of(), null);

        Wire.Request parsed = Wire.readRequest(framed);
        assertEquals("ping", parsed.method());
        assertEquals(Map.of(), parsed.kwargs());
    }

    @Test
    void readRequestRejectsAnEmptyStream() throws Exception {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        try (IpcStreamWriter w = new IpcStreamWriter(buf)) {
            w.writeSchema(PARAMS);
        }
        assertThrows(RpcError.class, () -> Wire.readRequest(buf.toByteArray()));
    }

    @Test
    void readRequestRejectsAMissingRequestVersion() throws Exception {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        try (IpcStreamWriter w = new IpcStreamWriter(buf)) {
            w.writeSchema(PARAMS);
            Wire.writeZeroBatch(w, PARAMS, Map.of(Metadata.RPC_METHOD, "bind"));
        }
        assertThrows(VersionError.class, () -> Wire.readRequest(buf.toByteArray()));
    }

    @Test
    void unaryResultRoundTrips() throws Exception {
        byte[] payload = {9, 8, 7, 6};
        byte[] stream = Wire.writeUnaryResult(ENVELOPE, payload);

        Wire.UnaryResult unwrapped = Wire.readUnaryResult(stream);
        assertArrayEquals(payload, unwrapped.result());
        assertEquals(ENVELOPE.getFields().size(), unwrapped.envelopeSchema().getFields().size());
    }

    @Test
    void readUnaryResultSkipsLeadingLogBatches() throws Exception {
        byte[] payload = {1, 2};
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        try (IpcStreamWriter w = new IpcStreamWriter(buf)) {
            w.writeSchema(ENVELOPE);
            Wire.writeZeroBatch(w, ENVELOPE, Map.of(
                    Metadata.LOG_LEVEL, "info", Metadata.LOG_MESSAGE, "hello"));
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("result", payload);
            try (VectorSchemaRoot root =
                         farm.query.vgirpc.marshal.Marshalling.encodeRow(ENVELOPE, row, Allocators.root())) {
                w.writeBatch(root, null);
            }
        }
        assertArrayEquals(payload, Wire.readUnaryResult(buf.toByteArray()).result());
    }

    @Test
    void readUnaryResultIsLenientOnAnErrorStream() throws Exception {
        byte[] stream = Wire.buildErrorStream(new IllegalStateException("nope"), ENVELOPE, "srv-1");
        assertNull(Wire.readUnaryResult(stream));
    }

    @Test
    void buildErrorStreamCarriesTheExceptionBackToTheClient() throws Exception {
        byte[] stream = Wire.buildErrorStream(new IllegalStateException("denied"), null, "srv-1");

        try (IpcStreamReader reader = new IpcStreamReader(
                new java.io.ByteArrayInputStream(stream), Allocators.root())) {
            Map<String, String> meta = reader.readNextBatch();
            RpcError err = Wire.errorFromMetadata(meta);
            assertEquals("IllegalStateException: denied", err.getMessage());
        }
    }

    @Test
    void findStateTokenReadsTheRequestShape() throws Exception {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        try (IpcStreamWriter w = new IpcStreamWriter(buf)) {
            w.writeSchema(PARAMS);
            Wire.writeZeroBatch(w, PARAMS, Map.of(Metadata.STREAM_STATE, "tok-abc"));
        }
        assertEquals("tok-abc", Wire.findStateToken(buf.toByteArray()));
    }

    @Test
    void findStateTokenWalksConcatenatedResponseStreams() throws Exception {
        // A producer init response: a header stream (no token) followed by the
        // producer's data stream carrying the continuation token.
        Schema header = new Schema(List.of(
                new Field("h", FieldType.nullable(new ArrowType.Int(32, true)), null)));

        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        try (IpcStreamWriter w = new IpcStreamWriter(buf)) {
            w.writeSchema(header);
            Wire.writeZeroBatch(w, header, Map.of());
        }
        try (IpcStreamWriter w = new IpcStreamWriter(buf)) {
            w.writeSchema(PARAMS);
            Wire.writeZeroBatch(w, PARAMS, Map.of(Metadata.STREAM_STATE, "tok-second-stream"));
        }
        assertEquals("tok-second-stream", Wire.findStateToken(buf.toByteArray()));
    }

    @Test
    void findStateTokenIsNullWhenAbsent() throws Exception {
        byte[] framed = Wire.writeRequest("bind", PARAMS, Map.of("name", "x", "count", 1L), null);
        assertNull(Wire.findStateToken(framed));
        assertNull(Wire.findStateToken(new byte[0]));
    }
}
