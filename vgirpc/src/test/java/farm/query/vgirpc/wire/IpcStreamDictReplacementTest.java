// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.wire;

import farm.query.vgirpc.marshal.Marshalling;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Every batch on a multi-batch stream must decode against its own
 * dictionaries.
 *
 * <p>This writer used to emit dictionaries once, with the first batch, and
 * ignore the provider handed to every later {@link IpcStreamWriter#writeBatch}.
 * A stream whose dictionaries changed between batches -- each batch built with
 * its own value table, which is how {@code Marshalling.encodeRowForWire} and any
 * per-batch encoder work -- then decoded every later batch against the first
 * batch's table: {@code a, a, a} where {@code a, b, c} was written, with no
 * error anywhere. HTTP hid it (one data batch per response stream); the pipe,
 * socket and subprocess transports run a whole stream through one writer.
 */
final class IpcStreamDictReplacementTest {

    static final Schema SCHEMA = new Schema(List.of(new Field("s", new FieldType(true, new ArrowType.Utf8(),
            new DictionaryEncoding(0L, false, new ArrowType.Int(16, true))), null)));

    @Test
    void laterBatchesCarryTheirOwnDictionaries() throws Exception {
        ByteArrayOutputStream sink = new ByteArrayOutputStream();
        try (IpcStreamWriter w = new IpcStreamWriter(sink)) {
            for (String v : List.of("a", "b", "b", "c")) {
                try (Marshalling.EncodedRow row = Marshalling.encodeRowForWire(SCHEMA, Map.of("s", v), Allocators.root())) {
                    w.writeBatch(row.root(), null, row.provider());
                }
            }
        }
        List<Object> got = new ArrayList<>();
        try (IpcStreamReader r = new IpcStreamReader(new ByteArrayInputStream(sink.toByteArray()), Allocators.root())) {
            while (r.readNextBatch() != null) {
                got.add(Marshalling.decodeRow(r.root(), r.dictionaryProvider()).get("s"));
            }
        }
        assertEquals(List.of("a", "b", "b", "c"), got);
    }
}
