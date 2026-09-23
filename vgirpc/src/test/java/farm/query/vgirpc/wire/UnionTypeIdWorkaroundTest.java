// Copyright 2026 Query Farm LLC - https://query.farm
package farm.query.vgirpc.wire;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Base64;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.Test;

/**
 * Regression for the arrow-java sparse-union type-id round-trip bug
 * (apache/arrow-java#1308) and its workaround in {@link IpcStreamWriter}.
 *
 * <p>A spec-canonical sparse union produced by Arrow C++/pyarrow (type ids
 * {@code [0, 1]}) is read by arrow-java and re-serialized through the VGI
 * {@link IpcStreamWriter}. Without the workaround, arrow-java's
 * {@code getField()} would emit type ids {@code [24, 3]} (MinorType ordinals)
 * while the buffer keeps {@code [0, 1]}, and a strict consumer rejects it. The
 * workaround rewrites the union field's type ids to the canonical
 * {@code [0, 1]}, so the round-trip is self-consistent.
 *
 * <p>When arrow-java#1308 is fixed, {@code getField()} will already report
 * {@code [0, 1]} and this test still passes — that is the signal the workaround
 * (and this test) can be removed.
 */
public class UnionTypeIdWorkaroundTest {

  // Canonical sparse union sparse_union<name: string=0, age: int16=1> from pyarrow.
  private static final String CANONICAL_UNION_IPC_B64 =
      "/////+AAAAAQAAAAAAAKAAwABgAFAAgACgAAAAABBAAEAAAAyP///wQAAAABAAAABAAAAIj///8AAAEOGAAAACQAAAAEAAAAAgAAAHAAAAAoAAAAAQAAAHUAAAAIAAgAAAAEAAgAAAAEAAAAAgAAAAAAAAABAAAAzP///wAAAQIQAAAAHAAAAAQAAAAAAAAAAwAAAGFnZQAIAAwACAAHAAgAAAAAAAABEAAAABAAFAAIAAYABwAMAAAAEAAQAAAAAAABBRAAAAAcAAAABAAAAAAAAAAEAAAAbmFtZQAAAAAEAAQABAAAAP/////oAAAAFAAAAAAAAAAMABYABgAFAAgADAAMAAAAAAMEABgAAAAoAAAAAAAAAAAACgAYAAwABAAIAAoAAAB8AAAAEAAAAAIAAAAAAAAAAAAAAAYAAAAAAAAAAAAAAAIAAAAAAAAACAAAAAAAAAAAAAAAAAAAAAgAAAAAAAAADAAAAAAAAAAYAAAAAAAAAAYAAAAAAAAAIAAAAAAAAAAAAAAAAAAAACAAAAAAAAAABAAAAAAAAAAAAAAAAwAAAAIAAAAAAAAAAAAAAAAAAAACAAAAAAAAAAAAAAAAAAAAAgAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAUAAAAGAAAAAAAAAEZyYW5reAAACQAFAAAAAAD/////AAAAAA==";

  @Test
  public void sparseUnionRoundTripPreservesTypeIds() throws Exception {
    byte[] input = Base64.getDecoder().decode(CANONICAL_UNION_IPC_B64);
    try (BufferAllocator alloc = new RootAllocator()) {
      byte[] output;
      try (ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(input), alloc)) {
        reader.loadNextBatch();
        VectorSchemaRoot in = reader.getVectorSchemaRoot();
        ByteArrayOutputStream sink = new ByteArrayOutputStream();
        // Re-serialize through the VGI writer (workaround runs here).
        try (IpcStreamWriter w = new IpcStreamWriter(java.nio.channels.Channels.newChannel(sink))) {
          w.writeBatch(in, null, null);
          w.writeEos();
        }
        output = sink.toByteArray();
      }
      try (ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(output), alloc)) {
        reader.loadNextBatch();
        ArrowType.Union u = (ArrowType.Union)
            reader.getVectorSchemaRoot().getSchema().getFields().get(0).getType();
        assertArrayEquals(new int[] {0, 1}, u.getTypeIds(),
            "re-serialized union must declare the canonical wire type ids");
        // Data survives the round-trip.
        assertEquals(2, reader.getVectorSchemaRoot().getRowCount());
      }
    }
  }
}
