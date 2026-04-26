// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.conformance;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

/**
 * Mirrors the Python {@code EmbeddedArrow} dataclass. Python encodes
 * {@code pa.RecordBatch} and {@code pa.Schema} fields as raw IPC byte
 * blobs; this Java side just round-trips the opaque bytes.
 */
public record EmbeddedArrow(
        byte[] batch,
        byte[] schema
) implements ArrowSerializableRecord {}
