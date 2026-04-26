// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.schema;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;

/**
 * Limited enum of Arrow type overrides exposable via {@link ArrowField}.
 * Anything more exotic should be handled with a custom {@link ArrowSerializableRecord}.
 */
public enum ArrowFieldType {
    INT8(new ArrowType.Int(8, true)),
    INT16(new ArrowType.Int(16, true)),
    INT32(new ArrowType.Int(32, true)),
    INT64(new ArrowType.Int(64, true)),
    UINT8(new ArrowType.Int(8, false)),
    UINT16(new ArrowType.Int(16, false)),
    UINT32(new ArrowType.Int(32, false)),
    UINT64(new ArrowType.Int(64, false)),
    FLOAT16(new ArrowType.FloatingPoint(FloatingPointPrecision.HALF)),
    FLOAT32(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
    FLOAT64(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
    UTF8(new ArrowType.Utf8()),
    LARGE_UTF8(new ArrowType.LargeUtf8()),
    BINARY(new ArrowType.Binary()),
    LARGE_BINARY(new ArrowType.LargeBinary()),
    BOOL(new ArrowType.Bool());

    private final ArrowType arrowType;
    ArrowFieldType(ArrowType t) { this.arrowType = t; }
    public ArrowType arrowType() { return arrowType; }
}
