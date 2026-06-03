// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.schema;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
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
    BOOL(new ArrowType.Bool()),
    DATE32(new ArrowType.Date(DateUnit.DAY)),
    TIME64_US(new ArrowType.Time(TimeUnit.MICROSECOND, 64)),
    TIMESTAMP_US(new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)),
    TIMESTAMP_US_UTC(new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC")),
    DURATION_US(new ArrowType.Duration(TimeUnit.MICROSECOND)),
    DECIMAL128_20_4(new ArrowType.Decimal(20, 4, 128)),
    BINARY_8(new ArrowType.FixedSizeBinary(8)),
    DICT_INT16_UTF8(new ArrowType.Utf8());

    private final ArrowType arrowType;
    ArrowFieldType(ArrowType t) { this.arrowType = t; }
    /** @return the underlying Arrow {@link ArrowType} for this field type. */
    public ArrowType arrowType() { return arrowType; }
}
