// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.schema;

/**
 * A value carried verbatim in a {@code binary} column: the record codec writes
 * {@link #rawBinary()} as the column value and never re-serializes it.
 *
 * <p>Used for raw-preserving wrappers of nested records carried as Arrow IPC
 * bytes (vgi large-values spec §3.3), whose encoding must be exactly the bytes
 * received. As a record component its type derives as {@code binary}
 * ({@code @Nullable} for a nullable column).
 *
 * <p>To decode, the implementing class declares
 * {@code public static <Self> fromRawBinary(byte[] bytes, java.lang.reflect.Type componentType, String path)}:
 * the codec calls it with the column value (ownership passes to the result),
 * the component's generic type, and its field path ({@code Record.component}).
 */
public interface RawBinary {
    /** @return the bytes to write, unmodified; implementations return their held array */
    byte[] rawBinary();
}
