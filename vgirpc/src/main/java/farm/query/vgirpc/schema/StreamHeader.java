// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.schema;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares the header record type for a streaming RPC method. Java's wildcard
 * {@code RpcStream<? extends StreamState>} return type can't encode the header
 * (unlike Python's {@code RpcStream[S, H]}), so the method is annotated instead.
 *
 * <p>The value must be a record implementing {@link ArrowSerializableRecord}.</p>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface StreamHeader {
    Class<? extends ArrowSerializableRecord> value();
}
