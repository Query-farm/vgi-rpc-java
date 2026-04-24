// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

/**
 * Sub-kind of a streaming RPC method, or {@link #UNKNOWN} when it cannot be
 * inferred reflectively (e.g. the generic bound of {@code Stream<?>} is erased).
 * Replaces the tri-state boxed {@code Boolean} previously threaded through
 * {@link RpcMethodInfo}.
 */
public enum StreamKind {
    /** Stream with {@link ProducerState}: server generates ticks, no client input batches after init. */
    PRODUCER,
    /** Stream with {@link ExchangeState}: client sends input batches, server sends output batches per tick. */
    EXCHANGE,
    /** Kind could not be inferred. */
    UNKNOWN
}
