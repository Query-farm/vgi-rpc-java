// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

/** Per-call I/O counters. Mutable, accumulated by the server during dispatch. */
public final class CallStatistics {

    /** Number of data batches received from the client. */
    public long inputBatches;
    /** Number of data batches sent to the client. */
    public long outputBatches;
    /** Total rows across all input batches. */
    public long inputRows;
    /** Total rows across all output batches. */
    public long outputRows;
    /** Total serialized bytes received from the client. */
    public long inputBytes;
    /** Total serialized bytes sent to the client. */
    public long outputBytes;

    /** True when at least one count is non-zero.
     *  @return {@code true} if any counter was incremented during the call,
     *          {@code false} when all six counters are still zero */
    public boolean nonZero() {
        return (inputBatches | outputBatches | inputRows | outputRows | inputBytes | outputBytes) != 0;
    }
}
