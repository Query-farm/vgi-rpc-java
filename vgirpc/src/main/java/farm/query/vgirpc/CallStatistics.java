// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

/** Per-call I/O counters. Mutable, accumulated by the server during dispatch. */
public final class CallStatistics {
    public long inputBatches;
    public long outputBatches;
    public long inputRows;
    public long outputRows;
    public long inputBytes;
    public long outputBytes;

    /** True when at least one count is non-zero. */
    public boolean nonZero() {
        return (inputBatches | outputBatches | inputRows | outputRows | inputBytes | outputBytes) != 0;
    }
}
