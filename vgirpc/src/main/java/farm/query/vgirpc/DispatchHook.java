// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

/**
 * Observability callpoints around RPC dispatch.
 *
 * <p>Implementations must be safe for concurrent use across dispatch threads.
 */
public interface DispatchHook {
    /** Invoked just before the handler runs. Return a token passed back to {@link #onDispatchEnd}. */
    Object onDispatchStart(DispatchInfo info);

    /** Invoked once the handler has returned. {@code error} is null on success. */
    void onDispatchEnd(Object token, DispatchInfo info, CallStatistics stats, Throwable error);
}
