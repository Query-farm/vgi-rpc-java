// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

/**
 * Observability callpoints around RPC dispatch.
 *
 * <p>Implementations must be safe for concurrent use across dispatch threads.
 */
public interface DispatchHook {
    /**
     * Invoked just before the handler runs. Return a token passed back to
     * {@link #onDispatchEnd}; a timing hook typically returns its start
     * timestamp here. Exceptions thrown by the hook are logged and do not
     * fail the call.
     *
     * @param info dispatch metadata known at call start (method, protocol,
     *             auth principal, …); the dispatcher may still mutate it
     *             before {@link #onDispatchEnd} fires
     * @return an opaque correlation token handed back to
     *         {@link #onDispatchEnd}, or {@code null} if none is needed
     */
    Object onDispatchStart(DispatchInfo info);

    /**
     * Invoked exactly once per dispatch after the handler finishes, in a
     * {@code finally} block — it fires whether the handler returned normally
     * or threw. Exceptions thrown by the hook are logged and do not fail the
     * call.
     *
     * @param token the value returned by {@link #onDispatchStart}, or
     *              {@code null} if that callback threw
     * @param info the same {@link DispatchInfo} passed to
     *             {@link #onDispatchStart}, now with end-of-call fields
     *             (e.g. session id/action) filled in
     * @param stats per-call I/O counters accumulated during dispatch
     * @param error the throwable that escaped the handler, or {@code null}
     *              when the dispatch completed normally
     */
    void onDispatchEnd(Object token, DispatchInfo info, CallStatistics stats, Throwable error);
}
