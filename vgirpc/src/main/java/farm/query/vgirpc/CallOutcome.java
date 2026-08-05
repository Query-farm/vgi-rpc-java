// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

/**
 * Per-call record of whether the response being built carries an RPC error.
 *
 * <p>A failed vgi-rpc call is not a failed HTTP request: the method was
 * reached, it raised, and the exception travels back as an Arrow EXCEPTION
 * batch inside a well-formed <strong>200</strong> response. Nothing about the
 * status line says so, which is why {@code X-VGI-RPC-Error: true} exists and
 * why something has to remember, between the point the error is serialized and
 * the point the response headers are written, that this response is a failure.
 *
 * <p>The dispatchers cannot carry that themselves: every error path writes the
 * exception into the response body and then <em>returns normally</em>, so a
 * caller looking only at control flow sees a success. The signal is instead
 * taken at the one place every error passes through —
 * {@code Wire.errorMetadata}, which builds the EXCEPTION batch's metadata — so
 * a new error path cannot forget to raise it.
 *
 * <p>Two readers depend on it: the HTTP transport, which sets the error flag on
 * the response, and {@link AccessLogHook}, whose {@code status} field would
 * otherwise report {@code "ok"} for a call whose whole payload is an exception.
 *
 * <p>Scopes nest safely. The HTTP transport opens one per request; the
 * dispatcher opens one per call for transports (pipe, unix, TCP) that have no
 * request boundary of their own. Under HTTP the inner open is inert and the
 * outer scope keeps the value alive until the response has been written.
 *
 * <p>Bound to the dispatching thread. Not safe for use across threads.
 */
public final class CallOutcome implements AutoCloseable {

    private static final ThreadLocal<CallOutcome> CURRENT = new ThreadLocal<>();

    /** Whether closing this handle uninstalls the scope, or it is a nested no-op. */
    private final boolean owner;
    private Throwable error;

    private CallOutcome(boolean owner) {
        this.owner = owner;
    }

    /**
     * Install a scope on the current thread, or join one already installed.
     *
     * @return a handle to close (ideally via try-with-resources); closing a
     *         nested handle leaves the enclosing scope untouched
     */
    public static CallOutcome open() {
        if (CURRENT.get() != null) return new CallOutcome(false);
        CallOutcome scope = new CallOutcome(true);
        CURRENT.set(scope);
        return scope;
    }

    /**
     * Note that an error is being written into the response. No-op when no
     * scope is installed.
     *
     * <p>The first error wins: a decode failure that then trips a second
     * failure on the way out should still be reported as what actually went
     * wrong.
     *
     * @param t the exception being serialized into the response
     */
    public static void recordError(Throwable t) {
        CallOutcome scope = CURRENT.get();
        if (scope != null && scope.error == null && t != null) scope.error = t;
    }

    /**
     * The error this call put on the wire.
     *
     * @return the exception, or {@code null} when the call is so far a success
     *         (or no scope is installed)
     */
    public static Throwable currentError() {
        CallOutcome scope = CURRENT.get();
        return scope == null ? null : scope.error;
    }

    /** @return whether this call has written an error into its response */
    public static boolean failed() {
        return currentError() != null;
    }

    /** Uninstall the scope, if this handle owns it. */
    @Override
    public void close() {
        if (owner) CURRENT.remove();
    }
}
