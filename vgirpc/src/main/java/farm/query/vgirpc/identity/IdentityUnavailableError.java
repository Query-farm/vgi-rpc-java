// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.identity;

import farm.query.vgirpc.HasErrorKind;

/**
 * The answer is not <em>knowable</em> -- a backing store is down, a 5xx upstream.
 *
 * <p>Transient, and distinct from a definitive rejection: a caller that negative-caches
 * "unknown" must not cache this. Cache an outage and a worker restart takes the fleet down for
 * the cache's lifetime; retry a rejection and the worker is hammered. As protocol methods every
 * handler exception surfaces the same way to a client, so {@link #errorKind()} carries the whole
 * definitive-versus-transient distinction that an HTTP route used to carry in its status code.
 *
 * <p>Deliberately <strong>not</strong> an {@link IllegalArgumentException}, and deliberately
 * outside {@code farm.query.vgirpc.http.AuthException}: this port's authenticator chain advances
 * to the next member on a rejection, so a sidecar outage raised as a rejection reads as "not my
 * credential, try the next" and arrives at the client as a 401 from the end of the chain --
 * turning a thirty-second blip into a fleet-wide re-login. It mirrors
 * {@code AuthUnavailableException}, which stays outside that hierarchy for the same reason.
 */
public final class IdentityUnavailableError extends RuntimeException implements HasErrorKind {

    /** The stable wire category for a transient identity-lookup failure. */
    public static final String ERROR_KIND = "identity_unavailable";

    /**
     * Seconds a caller should wait before retrying, when the raiser names none.
     *
     * <p>Identical in every port, and applied rather than merely offered: the no-arg spelling
     * uses it, and a non-positive value handed to the explicit one is corrected to it. A caller
     * required to supply the delay at every construction site is a caller that eventually passes
     * {@code 0}, and {@code 0} tells a client to retry a transient failure immediately -- which
     * turns one store outage into a retry storm against the store that is already down.
     */
    public static final int DEFAULT_RETRY_AFTER_SECONDS = 5;

    private final int retryAfterSeconds;

    /**
     * Report that the lookup could not be performed.
     *
     * @param message what was unreachable; never the subject credential
     */
    public IdentityUnavailableError(String message) {
        this(message, DEFAULT_RETRY_AFTER_SECONDS, null);
    }

    /**
     * Report that the lookup could not be performed, naming a retry delay.
     *
     * @param message what was unreachable; never the subject credential
     * @param retryAfterSeconds how long the caller should wait before retrying; non-positive is
     *     corrected to {@link #DEFAULT_RETRY_AFTER_SECONDS}, because "retry immediately" is not
     *     an answer a transient failure can give
     * @param cause the underlying failure, or {@code null}
     */
    public IdentityUnavailableError(String message, int retryAfterSeconds, Throwable cause) {
        super(message == null || message.isEmpty() ? "identity lookup unavailable" : message, cause);
        this.retryAfterSeconds =
                retryAfterSeconds > 0 ? retryAfterSeconds : DEFAULT_RETRY_AFTER_SECONDS;
    }

    /**
     * How long the caller should wait before retrying.
     *
     * @return the retry delay in seconds; always positive, so a client can wait on it without
     *     checking it first
     */
    public int retryAfterSeconds() {
        return retryAfterSeconds;
    }

    @Override
    public String errorKind() {
        return ERROR_KIND;
    }
}
