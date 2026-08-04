// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

/**
 * An authenticator could not answer. <em>Not</em> a rejection.
 *
 * <p>"The credential is bad" and "I could not find out whether the credential
 * is bad" are different answers, and collapsing them is expensive in both
 * directions. An identity sidecar restarting that surfaces as 401 makes every
 * caller re-authenticate at once — the DuckDB extension treats a second 401
 * after a refresh as fatal, so a thirty-second blip becomes a fleet-wide
 * re-login storm — and a caller that negative-caches rejections will cache the
 * outage along with them.
 *
 * <p>Deliberately <em>outside</em> the {@link AuthException} hierarchy, which is
 * the whole point. Every {@code AuthException} subtype names a
 * {@link AuthReason} and renders as a 401, and
 * {@link Authenticator#chain(Authenticator...)} catches {@code AuthException} to
 * mean "not my credential, try the next" — so an outage raised as one would be
 * swallowed and emerge as a 401 from the end of the chain. Unchecked so it
 * needs no signature change on {@link Authenticator#authenticate}, and
 * uncaught by the chain so it propagates to the request boundary, which renders
 * {@code 503} with {@code Retry-After}.
 *
 * <p>Raise it for transport failures, timeouts, and 5xx from a remote authority.
 * Never for a credential the authority actually answered about.
 */
public final class AuthUnavailableException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /** Short by design: a hint to retry, not a backoff schedule. */
    public static final int DEFAULT_RETRY_AFTER_SECONDS = 5;

    private final int retryAfterSeconds;

    /**
     * Create a transient-failure signal with the default retry hint.
     *
     * @param message operator-facing text; must never contain the credential
     */
    public AuthUnavailableException(String message) {
        this(message, DEFAULT_RETRY_AFTER_SECONDS, null);
    }

    /**
     * Create a transient-failure signal wrapping the underlying fault.
     *
     * @param message operator-facing text; must never contain the credential
     * @param cause the transport failure or timeout that prevented an answer
     */
    public AuthUnavailableException(String message, Throwable cause) {
        this(message, DEFAULT_RETRY_AFTER_SECONDS, cause);
    }

    /**
     * Create a transient-failure signal with an explicit retry hint.
     *
     * @param message operator-facing text; must never contain the credential
     * @param retryAfterSeconds seconds advertised in {@code Retry-After}; values
     *        below 1 are clamped up, since {@code Retry-After: 0} invites a hot loop
     * @param cause the underlying fault, or {@code null}
     */
    public AuthUnavailableException(String message, int retryAfterSeconds, Throwable cause) {
        super(message != null && !message.isEmpty() ? message : "authentication service unavailable", cause);
        this.retryAfterSeconds = Math.max(1, retryAfterSeconds);
    }

    /**
     * Seconds to advertise in the {@code Retry-After} response header.
     *
     * @return the retry hint, always {@code >= 1}
     */
    public int retryAfterSeconds() { return retryAfterSeconds; }
}
