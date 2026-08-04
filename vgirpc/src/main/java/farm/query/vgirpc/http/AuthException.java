// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

/**
 * Base for authenticator failures. Sealed so the 401 renderer can dispatch on
 * subtype to recover the {@link AuthReason} it reports.
 *
 * <p>Throwers should pick a concrete subtype:
 * {@link MissingCredentials} for absent headers / cookies,
 * {@link InvalidCredentials} for malformed, unverifiable, or rejected values,
 * {@link AuthFailure} for anything else — including a reason this hierarchy
 * has no dedicated type for.</p>
 *
 * <p>Every subtype means <em>rejected</em>, and every one renders as a 401. An
 * authenticator that could not reach its authority has not rejected anything
 * and must throw {@link AuthUnavailableException} instead, which is outside
 * this hierarchy precisely so the chain propagates it rather than reading it as
 * "not my credential, try the next".</p>
 */
public abstract sealed class AuthException extends Exception
        permits MissingCredentials, InvalidCredentials, AuthFailure {

    /** Optional challenge value for the {@code WWW-Authenticate} response header. */
    private final String wwwAuthenticate;

    /**
     * Create an authentication failure.
     *
     * @param message diagnostic message returned to the client
     * @param wwwAuthenticate value for the {@code WWW-Authenticate} challenge header, or {@code null} for none
     */
    protected AuthException(String message, String wwwAuthenticate) {
        super(message);
        this.wwwAuthenticate = wwwAuthenticate;
    }

    /**
     * The value to place on the {@code WWW-Authenticate} response header, or {@code null}.
     *
     * @return the challenge string supplied at construction, or {@code null} if none
     */
    public final String wwwAuthenticate() { return wwwAuthenticate; }

    /**
     * The reason code this failure reports on the wire.
     *
     * <p>The subtype <em>is</em> the classification — a thrower choosing
     * {@link MissingCredentials} has already declared that nothing was
     * presented — so this reads it off the type rather than inspecting the
     * message, which would misclassify the moment someone rewords a string.
     * The base answer is the unclassified fallback.</p>
     *
     * @return a code from the closed set of {@link AuthReason}
     */
    public AuthReason reason() { return AuthReason.UNAUTHORIZED; }
}
