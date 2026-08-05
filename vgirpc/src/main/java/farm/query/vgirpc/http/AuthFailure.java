// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

/**
 * An authentication rejection that names its own {@link AuthReason}.
 *
 * <p>{@link MissingCredentials} and {@link InvalidCredentials} cover the two
 * stages common enough to deserve a type; this covers the rest — an expired
 * token, an identified caller without the scope, a proxy-dependent gate — and
 * gives a custom {@link Authenticator} a way to state a code the framework
 * could not otherwise know.</p>
 *
 * <p>Constructed without a reason it reports {@link AuthReason#UNAUTHORIZED}.
 * That is the honest answer for a rejection that names nothing: inferring a
 * finer code would mean matching on message text, which misclassifies the
 * moment someone rewords a string.</p>
 */
public final class AuthFailure extends AuthException {

    private static final long serialVersionUID = 1L;

    private final AuthReason reason;

    /**
     * Create an unclassified failure.
     *
     * @param message diagnostic message surfaced as the envelope's {@code detail}
     */
    public AuthFailure(String message) { this(AuthReason.UNAUTHORIZED, message, null); }

    /**
     * Create a failure carrying a reason code.
     *
     * @param reason the code reported on the wire
     * @param message diagnostic message surfaced as the envelope's {@code detail};
     *        free text, but never a verifier's per-attempt state
     */
    public AuthFailure(AuthReason reason, String message) { this(reason, message, null); }

    /**
     * Create a failure carrying a reason code and a challenge.
     *
     * @param reason the code reported on the wire
     * @param message diagnostic message surfaced as the envelope's {@code detail}
     * @param wwwAuthenticate value for the {@code WWW-Authenticate} challenge header, or {@code null}
     */
    public AuthFailure(AuthReason reason, String message, String wwwAuthenticate) {
        super(message, wwwAuthenticate);
        this.reason = reason != null ? reason : AuthReason.UNAUTHORIZED;
    }

    @Override public AuthReason reason() { return reason; }
}
