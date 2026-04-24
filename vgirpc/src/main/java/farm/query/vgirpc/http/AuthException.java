// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

/**
 * Base for authenticator failures. Sealed so {@code HttpServer.writeAuthFailure}
 * can dispatch on subtype if it ever needs to distinguish 401 reasons.
 *
 * <p>Throwers should pick a concrete subtype:
 * {@link MissingCredentials} for absent headers / cookies,
 * {@link InvalidCredentials} for malformed, unverifiable, or rejected values.</p>
 */
public abstract sealed class AuthException extends Exception
        permits MissingCredentials, InvalidCredentials {

    private final String wwwAuthenticate;

    protected AuthException(String message, String wwwAuthenticate) {
        super(message);
        this.wwwAuthenticate = wwwAuthenticate;
    }

    /** The value to place on the {@code WWW-Authenticate} response header, or {@code null}. */
    public final String wwwAuthenticate() { return wwwAuthenticate; }
}
