// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

/**
 * Thrown by an {@link Authenticator} when credentials are missing, malformed,
 * or invalid. The framework maps this to HTTP 401 with an optional
 * {@code WWW-Authenticate} challenge header.
 */
public class AuthException extends Exception {

    private final String wwwAuthenticate;

    public AuthException(String message) { this(message, null); }

    public AuthException(String message, String wwwAuthenticate) {
        super(message);
        this.wwwAuthenticate = wwwAuthenticate;
    }

    /** The value to place on the {@code WWW-Authenticate} response header, or {@code null}. */
    public String wwwAuthenticate() { return wwwAuthenticate; }
}
