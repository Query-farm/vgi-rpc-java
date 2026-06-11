// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

/** Credentials were presented but are malformed, unverifiable, or rejected. */
public final class InvalidCredentials extends AuthException {
    /**
     * Create a failure with no {@code WWW-Authenticate} challenge.
     *
     * @param message diagnostic message
     */
    public InvalidCredentials(String message) { this(message, null); }
    /**
     * Create a failure carrying an optional {@code WWW-Authenticate} challenge.
     *
     * @param message diagnostic message
     * @param wwwAuthenticate value for the {@code WWW-Authenticate} challenge header, or {@code null}
     */
    public InvalidCredentials(String message, String wwwAuthenticate) { super(message, wwwAuthenticate); }
}
