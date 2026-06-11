// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

/** No credentials were presented (no {@code Authorization} header, no cookie, etc.). */
public final class MissingCredentials extends AuthException {
    /**
     * Create a failure with no {@code WWW-Authenticate} challenge.
     *
     * @param message diagnostic message
     */
    public MissingCredentials(String message) { this(message, null); }
    /**
     * Create a failure carrying an optional {@code WWW-Authenticate} challenge.
     *
     * @param message diagnostic message
     * @param wwwAuthenticate value for the {@code WWW-Authenticate} challenge header, or {@code null}
     */
    public MissingCredentials(String message, String wwwAuthenticate) { super(message, wwwAuthenticate); }
}
