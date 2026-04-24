// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

/** No credentials were presented (no {@code Authorization} header, no cookie, etc.). */
public final class MissingCredentials extends AuthException {
    public MissingCredentials(String message) { this(message, null); }
    public MissingCredentials(String message, String wwwAuthenticate) { super(message, wwwAuthenticate); }
}
