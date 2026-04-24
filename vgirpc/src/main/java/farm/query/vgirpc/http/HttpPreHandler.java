// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;

/**
 * Extension point for servlet-level handlers that run before the normal
 * route-dispatch in {@link HttpServer}. A {@code PreHandler} implementation
 * can inspect the request and either (a) fully handle it (return {@code true})
 * or (b) decline so downstream dispatch proceeds (return {@code false}).
 *
 * <p>Used by optional add-ons that need to mount additional routes on the
 * same port — e.g. the OAuth PKCE flow's {@code /_oauth/callback} handler —
 * without baking HTTP routing details into core.</p>
 */
public interface HttpPreHandler {

    /**
     * @return {@code true} if the request has been fully answered (the
     *     response is complete); {@code false} to let the default dispatch
     *     handle it.
     */
    boolean handle(HttpServletRequest request, HttpServletResponse response) throws IOException;
}
