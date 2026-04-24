// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http.auth;

import farm.query.vgirpc.AuthContext;
import farm.query.vgirpc.http.AuthException;
import farm.query.vgirpc.http.Authenticator;
import jakarta.servlet.http.HttpServletRequest;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * Bearer-token authenticator. Reads {@code Authorization: Bearer <token>} (or
 * {@code X-API-Key: <token>}) and delegates validation to a user-supplied
 * {@link Function} that maps the token string to an {@link AuthContext}.
 *
 * <p>Static mapping variant builds the validator from a token→context
 * {@link Map} — useful for tests and simple deployments.</p>
 */
public final class BearerAuthenticator implements Authenticator {

    /** Challenge returned on missing/invalid credentials. */
    private static final String CHALLENGE = "Bearer realm=\"vgi-rpc\"";

    private final Function<String, AuthContext> validator;

    public BearerAuthenticator(Function<String, AuthContext> validator) {
        this.validator = Objects.requireNonNull(validator, "validator");
    }

    /** Convenience: validate against a static token-to-context map. */
    public static BearerAuthenticator fromMap(Map<String, AuthContext> tokens) {
        Objects.requireNonNull(tokens, "tokens");
        return new BearerAuthenticator(token -> {
            AuthContext ctx = tokens.get(token);
            if (ctx == null) throw new IllegalArgumentException("Unknown bearer token");
            return ctx;
        });
    }

    @Override
    public AuthContext authenticate(HttpServletRequest request) throws AuthException {
        String header = request.getHeader("Authorization");
        String token = null;
        if (header != null && header.regionMatches(true, 0, "Bearer ", 0, 7)) {
            token = header.substring(7).trim();
        } else {
            String apiKey = request.getHeader("X-API-Key");
            if (apiKey != null && !apiKey.isEmpty()) token = apiKey;
        }
        if (token == null || token.isEmpty()) {
            throw new AuthException("Missing Authorization bearer header", CHALLENGE);
        }
        try {
            AuthContext ctx = validator.apply(token);
            if (ctx == null || !ctx.authenticated()) {
                throw new AuthException("Invalid bearer token", CHALLENGE);
            }
            return ctx;
        } catch (AuthException ae) {
            throw ae;
        } catch (IllegalArgumentException | SecurityException e) {
            throw new AuthException(e.getMessage() != null ? e.getMessage() : "Invalid bearer token", CHALLENGE);
        }
    }
}
