// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http.auth;

import farm.query.vgirpc.AuthContext;
import farm.query.vgirpc.http.AuthException;
import farm.query.vgirpc.http.Authenticator;
import farm.query.vgirpc.http.HttpHeaders;
import farm.query.vgirpc.http.InvalidCredentials;
import farm.query.vgirpc.http.MissingCredentials;
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

    /**
     * @param validator maps a presented token to an {@link AuthContext}; should
     *        throw {@link IllegalArgumentException}/{@link SecurityException} (or
     *        return an unauthenticated context) to reject the token
     */
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

    /**
     * Read a token from {@code Authorization: Bearer} (or {@code X-API-Key}) and
     * validate it via the configured function.
     *
     * @param request the inbound request
     * @return the authenticated context
     * @throws MissingCredentials if no token header is present
     * @throws InvalidCredentials if the token is rejected or maps to an unauthenticated context
     */
    @Override
    public AuthContext authenticate(HttpServletRequest request) throws AuthException {
        String header = request.getHeader(HttpHeaders.AUTHORIZATION);
        String token = null;
        if (header != null && header.regionMatches(true, 0, HttpHeaders.BEARER_PREFIX, 0, HttpHeaders.BEARER_PREFIX.length())) {
            token = header.substring(HttpHeaders.BEARER_PREFIX.length()).trim();
        } else {
            String apiKey = request.getHeader(HttpHeaders.API_KEY);
            if (apiKey != null && !apiKey.isEmpty()) token = apiKey;
        }
        if (token == null || token.isEmpty()) {
            throw new MissingCredentials("Missing Authorization bearer header", CHALLENGE);
        }
        try {
            AuthContext ctx = validator.apply(token);
            if (ctx == null || !ctx.authenticated()) {
                throw new InvalidCredentials("Invalid bearer token", CHALLENGE);
            }
            return ctx;
        } catch (IllegalArgumentException | SecurityException e) {
            throw new InvalidCredentials(e.getMessage() != null ? e.getMessage() : "Invalid bearer token", CHALLENGE);
        }
    }
}
