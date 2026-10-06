// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import farm.query.vgirpc.AuthContext;
import farm.query.vgirpc.identity.GrantClaims;
import farm.query.vgirpc.identity.GrantInvalidException;
import farm.query.vgirpc.identity.GrantKeys;
import farm.query.vgirpc.identity.IdentityImpl;
import farm.query.vgirpc.identity.IdentityUnavailableError;
import farm.query.vgirpc.identity.SealedGrants;
import farm.query.vgirpc.identity.TokenIdentity;
import farm.query.vgirpc.identity.TokenResolveHook;
import farm.query.vgirpc.identity.TokenUnresolvedError;
import jakarta.servlet.http.HttpServletRequest;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Bearer authenticators that close the {@code vgi_rpc.Identity.v1} loop (WIRE_PROTOCOL.md §16,
 * "Accepting identity credentials"; IDENTITY_V1_SPEC.md §9).
 *
 * <p>{@code issue_grant} mints a credential "presented later by unattended automation as an
 * ordinary bearer", and {@code resolve_token} answers which principal an opaque credential is --
 * but until these, neither fed back into authentication.
 *
 * <ul>
 *   <li>{@link #grants} accepts the framework's own sealed grants.</li>
 *   <li>{@link #resolveToken} asks the worker's {@code resolveToken} hook.</li>
 *   <li>{@link #compose} puts them after the deployment's own authenticator in the normative
 *       order, and {@link HttpServer} applies it automatically for a server hosting
 *       {@code vgi_rpc.Identity.v1}.</li>
 * </ul>
 *
 * <p>Routing is by prefix and strict both ways. A token without {@code vgig1.} never reaches the
 * grant verifier. A token <em>with</em> it that does not verify is refused outright (401) and
 * never reaches {@code resolveToken}: a forged or stale grant must not get a second chance from a
 * resolver that might answer for it.
 */
public final class IdentityBearer {

    /** {@code AuthContext.domain} of a grant-authenticated request. */
    public static final String GRANT_AUTH_DOMAIN = "grant";
    /** {@code AuthContext.domain} of a request authenticated through {@code resolveToken}. */
    public static final String TOKEN_AUTH_DOMAIN = "token";

    private static final String BEARER = "Bearer ";

    private IdentityBearer() {}

    /** The outcome of one identity authenticator. */
    private sealed interface Outcome {
        /** Accepted. */
        record Accepted(AuthContext auth) implements Outcome {}
        /** Not this authenticator's credential: the next one may take it. */
        record Pass(AuthException why) implements Outcome {}
    }

    /** One member of the identity chain. A refusal that must stop the chain is thrown. */
    @FunctionalInterface
    private interface Member {
        Outcome authenticate(String token) throws AuthException;
    }

    /**
     * Accept the framework's own sealed grants as bearer credentials.
     *
     * <p>The resulting {@link AuthContext} has domain {@code "grant"}, the grant's principal, and
     * claims {@code {grant_id, scopes, purpose}} -- and <strong>no {@code auth_time}</strong>, so a
     * grant-authenticated caller cannot {@code issue_grant}: grants never mint grants.
     *
     * @param keys the deployment's grant configuration
     * @return an authenticator: a bearer without the {@code vgig1.} prefix is not its credential
     *     ({@link InvalidCredentials}); one with it that does not verify is an {@link AuthFailure}
     *     carrying {@code invalid_credential} or {@code expired_credential}
     */
    public static Authenticator grants(GrantKeys keys) {
        return request -> standalone(request, grantMember(keys));
    }

    /**
     * Accept bearer credentials the worker's {@code resolveToken} resolves.
     *
     * <p>{@code null} from the hook is "unknown": not accepted. An outage --
     * {@link AuthUnavailableException}, or {@link IdentityUnavailableError} from the same hook --
     * propagates as {@link AuthUnavailableException}: 503 with {@code Retry-After}, never 401.
     * The hook never sees a {@code vgig1.} token, a JWS-shaped token, a blank one, or one over
     * 4096 UTF-8 bytes -- the shape guards {@code introspect_token} applies.
     *
     * @param hook the worker's {@code resolveToken}
     * @return an authenticator producing domain {@code "token"} and {@code {token_name}} claims
     */
    public static Authenticator resolveToken(TokenResolveHook hook) {
        return request -> standalone(request, resolveMember(hook));
    }

    /**
     * Append the identity bearer authenticators after the deployment's own.
     *
     * <p>Order: {@code deployment} (JWT, static, ...), then sealed grants, then
     * {@code resolveToken}. With neither identity source {@code deployment} is returned unchanged.
     * A request carrying no {@code Authorization} header is answered by {@code deployment} alone
     * (anonymous when there is none), exactly as before; one carrying a bearer that nothing
     * accepts is 401.
     *
     * @param deployment the deployment's authenticator, or {@code null}
     * @param grantKeys sealed-grant configuration, or {@code null}
     * @param resolveToken the worker's hook, or {@code null}
     * @return the composed authenticator
     */
    public static Authenticator compose(Authenticator deployment, GrantKeys grantKeys,
                                        TokenResolveHook resolveToken) {
        java.util.List<Member> members = new java.util.ArrayList<>();
        if (grantKeys != null) members.add(grantMember(grantKeys));
        if (resolveToken != null) members.add(resolveMember(resolveToken));
        if (members.isEmpty()) return deployment;
        return request -> {
            AuthContext deployed = null;
            AuthException deploymentFailure = null;
            if (deployment != null && deployment != Authenticator.ANONYMOUS) {
                try {
                    deployed = deployment.authenticate(request);
                    if (deployed != null && deployed.authenticated()) return deployed;
                } catch (AuthException e) {
                    deploymentFailure = e;
                }
            }
            String header = request.getHeader("Authorization");
            if (header == null || header.isEmpty()) {
                // No credential at all: the identity sources have nothing to say, so the answer
                // is the deployment's, as it was before them.
                if (deploymentFailure != null) throw deploymentFailure;
                return deployed != null ? deployed : AuthContext.ANONYMOUS;
            }
            if (!header.startsWith(BEARER)) {
                if (deploymentFailure != null) throw deploymentFailure;
                throw new InvalidCredentials("Authorization header is not a Bearer credential");
            }
            String token = header.substring(BEARER.length());
            AuthException last = deploymentFailure;
            for (Member m : members) {
                Outcome o = m.authenticate(token);
                if (o instanceof Outcome.Accepted a) return a.auth();
                last = ((Outcome.Pass) o).why();
            }
            throw last != null ? last : new InvalidCredentials("bearer credential not accepted");
        };
    }

    /**
     * Mark an authenticator the deployment composed itself, so {@link HttpServer} does not append
     * the identity bearers again. Required when the authenticator depends on proxy-injected
     * evidence (a proxy-proof gate, mTLS headers): wrap the gate around
     * {@link #compose(Authenticator, GrantKeys, TokenResolveHook)} and pass the result here.
     *
     * @param composed the deployment's complete authenticator
     * @return the same authenticator, marked
     */
    public static Authenticator explicit(Authenticator composed) {
        return new Explicit(composed);
    }

    /** An authenticator the deployment composed itself; see {@link #explicit}. */
    record Explicit(Authenticator inner) implements Authenticator {
        @Override
        public AuthContext authenticate(HttpServletRequest request) throws AuthException {
            return inner.authenticate(request);
        }
    }

    private static AuthContext standalone(HttpServletRequest request, Member member) throws AuthException {
        String header = request.getHeader("Authorization");
        if (header == null || header.isEmpty()) throw new MissingCredentials("Missing Authorization header");
        if (!header.startsWith(BEARER)) {
            throw new InvalidCredentials("Authorization header is not a Bearer credential");
        }
        Outcome o = member.authenticate(header.substring(BEARER.length()));
        if (o instanceof Outcome.Accepted a) return a.auth();
        throw ((Outcome.Pass) o).why();
    }

    private static Member grantMember(GrantKeys keys) {
        return token -> {
            if (!token.startsWith(SealedGrants.TOKEN_PREFIX)) {
                return new Outcome.Pass(new InvalidCredentials("not a sealed grant"));
            }
            GrantClaims claims;
            try {
                claims = SealedGrants.verify(keys, token);
            } catch (GrantInvalidException e) {
                // Thrown, not passed: a grant-shaped token stops here and never reaches
                // resolveToken, which might answer for it.
                throw new AuthFailure(e.expired() ? AuthReason.EXPIRED_CREDENTIAL : AuthReason.INVALID_CREDENTIAL,
                        "sealed grant rejected: " + e.getMessage());
            }
            Map<String, Object> c = new LinkedHashMap<>();
            c.put("grant_id", claims.grantId());
            c.put("scopes", claims.scopes());
            c.put("purpose", claims.purpose());
            return new Outcome.Accepted(new AuthContext(GRANT_AUTH_DOMAIN, true, claims.principal(), c));
        };
    }

    private static Member resolveMember(TokenResolveHook hook) {
        return token -> {
            if (token.startsWith(SealedGrants.TOKEN_PREFIX)) {
                return new Outcome.Pass(new InvalidCredentials("sealed grants are not resolved by resolveToken"));
            }
            try {
                IdentityImpl.rejectJwsShaped(token);
            } catch (TokenUnresolvedError e) {
                return new Outcome.Pass(new InvalidCredentials("bearer credential rejected"));
            }
            TokenIdentity identity;
            try {
                identity = hook.resolve(token);
            } catch (IdentityUnavailableError e) {
                throw new AuthUnavailableException(e.getMessage(), e.retryAfterSeconds(), e);
            }
            if (identity == null) {
                return new Outcome.Pass(new InvalidCredentials("bearer credential did not resolve"));
            }
            Map<String, Object> c = new LinkedHashMap<>();
            c.put("token_name", identity.token_name());
            return new Outcome.Accepted(new AuthContext(TOKEN_AUTH_DOMAIN, true, identity.principal(), c));
        };
    }
}
