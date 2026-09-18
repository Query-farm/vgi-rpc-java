// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.identity;

import farm.query.vgirpc.CallContext;

import java.util.List;

/**
 * {@code vgi_rpc.Identity.v1} -- resolving a credential, and minting a grant.
 *
 * <p>Identity lives here, at the RPC layer, rather than in any application protocol: a bearer
 * token is not an application concept, the auth primitives it builds on ({@code AuthContext},
 * the authenticator chain, {@code AuthUnavailableException}) are already here, and implementing
 * it once is the whole point. It was previously an HTTP JSON route,
 * {@code POST {prefix}/__introspect_token__}, which meant it existed only on one transport and
 * had to be hand-written in every port.
 *
 * <p>Two methods share one set of guards, and they are guarded <em>differently</em> on purpose.
 *
 * <p>{@code introspect_token} answers "which principal is this credential" for a reverse proxy
 * that terminates the only public listener. The answer is an identity assertion made by the
 * thing being protected, which the asker then acts on using credentials the worker does not hold
 * -- storage credentials, entitlement lookups, policy-tier selection. "Trust it as much as you
 * trust the worker" is the wrong frame: it must be trusted <em>more</em>. So every rejection is
 * uniform, the caller must be on an allowlist with no permissive default, and a JWS-shaped
 * subject never reaches the resolver.
 *
 * <p>It is deliberately <strong>not rate limited</strong>. The allowlist is the control: the only
 * callers are trusted askers, in practice a proxy. A per-caller limit there bounds only guessing,
 * which is hopeless against a random credential at any rate, and not the real harm of a leaked
 * introspector credential -- resolving a <em>stolen</em> credential to its owner takes one call.
 * What it did do was harm: the asker calls on behalf of everyone who presents a bearer, so a
 * per-caller budget is one budget for every user's login, drainable by unauthenticated junk
 * credentials. Throttling untrusted traffic belongs where it arrives -- at the asker, per client
 * -- and a throttled answer is never {@code introspection_refused}, which a caller may cache as
 * definitive.
 *
 * <p>{@code issue_grant} mints a credential for the <em>calling</em> user, so it is not an oracle
 * about anybody else. It therefore needs no allowlist, and its rejections are deliberately
 * <em>actionable</em>: a console that cannot tell "your login is too old" from "no" cannot know
 * to re-prompt.
 *
 * <p>Method and parameter names are snake_case, and their declaration order is fixed. The
 * framework binds arguments by parameter name and derives the protocol hash from the schema, so
 * renaming or reordering either is a wire-breaking change -- do not normalise them to Java
 * camelCase.
 *
 * <p>Registered by the server rather than by an application, under the reserved
 * {@code vgi_rpc.} prefix, the way {@code vgi_rpc.Reflection.v1} is -- and only when the
 * deployment configured a hook, because absent beats routed-and-refusing.
 */
public interface Identity {

    /**
     * The wire name of the identity protocol.
     *
     * <p>Under the reserved {@code vgi_rpc.} prefix, so an application cannot register a protocol
     * that impersonates it.
     */
    String PROTOCOL_NAME = "vgi_rpc.Identity.v1";

    /**
     * Cap on a credential the framework will even attempt to resolve, in <strong>UTF-8
     * bytes</strong>.
     *
     * <p>Anything longer is not a bearer token; refusing early keeps a resolver from being handed
     * megabytes.
     *
     * <p>Bytes, and the unit is spelled out because the ports reached for three different ones
     * for the same constant: codepoints (Python, Rust), UTF-16 code units (Java, C#, TypeScript),
     * bytes (Go, C++). All three agree for an ASCII credential -- which every real bearer token
     * is -- so nothing observable changes for real traffic. It is fixed anyway, because
     * "approximately the same limit" is exactly how this module's other divergences started and
     * each turned out to be a hole once somebody measured it. Bytes is what the purpose implies
     * (megabytes are bytes) and it is the most conservative of the three, so standardising on it
     * can only refuse earlier.
     *
     * <p>{@link String#length()} is UTF-16 code units and is the wrong measure here: a credential
     * of multibyte characters would get more than its intended allowance.
     */
    int MAX_TOKEN_BYTES = 4096;

    /**
     * Resolve an opaque bearer credential to the identity it authenticates as.
     *
     * <p>For a reverse proxy that terminates the only public listener and must know <em>which
     * principal</em> a credential is before it can authorize anything. When the credential is
     * opaque the proxy holds no local copy and has to ask the worker.
     *
     * <p>Deliberately <em>not</em> "replay the credential through the worker's own authenticate
     * chain": that would run an independently-configured audience and issuer set, cannot replay
     * cookie- or mTLS-derived identity, and would silently elevate any address-allowlist member.
     *
     * @param token the opaque credential. Never a JWS -- three-segment credentials are refused
     *     before reaching the resolver, because routing one onward would hand a third party a
     *     token the asker may itself have rejected
     * @param ctx the calling context; the caller's own authenticated identity is what the
     *     allowlist is keyed on
     * @return the resolved identity
     * @throws IntrospectionRefusedError when the caller may not introspect
     * @throws TokenUnresolvedError when the credential does not resolve
     * @throws IdentityUnavailableError when the answer is not knowable
     */
    TokenIdentity introspect_token(String token, CallContext ctx);

    /**
     * Mint a standing delegation credential for the <em>calling</em> user.
     *
     * <p>OAuth cannot express durable delegation: it fuses the grant, the credential and the
     * session into one refresh token, so an IdP shortening session lifetime shortens the grant.
     * This is the durable record -- minted while the user is present, presented later by
     * unattended automation as an ordinary bearer.
     *
     * <p><strong>There is no subject parameter.</strong> The subject is always the caller's
     * authenticated principal, so cross-subject minting is closed by construction rather than by
     * a check. That is also why this method needs no allowlist while {@code introspect_token} has
     * one: introspection resolves <em>other people's</em> credentials, so "any authenticated
     * caller" is an open oracle there; issuance is always about the caller themselves.
     *
     * @param purpose why the grant is being minted, for the audit trail
     * @param scopes what the grant may do. The worker decides what these mean; the framework
     *     neither interprets nor validates them
     * @param ttl_seconds requested lifetime. A request, not an instruction -- the worker may
     *     return a shorter one, and the returned {@code expires_at} is authoritative
     * @param ctx the calling context, which supplies the subject and the freshness evidence
     * @return the minted grant
     * @throws StaleAuthError when the caller has not recently authenticated with an identity
     *     provider
     * @throws GrantRefusedError when the worker declines
     */
    IssuedGrant issue_grant(String purpose, List<String> scopes, long ttl_seconds, CallContext ctx);
}
