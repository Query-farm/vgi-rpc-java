// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import java.util.Optional;

/**
 * Resolves an opaque bearer credential to the identity it authenticates as.
 *
 * <p>Deliberately narrow, and deliberately <em>not</em> "replay the credential
 * through this worker's own {@link Authenticator}". That is the attractive
 * design and it breaks four ways: a precondition gate wrapping the chain (the
 * proxy-proof gate, say) makes the replay unimplementable; the replay runs the
 * worker's independently-configured audience/issuer set, so a credential the
 * asker itself <em>rejected</em> could be accepted here; cookie- and
 * mTLS/IP-derived identity cannot be replayed at all, and a synthesized request
 * carries the proxy's own address — silently elevating any address-allowlist
 * member rather than failing cleanly; and it invents a fake-request contract
 * every future authenticator would have to honour, with no type to enforce it.
 *
 * <p>A resolver sees only the credential, so it cannot accidentally depend on
 * any of that.
 */
@FunctionalInterface
public interface TokenResolver {

    /**
     * Resolve {@code credential}.
     *
     * <p>Implementations must not log, echo, or embed the credential — digest it
     * (see {@code TokenIntrospection}) if a diagnostic needs to correlate one
     * credential's failures across records.
     *
     * @param credential the opaque bearer credential presented by the asker
     * @return the identity, or {@link Optional#empty()} when the credential does
     *         not resolve — unknown, expired and malformed are one answer, since
     *         reporting which would confirm that a guessed credential exists
     * @throws AuthUnavailableException when the answer is not knowable: a backing
     *         store that is down is not the same as a credential that is unknown,
     *         and a caller that negative-caches the second must not cache the first
     */
    Optional<TokenIdentity> resolve(String credential);
}
