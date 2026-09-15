// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.identity;

/**
 * Worker-supplied policy for {@code introspect_token}: what a credential resolves to.
 *
 * <p>The framework owns the guards and owns none of this. It decides who may ask, how often, and
 * what shape of credential is refused outright; the hook decides what a credential means. That
 * split is deliberate -- the guards are the part that is identical in every deployment and
 * catastrophic to get wrong, and the policy is the part that is different in every deployment
 * and cannot be guessed.
 */
@FunctionalInterface
public interface TokenResolveHook {

    /**
     * Resolve an opaque credential.
     *
     * <p>The two failure modes are different answers and a caller caches them differently.
     * Returning {@code null} says "the store answered and this credential is unknown", which a
     * caller may negative-cache. Raising {@link IdentityUnavailableError} says "the answer is not
     * knowable", which it must not.
     *
     * @param token the opaque credential. Never a JWS: three-segment credentials are refused
     *     before they reach this hook
     * @return the resolved identity, or {@code null} when the credential is unknown
     * @throws IdentityUnavailableError when the backing store could not answer
     */
    TokenIdentity resolve(String token);
}
