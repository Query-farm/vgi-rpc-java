// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0
package farm.query.vgirpc.identity;

import farm.query.vgirpc.AuthContext;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Built-in provider-neutral authentication composition policies. */
public final class PeerAuthenticationPolicies {
    private PeerAuthenticationPolicies() {}

    public static AuthContext observe(PeerEvidenceSet evidence, AuthContext existingAuth) { return existingAuth; }

    public static PeerAuthenticationPolicy require(String provider) {
        return (evidence, auth) -> {
            evidence.requireAvailableProvider(provider);
            return withBinding(auth, evidence.bindingDigest(List.of(provider)));
        };
    }

    public static PeerAuthenticationPolicy primary(String provider) {
        return (evidence, ignored) -> {
            PeerIdentity identity = evidence.requireUsableProvider(provider);
            Map<String, Object> claims = new LinkedHashMap<>();
            claims.put("issuer", identity.issuer());
            claims.put("subject_kind", identity.subjectKind().wireValue());
            claims.put("assurance", identity.assurance().wireValue());
            claims.put("evidence_source", identity.evidenceSource());
            claims.put("subject", identity.subjectKey());
            claims.put("peer_evidence_binding", evidence.bindingDigest(List.of(provider)));
            return new AuthContext(provider, true, identity.canonicalPrincipal(), claims);
        };
    }

    public static PeerAuthenticationPolicy anyOf(String... providers) {
        if (providers == null || providers.length == 0) throw new IllegalArgumentException("at least one provider is required");
        List<String> selected = List.of(providers);
        return (evidence, auth) -> {
            for (String provider : selected) {
                PeerIdentityStatus status = evidence.status(provider);
                if (status == PeerIdentityStatus.INVALID || status == PeerIdentityStatus.UNTRUSTED_PROXY) {
                    throw new PeerIdentityRejectedException("peer identity provider \"" + provider + "\" rejected evidence");
                }
                if (evidence.eligibleSubjects(provider).size() > 1) {
                    throw new PeerIdentityRejectedException("peer identity provider \"" + provider + "\" produced ambiguous subjects");
                }
            }
            if (auth.authenticated()) return auth;
            for (String provider : selected) {
                if (evidence.status(provider) == PeerIdentityStatus.AVAILABLE
                        && evidence.eligibleSubjects(provider).size() == 1) {
                    return primary(provider).evaluate(evidence, auth);
                }
            }
            if (selected.stream().anyMatch(provider -> evidence.status(provider) == PeerIdentityStatus.UNAVAILABLE
                    || evidence.status(provider) == PeerIdentityStatus.PERMISSION_DENIED)) {
                throw new PeerIdentityUnavailableException("no usable authentication factor; a peer provider is unavailable");
            }
            throw new PeerIdentityRejectedException("no configured provider produced a verified subject");
        };
    }

    public static PeerAuthenticationPolicy allOf(List<String> providers, PeerIdentityLinker linker) {
        return allOf(providers, linker, providers != null && !providers.isEmpty() ? providers.getFirst() : null);
    }

    public static PeerAuthenticationPolicy allOf(
            List<String> providers, PeerIdentityLinker linker, String principalProvider) {
        if (providers == null || providers.isEmpty() || linker == null) {
            throw new IllegalArgumentException("all-of requires providers and an identity linker");
        }
        List<String> selected = List.copyOf(providers);
        if (!selected.contains(principalProvider)) {
            throw new IllegalArgumentException("principalProvider must be one of providers");
        }
        return (evidence, auth) -> {
            if (!auth.authenticated()) throw new PeerIdentityRejectedException("all-of requires application authentication");
            Map<String, PeerIdentity> identities = new LinkedHashMap<>();
            selected.forEach(provider -> identities.put(provider, evidence.requireUsableProvider(provider)));
            linker.verify(auth, Map.copyOf(identities));
            PeerIdentity primary = identities.get(principalProvider);
            Map<String, Object> claims = new LinkedHashMap<>();
            claims.put("issuer", primary.issuer());
            claims.put("subject_kind", primary.subjectKind().wireValue());
            claims.put("assurance", primary.assurance().wireValue());
            claims.put("evidence_source", primary.evidenceSource());
            claims.put("subject", primary.subjectKey());
            claims.put("application_domain", auth.domain() != null ? auth.domain() : "");
            claims.put("application_principal", auth.principal() != null ? auth.principal() : "");
            claims.put("peer_evidence_binding", evidence.bindingDigest(selected, auth));
            return new AuthContext(principalProvider, true, primary.canonicalPrincipal(), claims);
        };
    }

    private static AuthContext withBinding(AuthContext auth, String binding) {
        Map<String, Object> claims = new LinkedHashMap<>(auth.claims());
        claims.put("peer_evidence_binding", binding);
        return new AuthContext(auth.domain(), auth.authenticated(), auth.principal(), claims);
    }
}
