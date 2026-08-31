// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0
package farm.query.vgirpc.identity;

import farm.query.vgirpc.AuthContext;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Immutable aggregate of every peer provider's result for one call. */
public final class PeerEvidenceSet {
    public static final PeerEvidenceSet EMPTY = new PeerEvidenceSet(List.of());

    private final List<PeerIdentity> identities;
    private final Map<String, PeerIdentityStatus> statuses;

    public PeerEvidenceSet(List<PeerIdentityResult> results) {
        List<PeerIdentity> found = new ArrayList<>();
        Map<String, PeerIdentityStatus> byProvider = new LinkedHashMap<>();
        for (PeerIdentityResult result : results != null ? results : List.<PeerIdentityResult>of()) {
            if (byProvider.putIfAbsent(result.provider(), result.status()) != null) {
                throw new IllegalArgumentException("duplicate peer identity provider: " + result.provider());
            }
            found.addAll(result.identities());
        }
        identities = List.copyOf(found);
        statuses = Collections.unmodifiableMap(byProvider);
    }

    public List<PeerIdentity> identities() { return identities; }
    public Map<String, PeerIdentityStatus> providerStatuses() { return statuses; }
    public PeerIdentityStatus status(String provider) { return statuses.getOrDefault(provider, PeerIdentityStatus.OFF); }
    public List<PeerIdentity> forProvider(String provider) {
        return identities.stream().filter(identity -> identity.provider().equals(provider)).toList();
    }
    public List<PeerIdentity> eligibleSubjects(String provider) {
        return forProvider(provider).stream().filter(identity -> identity.subjectVerified()
                && identity.subjectKey() != null && identity.subjectStability() == SubjectStability.STABLE).toList();
    }
    public PeerIdentity uniqueVerifiedSubject(String provider) {
        List<PeerIdentity> matches = eligibleSubjects(provider);
        if (matches.size() != 1) {
            throw new PeerIdentityRejectedException("provider \"" + provider + "\" did not produce one verified stable subject");
        }
        return matches.getFirst();
    }
    public PeerIdentity requireUsableProvider(String provider) {
        PeerIdentityStatus status = status(provider);
        if (status == PeerIdentityStatus.UNAVAILABLE || status == PeerIdentityStatus.PERMISSION_DENIED) {
            throw new PeerIdentityUnavailableException("peer identity provider \"" + provider + "\" is unavailable");
        }
        if (status == PeerIdentityStatus.INVALID || status == PeerIdentityStatus.UNTRUSTED_PROXY) {
            throw new PeerIdentityRejectedException("peer identity provider \"" + provider + "\" rejected evidence");
        }
        return uniqueVerifiedSubject(provider);
    }
    public List<PeerIdentity> requireAvailableProvider(String provider) {
        PeerIdentityStatus status = status(provider);
        if (status == PeerIdentityStatus.UNAVAILABLE || status == PeerIdentityStatus.PERMISSION_DENIED) {
            throw new PeerIdentityUnavailableException("peer identity provider \"" + provider + "\" is unavailable");
        }
        if (status == PeerIdentityStatus.INVALID || status == PeerIdentityStatus.UNTRUSTED_PROXY) {
            throw new PeerIdentityRejectedException("peer identity provider \"" + provider + "\" rejected evidence");
        }
        List<PeerIdentity> found = forProvider(provider);
        if (status != PeerIdentityStatus.AVAILABLE || found.isEmpty()) {
            throw new PeerIdentityRejectedException("peer identity provider \"" + provider + "\" did not produce evidence");
        }
        return found;
    }

    /** SHA-256 digest used to bind cursors, sessions, caches, and audit state. */
    public String bindingDigest(List<String> providers) { return bindingDigest(providers, null); }
    public String bindingDigest(List<String> providers, AuthContext applicationAuth) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            Set<String> selected = new LinkedHashSet<>(providers);
            List<String> sortedProviders = new ArrayList<>(selected);
            sortedProviders.sort(PeerEvidenceSet::compareUtf8);
            for (String provider : sortedProviders) {
                add(digest, provider);
                add(digest, status(provider).wireValue());
                List<List<String>> rows = new ArrayList<>();
                for (PeerIdentity identity : forProvider(provider)) rows.add(fields(identity));
                rows.sort(PeerEvidenceSet::compareFields);
                rows.forEach(row -> row.forEach(field -> add(digest, field)));
            }
            if (applicationAuth != null) {
                add(digest, "application_auth");
                add(digest, applicationAuth.domain() != null ? applicationAuth.domain() : "");
                add(digest, applicationAuth.principal() != null ? applicationAuth.principal() : "");
            }
            return java.util.HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    private static List<String> fields(PeerIdentity identity) {
        return List.of(identity.provider(), identity.issuer(), value(identity.subjectKey()),
                identity.assurance().wireValue(), identity.evidenceSource(), identity.transport(),
                identity.subjectKind().wireValue(), identity.subjectStability().wireValue(),
                Boolean.toString(identity.subjectVerified()), Boolean.toString(identity.capabilitiesVerified()),
                "", "",
                JsonValues.canonicalJson(identity.attributes()), JsonValues.canonicalJson(identity.capabilities()));
    }
    private static String value(String value) { return value != null ? value : ""; }
    private static void add(MessageDigest digest, String field) {
        byte[] bytes = field.getBytes(StandardCharsets.UTF_8);
        digest.update(ByteBuffer.allocate(Long.BYTES).putLong(bytes.length).array());
        digest.update(bytes);
    }
    private static int compareFields(List<String> left, List<String> right) {
        for (int index = 0; index < left.size(); index++) {
            int comparison = compareUtf8(left.get(index), right.get(index));
            if (comparison != 0) return comparison;
        }
        return 0;
    }
    private static int compareUtf8(String left, String right) {
        return java.util.Arrays.compareUnsigned(left.getBytes(StandardCharsets.UTF_8), right.getBytes(StandardCharsets.UTF_8));
    }
}
