// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0
package farm.query.vgirpc.identity;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/** Immutable verified or observed evidence about one transport peer. */
public record PeerIdentity(
        String provider,
        String evidenceSource,
        IdentityAssurance assurance,
        String issuer,
        String transport,
        PeerSubjectKind subjectKind,
        String subjectKey,
        SubjectStability subjectStability,
        boolean subjectVerified,
        Map<String, Object> attributes,
        Map<String, Object> capabilities,
        boolean capabilitiesVerified,
        String sourceAddress,
        String proxyAddress) {

    public PeerIdentity {
        if (provider == null || provider.isBlank() || evidenceSource == null || evidenceSource.isBlank()
                || issuer == null || issuer.isBlank() || transport == null || transport.isBlank()) {
            throw new IllegalArgumentException("provider, evidenceSource, issuer, and transport are required");
        }
        if (assurance == null) throw new IllegalArgumentException("assurance is required");
        JsonValues.requireWellFormed(provider, "provider");
        JsonValues.requireWellFormed(evidenceSource, "evidenceSource");
        JsonValues.requireWellFormed(issuer, "issuer");
        JsonValues.requireWellFormed(transport, "transport");
        if (subjectKey != null) JsonValues.requireWellFormed(subjectKey, "subjectKey");
        if (sourceAddress != null) JsonValues.requireWellFormed(sourceAddress, "sourceAddress");
        if (proxyAddress != null) JsonValues.requireWellFormed(proxyAddress, "proxyAddress");
        subjectKind = subjectKind != null ? subjectKind : PeerSubjectKind.UNKNOWN;
        subjectStability = subjectStability != null ? subjectStability : SubjectStability.NONE;
        if (subjectVerified && (subjectKey == null || subjectKey.isEmpty())) {
            throw new IllegalArgumentException("verified peer identity requires subjectKey");
        }
        if (subjectKey == null && subjectStability != SubjectStability.NONE) {
            throw new IllegalArgumentException("subjectless peer identity must use NONE stability");
        }
        attributes = JsonValues.snapshotMap(attributes);
        capabilities = JsonValues.snapshotMap(capabilities);
    }

    /** Provider/issuer-namespaced stable principal suitable for AuthContext. */
    public String canonicalPrincipal() {
        if (subjectKey == null || subjectKey.isEmpty()) {
            throw new IllegalStateException("subjectless peer evidence has no canonical principal");
        }
        return "peer/" + percent(provider) + "/" + percent(issuer) + "/" + percent(subjectKey);
    }

    private static String percent(String input) {
        byte[] bytes = input.getBytes(StandardCharsets.UTF_8);
        StringBuilder text = new StringBuilder();
        for (byte raw : bytes) {
            int value = raw & 0xff;
            if ((value >= 'a' && value <= 'z') || (value >= 'A' && value <= 'Z')
                    || (value >= '0' && value <= '9') || value == '-' || value == '.' || value == '_' || value == '~') {
                text.append((char) value);
            } else {
                text.append('%');
                text.append(Character.toUpperCase(Character.forDigit(value >>> 4, 16)));
                text.append(Character.toUpperCase(Character.forDigit(value & 15, 16)));
            }
        }
        return text.toString();
    }
}
