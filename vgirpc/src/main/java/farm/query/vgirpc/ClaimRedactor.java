// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Decides what a call's authentication claims look like once they reach the
 * access log.
 *
 * <p>An access log outlives the token it describes by months or years and is
 * shipped to systems chosen for searchability rather than for holding personal
 * data, so standard OIDC claims ({@code email}, {@code phone_number},
 * {@code given_name}, …) and credential-shaped ones ({@code *_token},
 * {@code *_key}, {@code password}) must not reach it verbatim.
 *
 * <p>Redaction is <strong>key-based</strong>: a value is matched on the name it
 * arrived under, never on its content. A claim called {@code context} holding an
 * email address is not caught, and cannot be without guessing at free text — a
 * boundary worth stating rather than pretending to exceed.
 */
@FunctionalInterface
public interface ClaimRedactor {

    /** Placeholder substituted for a sensitive claim value. */
    String REDACTED = "[redacted]";

    /**
     * Return what should be logged for {@code claims}.
     *
     * @param claims the authenticated principal's raw claims; never {@code null}
     * @return the claims to log; implementations should return a fresh map rather
     *         than mutating the argument
     */
    Map<String, Object> redact(Map<String, Object> claims);

    /**
     * Names whose values are replaced: credentials first, then the standard OIDC
     * claims that are personal data.
     *
     * <p>{@code ^name$} is anchored because {@code name} alone is PII while
     * {@code token_name} and friends are already caught by the credential half.
     */
    Pattern SENSITIVE_CLAIM_NAMES = Pattern.compile(
            "password|token|secret|key|authorization"
            + "|email|phone|address|birthdate|gender"
            + "|^name$|given_name|family_name|middle_name|nickname|preferred_username"
            + "|picture|profile|website",
            Pattern.CASE_INSENSITIVE);

    /**
     * The default policy: replace sensitive values, keep every key.
     *
     * <p>Values are replaced rather than dropped because <em>which</em> claims a
     * credential carried is a question an audit log exists to answer; what they
     * contained is not.
     *
     * @return a redactor that substitutes {@link #REDACTED} for values whose key
     *         matches {@link #SENSITIVE_CLAIM_NAMES}
     */
    static ClaimRedactor byKeyName() {
        return claims -> {
            Map<String, Object> out = new LinkedHashMap<>(claims.size());
            for (Map.Entry<String, Object> e : claims.entrySet()) {
                out.put(e.getKey(),
                        SENSITIVE_CLAIM_NAMES.matcher(e.getKey()).find() ? REDACTED : e.getValue());
            }
            return out;
        };
    }

    /**
     * Pass claims through verbatim. Only for logs a service owns end to end.
     *
     * @return a redactor that copies the claims unchanged
     */
    static ClaimRedactor none() {
        return LinkedHashMap::new;
    }
}
