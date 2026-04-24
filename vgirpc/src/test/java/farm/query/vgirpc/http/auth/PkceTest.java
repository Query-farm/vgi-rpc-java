// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http.auth;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class PkceTest {

    @Test
    void code_verifier_is_43_url_safe_chars() {
        String v = Pkce.generateCodeVerifier();
        assertEquals(43, v.length());
        assertTrue(v.chars().allMatch(c -> (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
                || (c >= '0' && c <= '9') || c == '-' || c == '_'));
    }

    @Test
    void state_nonce_is_non_empty_url_safe() {
        String n = Pkce.generateStateNonce();
        assertTrue(n.length() >= 32);
        assertTrue(n.chars().allMatch(c -> (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
                || (c >= '0' && c <= '9') || c == '-' || c == '_'));
    }

    @Test
    void code_challenge_matches_rfc_7636_test_vector() {
        // RFC 7636 Appendix B vector: code verifier = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"
        String v = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk";
        String expected = "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM";
        assertEquals(expected, Pkce.codeChallengeS256(v));
    }

    @Test
    void code_challenges_are_deterministic() {
        String v = Pkce.generateCodeVerifier();
        assertEquals(Pkce.codeChallengeS256(v), Pkce.codeChallengeS256(v));
    }

    @Test
    void distinct_verifiers_yield_distinct_challenges() {
        String v1 = Pkce.generateCodeVerifier();
        String v2 = Pkce.generateCodeVerifier();
        assertNotEquals(v1, v2);
        assertNotEquals(Pkce.codeChallengeS256(v1), Pkce.codeChallengeS256(v2));
    }
}
