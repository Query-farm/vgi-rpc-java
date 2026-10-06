// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.identity;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Sealed grants against the reference's {@code grant_token_vectors.json} (IDENTITY_V1_SPEC.md
 * §9), copied verbatim into test resources: every port mints and verifies byte-identically.
 *
 * <p>The file is decoded as UTF-8 explicitly -- one vector carries a non-ASCII audience, and a
 * platform-default decode (cp1252 on Windows) turns it into another audience entirely.
 */
final class SealedGrantVectorsTest {

    private static final JsonNode VECTORS = load();

    private static JsonNode load() {
        try (InputStream in = SealedGrantVectorsTest.class.getResourceAsStream("/grant_token_vectors.json")) {
            assertNotNull(in, "grant_token_vectors.json missing from test resources");
            return new ObjectMapper().readTree(new String(in.readAllBytes(), StandardCharsets.UTF_8));
        } catch (java.io.IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static byte[] b64(String s) { return Base64.getDecoder().decode(s); }

    private static GrantKeys verifyKeys(JsonNode c) {
        JsonNode d = VECTORS.get("defaults");
        List<byte[]> keys = new ArrayList<>();
        for (JsonNode k : (c.has("verify_keys_b64") ? c : d).get("verify_keys_b64")) keys.add(b64(k.asText()));
        String audience = (c.has("audience") ? c : d).get("audience").asText();
        long ttl = (c.has("max_ttl_seconds") ? c : d).get("max_ttl_seconds").asLong();
        long skew = (c.has("clock_skew_seconds") ? c : d).get("clock_skew_seconds").asLong();
        return new GrantKeys(keys, audience, ttl, skew);
    }

    private static double now(JsonNode c) {
        return (c.has("now") ? c : VECTORS.get("defaults")).get("now").asDouble();
    }

    @TestFactory
    List<DynamicTest> mintReproducesEveryVectorToken() {
        List<DynamicTest> tests = new ArrayList<>();
        for (JsonNode c : VECTORS.get("mint")) {
            tests.add(DynamicTest.dynamicTest(c.get("name").asText(), () -> {
                GrantKeys keys = new GrantKeys(List.of(b64(c.get("minting_key_b64").asText())),
                        c.get("audience").asText(), c.get("max_ttl_seconds").asLong());
                JsonNode r = c.get("request");
                List<String> scopes = new ArrayList<>();
                r.get("scopes").forEach(s -> scopes.add(s.asText()));
                assertEquals(c.get("kid_hex").asText(), HexFormat.of().formatHex(keys.mintingKid()));
                assertEquals(c.get("aad_hex").asText(), HexFormat.of().formatHex(keys.aad(keys.mintingKid())));
                SealedGrants.Minted m = SealedGrants.mint(keys, r.get("principal").asText(), scopes,
                        r.get("purpose").asText(), r.get("ttl_seconds").asLong(), c.get("now").asLong(),
                        r.get("grant_id").asText(), HexFormat.of().parseHex(c.get("nonce_hex").asText()));
                assertEquals(c.get("payload_hex").asText(),
                        HexFormat.of().formatHex(SealedGrants.encodePayload(m.claims())));
                assertEquals(c.get("token").asText(), m.token());
                // And it verifies back to the vector's claims.
                GrantClaims back = SealedGrants.verify(keys, m.token(), c.get("now").asDouble() + 1);
                JsonNode want = c.get("claims");
                assertEquals(want.get("principal").asText(), back.principal());
                assertEquals(want.get("expires_at").asLong(), back.expiresAt());
                assertEquals(scopes, back.scopes());
            }));
        }
        return tests;
    }

    @TestFactory
    List<DynamicTest> everyAcceptCaseVerifies() {
        List<DynamicTest> tests = new ArrayList<>();
        for (JsonNode c : VECTORS.get("accept")) {
            tests.add(DynamicTest.dynamicTest(c.get("name").asText(),
                    () -> assertNotNull(SealedGrants.verify(verifyKeys(c), c.get("token").asText(), now(c)))));
        }
        return tests;
    }

    @TestFactory
    List<DynamicTest> everyRejectCaseIsRefusedWithItsExpiredFlag() {
        List<DynamicTest> tests = new ArrayList<>();
        for (JsonNode c : VECTORS.get("reject")) {
            tests.add(DynamicTest.dynamicTest(c.get("name").asText(), () -> {
                GrantInvalidException e = assertThrows(GrantInvalidException.class,
                        () -> SealedGrants.verify(verifyKeys(c), c.get("token").asText(), now(c)));
                assertEquals(c.get("expired").asBoolean(), e.expired(), e.getMessage());
            }));
        }
        return tests;
    }

    @Test
    void hchacha20MatchesTheDraftVector() {
        // draft-irtf-cfrg-xchacha-03 §2.2.1.
        byte[] key = HexFormat.of().parseHex("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f");
        byte[] nonce = HexFormat.of().parseHex("000000090000004a0000000031415927");
        assertEquals("82413b4227b27bfed30e42508a877d73a0f9e4d58a74a853c12ec41326d3ecdc",
                HexFormat.of().formatHex(XChaCha20Poly1305.hchacha20(key, nonce)));
    }

    @Test
    void configuration() {
        String k = Base64.getEncoder().encodeToString(new byte[32]);
        assertNull(GrantKeys.fromEnv(Map.of()));
        GrantKeys keys = GrantKeys.fromEnv(Map.of(GrantKeys.KEYS_ENV, k, GrantKeys.AUDIENCE_ENV, "a",
                GrantKeys.MAX_TTL_ENV, "60"));
        assertEquals("a", keys.audience());
        assertEquals(60, keys.maxTtlSeconds());
        // A malformed key, a short key, duplicates, or a bad lifetime refuse to start.
        assertThrows(IllegalArgumentException.class, () -> GrantKeys.fromEnv(Map.of(GrantKeys.KEYS_ENV, "!!")));
        assertThrows(IllegalArgumentException.class,
                () -> GrantKeys.fromEnv(Map.of(GrantKeys.KEYS_ENV, Base64.getEncoder().encodeToString(new byte[16]))));
        assertThrows(IllegalArgumentException.class, () -> GrantKeys.fromEnv(Map.of(GrantKeys.KEYS_ENV, k + "," + k)));
        assertThrows(IllegalArgumentException.class,
                () -> GrantKeys.fromEnv(Map.of(GrantKeys.KEYS_ENV, k, GrantKeys.MAX_TTL_ENV, "0")));
        assertFalse(keys.toString().contains(k));
    }

    @Test
    void theSealedMinterRefusesANonPositiveLifetime() {
        GrantKeys keys = new GrantKeys(List.of(new byte[32]), "", 600);
        assertThrows(GrantRefusedError.class, () -> SealedGrants.minter(keys).mint("p", "x", List.of(), 0));
        IssuedGrant g = SealedGrants.minter(keys).mint("p", "x", List.of("r"), 60);
        GrantClaims c = SealedGrants.verify(keys, g.token());
        assertEquals("p", c.principal());
        assertEquals(g.grant_id(), c.grantId());
        assertEquals(32, c.grantId().length());
        assertArrayEquals(keys.mintingKid(), SealedGrants.keyId(new byte[32]));
    }
}
