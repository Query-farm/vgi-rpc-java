// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Tests for {@link ProxyProof}, including cross-language agreement. */
class ProxyProofTest {

    // Golden vectors from the Python reference implementation. Verifying these is the only thing
    // that proves Java frames the canonical string identically — a port can round-trip perfectly
    // against itself while framing the MAC input differently from every other language.
    private static final String GOLDEN_TOKEN =
            "v1.conformance-proxy.1700000000.Q0ZPUk1BTkNFTk9OQ0UxMQ.XQ2QBf35oajjaP7HIas3OfyEvNhyXTTptbrxWFxWk3I";
    private static final String GOLDEN_ORIGIN = "conformance-origin";
    private static final String GOLDEN_KID = "conformance-proxy";
    private static final long GOLDEN_TIME = 1700000000L;
    private static final String GOLDEN_DERIVED =
            "af85db125b8270bc0a0971736340dc8476ba70e1fad472b72b68ba739bd1cd94";
    private static final String GOLDEN_NONCE = "Q0ZPUk1BTkNFTk9OQ0UxMQ";

    private static byte[] secret() {
        byte[] s = new byte[32];
        java.util.Arrays.fill(s, (byte) 0x11);
        return s;
    }

    private static ProxyProof.Config config(long now) {
        Map<String, ProxyProof.Secret> secrets = new HashMap<>();
        secrets.put(GOLDEN_KID, new ProxyProof.Secret(secret(), GOLDEN_KID));
        return ProxyProof.Config.of(ProxyProof.Mode.REQUIRE, GOLDEN_ORIGIN, secrets)
                .withClock(() -> now);
    }

    private static String hex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    @Test
    @DisplayName("verifies a token minted by the Python reference implementation")
    void verifiesPythonMintedToken() throws Exception {
        Map<String, Object> claims = ProxyProof.verify(GOLDEN_TOKEN, config(GOLDEN_TIME), null);
        assertEquals("true", claims.get("verified"));
        assertEquals(GOLDEN_KID, claims.get("proxy"));
    }

    @Test
    @DisplayName("minting produces byte-identical output to Python")
    void mintMatchesPython() {
        String token = ProxyProof.mint(secret(), GOLDEN_KID, GOLDEN_ORIGIN, GOLDEN_TIME, GOLDEN_NONCE);
        assertEquals(GOLDEN_TOKEN, token, "Java mint diverged from the reference implementation");
    }

    @Test
    @DisplayName("secret derivation matches Python")
    void derivationMatchesPython() {
        byte[] base = new byte[32];
        for (int i = 0; i < base.length; i++) {
            base[i] = (byte) i;
        }
        assertEquals(GOLDEN_DERIVED, hex(ProxyProof.deriveSecret(base, "prod-use1", "worker-a")));
    }

    @Test
    @DisplayName("derivation boundaries cannot be shifted between the two ids")
    void derivationSeparatorIsUnambiguous() {
        byte[] base = new byte[32];
        assertNotEquals(
                hex(ProxyProof.deriveSecret(base, "ab", "c.d")),
                hex(ProxyProof.deriveSecret(base, "a", "b.c.d")));
    }

    @Test
    @DisplayName("malformed tokens are rejected as malformed")
    void malformedRejected() {
        ProxyProof.Config cfg = config(GOLDEN_TIME);
        String[] tokens = {
            "",
            "garbage",
            "v1.a.b.c",
            "v1.a.b.c.d.e",
            "v2." + GOLDEN_KID + ".1." + GOLDEN_NONCE + "." + "A".repeat(43),
            "v1.bad!kid.1." + GOLDEN_NONCE + "." + "A".repeat(43),
            "v1." + GOLDEN_KID + ".xyz." + GOLDEN_NONCE + "." + "A".repeat(43),
            "v1." + GOLDEN_KID + ".1.short." + "A".repeat(43),
            "v1." + GOLDEN_KID + ".1." + GOLDEN_NONCE + ".!!!",
            "v1." + "x".repeat(600),
        };
        for (String token : tokens) {
            ProxyProof.ProofFailure failure =
                    assertThrows(
                            ProxyProof.ProofFailure.class,
                            () -> ProxyProof.verify(token, cfg, null),
                            "accepted " + token);
            assertEquals("malformed", failure.reason(), "token " + token);
        }
    }

    @Test
    @DisplayName("an unconfigured key id is rejected")
    void unknownKidRejected() {
        Map<String, ProxyProof.Secret> secrets = new HashMap<>();
        secrets.put("other", new ProxyProof.Secret(secret(), "other"));
        ProxyProof.Config cfg =
                ProxyProof.Config.of(ProxyProof.Mode.REQUIRE, GOLDEN_ORIGIN, secrets)
                        .withClock(() -> GOLDEN_TIME);
        ProxyProof.ProofFailure failure =
                assertThrows(ProxyProof.ProofFailure.class, () -> ProxyProof.verify(GOLDEN_TOKEN, cfg, null));
        assertEquals("unknown_kid", failure.reason());
    }

    @Test
    @DisplayName("a proof minted for another worker does not verify here")
    void wrongOriginRejected() {
        // Audience binding: the origin id is folded into the MAC but never transmitted, so it
        // cannot be adjusted by the caller.
        Map<String, ProxyProof.Secret> secrets = new HashMap<>();
        secrets.put(GOLDEN_KID, new ProxyProof.Secret(secret(), GOLDEN_KID));
        ProxyProof.Config cfg =
                ProxyProof.Config.of(ProxyProof.Mode.REQUIRE, "some-other-worker", secrets)
                        .withClock(() -> GOLDEN_TIME);
        ProxyProof.ProofFailure failure =
                assertThrows(ProxyProof.ProofFailure.class, () -> ProxyProof.verify(GOLDEN_TOKEN, cfg, null));
        assertEquals("bad_mac", failure.reason());
    }

    @Test
    @DisplayName("the timestamp window is enforced at both ends")
    void timeWindowIsTwoSided() throws Exception {
        // The future case is what catches a verifier checking only an upper bound, which would let
        // a future-dated proof pass indefinitely — the defect in this package's SignedCookie helper.
        ProxyProof.ProofFailure expired =
                assertThrows(
                        ProxyProof.ProofFailure.class,
                        () -> ProxyProof.verify(GOLDEN_TOKEN, config(GOLDEN_TIME + 91), null));
        assertEquals("expired", expired.reason());

        ProxyProof.ProofFailure future =
                assertThrows(
                        ProxyProof.ProofFailure.class,
                        () -> ProxyProof.verify(GOLDEN_TOKEN, config(GOLDEN_TIME - 91), null));
        assertEquals("not_yet_valid", future.reason());

        assertEquals("true", ProxyProof.verify(GOLDEN_TOKEN, config(GOLDEN_TIME + 20), null).get("verified"));
    }

    @Test
    @DisplayName("a MAC over an incorrectly framed canonical string does not verify")
    void macFramingMustBeSeparated() {
        // Catches a port whose crypto is right but whose framing is not — the failure a
        // self-round-trip inside one implementation cannot reveal.
        String concatenated = "vgi.proxy.proof.v1" + GOLDEN_KID + "1700000000" + GOLDEN_NONCE + GOLDEN_ORIGIN;
        byte[] mac = Crypto.hmacSha256(secret(), concatenated.getBytes(StandardCharsets.UTF_8));
        String token =
                "v1."
                        + GOLDEN_KID
                        + ".1700000000."
                        + GOLDEN_NONCE
                        + "."
                        + Base64.getUrlEncoder().withoutPadding().encodeToString(mac);
        ProxyProof.ProofFailure failure =
                assertThrows(
                        ProxyProof.ProofFailure.class, () -> ProxyProof.verify(token, config(GOLDEN_TIME), null));
        assertEquals("bad_mac", failure.reason());
    }

    @Test
    @DisplayName("a replayed nonce is rejected")
    void replayRejected() throws Exception {
        ProxyProof.NonceCache cache = new ProxyProof.NonceCache(30, 100);
        ProxyProof.Config cfg = config(GOLDEN_TIME);
        assertEquals("true", ProxyProof.verify(GOLDEN_TOKEN, cfg, cache).get("verified"));
        ProxyProof.ProofFailure failure =
                assertThrows(ProxyProof.ProofFailure.class, () -> ProxyProof.verify(GOLDEN_TOKEN, cfg, cache));
        assertEquals("replayed", failure.reason());
    }

    @Test
    @DisplayName("the nonce cache enforces a hard capacity cap")
    void nonceCacheCapacityIsHard() {
        // A TTL bounds how long an entry lives, never how many arrive inside the window, so a
        // TTL-only cache is a remote memory-exhaustion vector.
        ProxyProof.NonceCache cache = new ProxyProof.NonceCache(3600, 10);
        for (int i = 0; i < 500; i++) {
            cache.checkAndAdd("nonce-" + i, GOLDEN_TIME);
        }
        assertTrue(cache.size() <= 10, "capacity cap not enforced: " + cache.size());
    }

    @Test
    @DisplayName("nonce entries expire past the TTL")
    void nonceCacheExpires() {
        ProxyProof.NonceCache cache = new ProxyProof.NonceCache(30, 100);
        assertTrue(cache.checkAndAdd("n1", 1000));
        assertFalse(cache.checkAndAdd("n1", 1000));
        assertTrue(cache.checkAndAdd("n1", 1031), "entry should expire past the TTL");
    }

    @Test
    @DisplayName("OFF mode installs no gate rather than a passing one")
    void offModeRefusesToBuild() {
        Map<String, ProxyProof.Secret> secrets = new HashMap<>();
        secrets.put(GOLDEN_KID, new ProxyProof.Secret(secret(), GOLDEN_KID));
        ProxyProof.Config cfg = ProxyProof.Config.of(ProxyProof.Mode.OFF, GOLDEN_ORIGIN, secrets);
        assertThrows(IllegalArgumentException.class, () -> ProxyProof.require(cfg, null));
    }

    @Test
    @DisplayName("secret specifications parse, and malformed ones are refused whole")
    void parseSecrets() {
        Map<String, ProxyProof.Secret> parsed = ProxyProof.parseSecrets("prod-use1:" + "11".repeat(32));
        assertEquals("prod-use1", parsed.get("prod-use1").label());
        for (String bad : new String[] {"prod-use1", "prod-use1:zz", "bad!kid:" + "11".repeat(32), ""}) {
            assertThrows(IllegalArgumentException.class, () -> ProxyProof.parseSecrets(bad), "accepted " + bad);
        }
    }
}
