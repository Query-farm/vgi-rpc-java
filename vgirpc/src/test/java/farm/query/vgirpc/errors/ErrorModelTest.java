// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.errors;

import com.fasterxml.jackson.databind.ObjectMapper;
import farm.query.vgirpc.AuthContext;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.MethodNotImplementedError;
import farm.query.vgirpc.ProtocolNotSpecifiedError;
import farm.query.vgirpc.ProtocolNotSupportedError;
import farm.query.vgirpc.ProtocolVersionError;
import farm.query.vgirpc.ResponseTooLargeError;
import farm.query.vgirpc.RpcError;
import farm.query.vgirpc.ServerDrainingError;
import farm.query.vgirpc.SessionLostError;
import farm.query.vgirpc.http.AuthUnavailableException;
import farm.query.vgirpc.identity.GrantRefusedError;
import farm.query.vgirpc.identity.Identity;
import farm.query.vgirpc.identity.IdentityImpl;
import farm.query.vgirpc.identity.IdentityUnavailableError;
import farm.query.vgirpc.identity.IntrospectionRefusedError;
import farm.query.vgirpc.identity.StaleAuthError;
import farm.query.vgirpc.identity.TokenUnresolvedError;
import farm.query.vgirpc.wire.Metadata;
import farm.query.vgirpc.wire.Wire;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The error model's rules, with the reference's test vectors (MULTI_PROTOCOL_HOSTING.md §3). */
final class ErrorModelTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    private static Map<String, Object> obj(Object... kv) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) m.put((String) kv[i], kv[i + 1]);
        return m;
    }

    private static List<Map<String, Object>> paddedErrorInfo(String padding) {
        return List.of(obj("@type", "vgi_rpc.ErrorInfo", "metadata", Map.of("p", padding)));
    }

    // --- V5: the cap boundary -------------------------------------------------------

    @Test
    void theCapIsInclusiveAt4096Bytes() {
        String at = ErrorModel.encode(paddedErrorInfo("x".repeat(4045)));
        assertNotNull(at);
        assertEquals(4096, at.getBytes(java.nio.charset.StandardCharsets.UTF_8).length);
        assertNull(ErrorModel.encode(paddedErrorInfo("x".repeat(4046))));
    }

    @Test
    void theCapIsMeasuredInUtf8Bytes() {
        List<Map<String, Object>> d = List.of(obj("@type", "vgi_rpc.LocalizedMessage",
                "locale", "fr", "message", "é".repeat(2048)));
        assertNull(ErrorModel.encode(d));
    }

    @Test
    void anOversizedArrayIsDroppedWholeOnTheWire() {
        StatusError e = new StatusError("big", Code.RESOURCE_EXHAUSTED, "details_oversized", List.of(
                new ErrorDetail.RetryInfo(1),
                new ErrorDetail.ErrorInfo(Map.of("padding", "x".repeat(5000)))));
        Map<String, String> md = Wire.errorMetadata(e, "s", false);
        assertEquals("RESOURCE_EXHAUSTED", md.get(Metadata.ERROR_CODE));
        assertEquals("details_oversized", md.get(Metadata.ERROR_KIND));
        assertFalse(md.containsKey(Metadata.ERROR_DETAILS));
        assertFalse(md.get(Metadata.LOG_EXTRA).contains("error_details"));
        RpcError back = Wire.errorFromMetadata(md);
        assertEquals(List.of(), back.errorDetails());
        assertFalse(back.isRetryable(), "a partially delivered RetryInfo would make this retryable");
    }

    // --- V6: rule violations are dropped at emission ---------------------------------

    @Test
    void ruleViolationsAreDropped() {
        assertNull(ErrorModel.encode(List.of(
                obj("@type", "vgi_rpc.RetryInfo", "retry_delay_seconds", 1),
                obj("@type", "vgi_rpc.RetryInfo", "retry_delay_seconds", 2))));
        assertNull(ErrorModel.encode(List.of(obj("@type", "vgi_rpc.Made.Up"))));
        assertNull(ErrorModel.encode(List.of(obj("@type", "Unqualified"))));
        assertNull(ErrorModel.encode(List.of(obj("note", "no type"))));
        assertNotNull(ErrorModel.encode(List.of(obj("@type", "my.proto.v1.Custom"))));
        assertThrows(IllegalArgumentException.class, () -> new StatusError(Code.UNKNOWN, "m", null,
                List.of(obj("@type", "vgi_rpc.Made.Up"))));
    }

    // --- V7: client tolerance --------------------------------------------------------

    @Test
    void decodingIsTolerant() {
        assertEquals(List.of(), ErrorModel.decode("{\"@type\":\"vgi_rpc.RetryInfo\"}"));
        assertEquals(List.of(), ErrorModel.decode("not json"));
        List<Map<String, Object>> raw = ErrorModel.decode("[1, {\"@type\":\"x.y.Unknown\"},"
                + "{\"@type\":\"vgi_rpc.RetryInfo\",\"retry_delay_seconds\":\"soon\"},"
                + "{\"@type\":\"vgi_rpc.ErrorInfo\",\"metadata\":{\"a\":1}},"
                + "{\"@type\":\"vgi_rpc.QuotaFailure\",\"violations\":[]}]");
        assertEquals(4, raw.size(), "non-objects skipped, unknown and malformed kept raw");
        List<ErrorDetail> typed = ErrorModel.typed(raw);
        assertEquals(1, typed.size());
        assertTrue(typed.get(0) instanceof ErrorDetail.QuotaFailure);
        assertTrue(ErrorModel.typed(List.of(obj("@type", "vgi_rpc.RetryInfo",
                "retry_delay_seconds", -1))).isEmpty());
    }

    // --- V3: retryability ------------------------------------------------------------

    @Test
    void retryabilityFollowsTheCode() {
        List<Map<String, Object>> retry = List.of(new ErrorDetail.RetryInfo(3).toJson());
        assertTrue(ErrorModel.isRetryable(Code.UNAVAILABLE, List.of()));
        assertTrue(ErrorModel.isRetryable(Code.RESOURCE_EXHAUSTED, retry));
        assertFalse(ErrorModel.isRetryable(Code.RESOURCE_EXHAUSTED, List.of()));
        assertFalse(ErrorModel.isRetryable(Code.ABORTED, retry));
        assertFalse(ErrorModel.isRetryable(Code.INTERNAL, retry));
        assertFalse(new RpcError("E", "m", "", "", null, "", List.of()).isRetryable());
        assertFalse(new RpcError("E", "m", "", "", null, "NOT_A_CODE", List.of()).isRetryable());
        assertEquals(Code.UNKNOWN, new RpcError("E", "m", "", "", null, "NOT_A_CODE", List.of()).code());
    }

    // --- V1 / V2 through the wire ----------------------------------------------------

    @Test
    void theThreeLayersRoundTripAndAreMirrored() throws Exception {
        List<Map<String, Object>> details = new ArrayList<>();
        details.add(new ErrorDetail.ErrorInfo(Map.of("fixture", "conformance.Secondary.v1")).toJson());
        details.add(new ErrorDetail.RetryInfo(7).toJson());
        details.add(obj("@type", "conformance.Secondary.v1.Probe", "note", "unknown"));
        StatusError e = new StatusError(Code.UNAVAILABLE, "down", "backend_down", details);

        Map<String, String> md = Wire.errorMetadata(e, "s", false);
        assertEquals("UNAVAILABLE", md.get(Metadata.ERROR_CODE));
        assertEquals("backend_down", md.get(Metadata.ERROR_KIND));
        assertTrue(md.get(Metadata.ERROR_DETAILS).contains("\"retry_delay_seconds\":7"),
                md.get(Metadata.ERROR_DETAILS));
        @SuppressWarnings("unchecked")
        Map<String, Object> extra = JSON.readValue(md.get(Metadata.LOG_EXTRA), Map.class);
        assertEquals("UNAVAILABLE", extra.get("error_code"));
        assertEquals("backend_down", extra.get("error_kind"));
        assertTrue(extra.get("error_details") instanceof List<?>, "the mirror is an array, not a string");
        assertFalse(extra.containsKey("traceback"));

        RpcError back = Wire.errorFromMetadata(md);
        assertEquals("UNAVAILABLE", back.errorCode());
        assertEquals("backend_down", back.errorKind());
        assertEquals(3, back.errorDetails().size());
        assertEquals(7.0, back.retryInfo().retryDelaySeconds());
        assertEquals(Map.of("fixture", "conformance.Secondary.v1"), back.errorInfo().metadata());
        assertEquals(2, back.details().size(), "the probe is skipped by typed access");
        assertTrue(back.isRetryable());

        // Read off the mirror when the top-level keys are gone.
        Map<String, String> mirrorOnly = new HashMap<>(md);
        mirrorOnly.remove(Metadata.ERROR_CODE);
        mirrorOnly.remove(Metadata.ERROR_KIND);
        mirrorOnly.remove(Metadata.ERROR_DETAILS);
        RpcError fromMirror = Wire.errorFromMetadata(mirrorOnly);
        assertEquals("UNAVAILABLE", fromMirror.errorCode());
        assertEquals("backend_down", fromMirror.errorKind());
        assertEquals(3, fromMirror.errorDetails().size());
    }

    @Test
    void aCodeWithoutAKindAndAnUnclassifiedError() {
        Map<String, String> md = Wire.errorMetadata(new StatusError("x", Code.ABORTED), "s", true);
        assertEquals("ABORTED", md.get(Metadata.ERROR_CODE));
        assertFalse(md.containsKey(Metadata.ERROR_KIND));
        assertTrue(md.get(Metadata.LOG_EXTRA).contains("traceback"));
        Map<String, String> plain = Wire.errorMetadata(new IllegalStateException("boom"), "s", true);
        assertEquals("UNKNOWN", plain.get(Metadata.ERROR_CODE));
        // A server older than the model sent no code: "" stays "", never defaulted.
        Map<String, String> old = new HashMap<>(plain);
        old.remove(Metadata.ERROR_CODE);
        old.put(Metadata.LOG_EXTRA, "{\"exception_type\":\"ValueError\"}");
        assertEquals("", Wire.errorFromMetadata(old).errorCode());
    }

    // --- V8: every framework kind carries its code -----------------------------------

    @Test
    void everyFrameworkKindCarriesItsCode() {
        Map<Throwable, Code> table = new LinkedHashMap<>();
        table.put(new MethodNotImplementedError("m"), Code.UNIMPLEMENTED);
        table.put(new ProtocolNotSupportedError("m"), Code.UNIMPLEMENTED);
        table.put(new ProtocolNotSpecifiedError("m"), Code.INVALID_ARGUMENT);
        table.put(new ProtocolVersionError("m", "p", "1.0.0", "2.0.0"), Code.FAILED_PRECONDITION);
        table.put(new SessionLostError("m"), Code.ABORTED);
        table.put(new ServerDrainingError("m"), Code.UNAVAILABLE);
        table.put(new IdentityUnavailableError("m"), Code.UNAVAILABLE);
        table.put(new StaleAuthError("m"), Code.UNAUTHENTICATED);
        table.put(new IntrospectionRefusedError("m"), Code.PERMISSION_DENIED);
        table.put(new GrantRefusedError("m"), Code.PERMISSION_DENIED);
        table.put(new TokenUnresolvedError("m"), Code.NOT_FOUND);
        table.put(new ResponseTooLargeError("m", 2, 1), Code.RESOURCE_EXHAUSTED);
        for (Map.Entry<Throwable, Code> e : table.entrySet()) {
            Map<String, String> md = Wire.errorMetadata(e.getKey(), "s", false);
            assertEquals(e.getValue().name(), md.get(Metadata.ERROR_CODE), e.getKey().getClass().getName());
        }
        assertTrue(Wire.errorMetadata(new ServerDrainingError("m"), "s", false)
                .get(Metadata.ERROR_DETAILS).contains("vgi_rpc.RetryInfo"));
        assertTrue(Wire.errorMetadata(new IdentityUnavailableError("m", 9, null), "s", false)
                .get(Metadata.ERROR_DETAILS).contains("\"retry_delay_seconds\":9"));
        assertFalse(Wire.errorMetadata(new ResponseTooLargeError("m", 2, 1), "s", false)
                .containsKey(Metadata.ERROR_DETAILS));
    }

    // --- §16: the identity translation ------------------------------------------------

    private static CallContext ctx(String principal, Double authTime) {
        Map<String, Object> claims = new HashMap<>();
        if (authTime != null) claims.put("auth_time", authTime);
        return new CallContext(new AuthContext("test", true, principal, claims), msg -> { }, Map.of(),
                "srv", "m", Identity.PROTOCOL_NAME, "");
    }

    @Test
    void anAuthUnavailableHookIsTranslatedKeepingItsHint() {
        IdentityImpl impl = IdentityImpl.builder()
                .resolveToken(token -> {
                    throw new AuthUnavailableException("store down", 7, null);
                })
                .mintGrant((principal, purpose, scopes, ttl) -> {
                    throw new AuthUnavailableException("grant store down", 7, null);
                })
                .introspectPrincipals("proxy")
                .build();
        IdentityUnavailableError resolved = assertThrows(IdentityUnavailableError.class,
                () -> impl.introspect_token("opaque-token", ctx("proxy", null)));
        assertEquals(7, resolved.retryAfterSeconds());
        IdentityUnavailableError minted = assertThrows(IdentityUnavailableError.class,
                () -> impl.issue_grant("p", List.of(), 60, ctx("alice", System.currentTimeMillis() / 1000.0)));
        assertEquals(7, minted.retryAfterSeconds());
        Map<String, String> md = Wire.errorMetadata(minted, "s", false);
        assertEquals("identity_unavailable", md.get(Metadata.ERROR_KIND));
        assertEquals("UNAVAILABLE", md.get(Metadata.ERROR_CODE));
        assertTrue(md.get(Metadata.ERROR_DETAILS).contains("\"retry_delay_seconds\":7"));
    }
}
