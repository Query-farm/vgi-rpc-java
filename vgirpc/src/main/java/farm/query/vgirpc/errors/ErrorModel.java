// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.errors;

import com.fasterxml.jackson.databind.ObjectMapper;
import farm.query.vgirpc.HasErrorKind;
import farm.query.vgirpc.RpcError;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The rules of the error model, in one place: reading the three layers off an exception,
 * applying the catalog rules and the 4 KiB cap to a details array, decoding one tolerantly, and
 * classifying retryability.
 */
public final class ErrorModel {

    /** Cap on the serialized {@code vgi_rpc.error_details} value, in UTF-8 bytes. A server whose
     *  array would exceed it omits the array entirely -- never a prefix, never a subset. */
    public static final int MAX_ERROR_DETAILS_BYTES = 4096;

    private static final String RESERVED_PREFIX = "vgi_rpc.";
    private static final ObjectMapper JSON = new ObjectMapper();

    private ErrorModel() {}

    /**
     * The canonical code an exception declares, or {@link Code#UNKNOWN}.
     *
     * <p>A {@link HasErrorCode} names its own. A re-thrown client-side {@link RpcError} relays the
     * code it was decoded with. Anything else is unclassified, and {@code UNKNOWN} is the honest
     * answer for that -- every kind the framework defines names its code where it is defined.
     *
     * @param t the exception being reported
     * @return its code; never {@code null}
     */
    public static Code codeOf(Throwable t) {
        if (t instanceof HasErrorCode hc) {
            Code c = hc.errorCode();
            return c == null ? Code.UNKNOWN : c;
        }
        if (t instanceof RpcError rpc) return Code.parse(rpc.errorCode());
        return Code.UNKNOWN;
    }

    /**
     * The non-empty {@code error_kind} an exception declares, or {@code null}.
     *
     * @param t the exception being reported
     * @return the kind, or {@code null}
     */
    public static String kindOf(Throwable t) {
        if (t instanceof HasErrorKind hk) {
            String k = hk.errorKind();
            return k == null || k.isEmpty() ? null : k;
        }
        return null;
    }

    /**
     * The detail objects an exception declares. Never throws: an exception whose details are
     * broken still has to be reported.
     *
     * @param t the exception being reported
     * @return its details as JSON objects, or an empty list
     */
    public static List<Map<String, Object>> detailsOf(Throwable t) {
        try {
            List<Map<String, Object>> raw = null;
            if (t instanceof HasErrorDetails hd) raw = hd.errorDetails();
            else if (t instanceof RpcError rpc) raw = rpc.errorDetails();
            if (raw == null) return List.of();
            List<Map<String, Object>> out = new ArrayList<>(raw.size());
            for (Map<String, Object> m : raw) out.add(m == null ? Map.of() : new LinkedHashMap<>(m));
            return out;
        } catch (RuntimeException broken) {
            return List.of();
        }
    }

    /**
     * Convert typed details to their JSON object forms.
     *
     * @param details typed catalog details
     * @return their JSON objects, in order
     */
    public static List<Map<String, Object>> toJson(List<? extends ErrorDetail> details) {
        List<Map<String, Object>> out = new ArrayList<>();
        if (details != null) for (ErrorDetail d : details) out.add(d.toJson());
        return out;
    }

    /**
     * Apply the catalog rules to a details array.
     *
     * @param details the detail objects, in order
     * @throws IllegalArgumentException when a type is missing, repeated, unqualified, or claims
     *     the reserved {@code vgi_rpc.} prefix without being in the catalog
     */
    public static void validate(List<Map<String, Object>> details) {
        Set<String> seen = new HashSet<>();
        for (Map<String, Object> obj : details) {
            Object type = obj == null ? null : obj.get("@type");
            if (!(type instanceof String t) || t.isEmpty()) {
                throw new IllegalArgumentException("every error detail must name its type in '@type'");
            }
            if (!seen.add(t)) {
                throw new IllegalArgumentException("error detail type '" + t + "' appears more than once");
            }
            if (t.startsWith(RESERVED_PREFIX) && !ErrorDetail.inCatalog(t)) {
                throw new IllegalArgumentException("'" + t + "' claims the reserved 'vgi_rpc.' prefix "
                        + "but is not in the catalog. A protocol-defined detail type must live under "
                        + "its own protocol's name.");
            }
            if (t.indexOf('.') < 0) {
                throw new IllegalArgumentException("'" + t + "' is not qualified; protocol-defined "
                        + "types live under the protocol's name");
            }
        }
    }

    /**
     * Serialize a details array for {@code vgi_rpc.error_details}, enforcing the rules.
     *
     * <p>Returns {@code null} -- meaning <em>omit the key</em> -- for an empty array, for one that
     * breaks a catalog rule, and for one whose serialized form exceeds
     * {@link #MAX_ERROR_DETAILS_BYTES}. The array is dropped whole: a client cannot tell a
     * truncated list from a complete one, so a partial list is worse than none.
     *
     * @param details the detail objects
     * @return compact JSON text, or {@code null}
     */
    public static String encode(List<Map<String, Object>> details) {
        if (details == null || details.isEmpty()) return null;
        String text;
        try {
            validate(details);
            text = JSON.writeValueAsString(details);
        } catch (Exception broken) {
            return null;
        }
        if (text.getBytes(StandardCharsets.UTF_8).length > MAX_ERROR_DETAILS_BYTES) return null;
        return text;
    }

    /**
     * Decode a {@code vgi_rpc.error_details} value into its JSON objects.
     *
     * <p>Tolerant by design: anything that is not a JSON array decodes as empty, and non-object
     * elements are skipped. Unknown {@code @type} values are kept -- filtering to the catalog is
     * what the typed accessors do.
     *
     * @param raw the metadata value, or {@code null} when absent
     * @return the detail objects, in wire order
     */
    public static List<Map<String, Object>> decode(String raw) {
        if (raw == null) return List.of();
        Object parsed;
        try {
            parsed = JSON.readValue(raw, Object.class);
        } catch (Exception malformed) {
            return List.of();
        }
        return objectsOf(parsed);
    }

    /**
     * Keep the object elements of an already-decoded JSON value, or nothing when it is not an
     * array -- the {@code log_extra} mirror's decoding.
     *
     * @param value a decoded JSON value
     * @return its object elements, in order
     */
    @SuppressWarnings("unchecked")
    public static List<Map<String, Object>> objectsOf(Object value) {
        if (!(value instanceof List<?> list)) return List.of();
        List<Map<String, Object>> out = new ArrayList<>();
        for (Object o : list) {
            if (o instanceof Map<?, ?> m) out.add(new LinkedHashMap<>((Map<String, Object>) m));
        }
        return out;
    }

    /**
     * The details this client understands, in wire order; unknown and malformed ones skipped.
     *
     * @param details the raw detail objects
     * @return the typed details
     */
    public static List<ErrorDetail> typed(List<Map<String, Object>> details) {
        List<ErrorDetail> out = new ArrayList<>();
        if (details == null) return out;
        for (Map<String, Object> m : details) {
            ErrorDetail d = ErrorDetail.parse(m);
            if (d != null) out.add(d);
        }
        return out;
    }

    /**
     * Whether the rule in WIRE_PROTOCOL.md §8 calls an error retryable.
     *
     * <p>{@code UNAVAILABLE} is; {@code RESOURCE_EXHAUSTED} is only with {@code RetryInfo}.
     * Everything else is final -- {@code ABORTED} included, which means "retry the whole operation
     * at a higher level", not this call. A classification, not a policy: nothing in this library
     * retries an RPC error automatically, because a method may not be idempotent.
     *
     * @param code the canonical code
     * @param details the detail objects
     * @return whether retrying this call is warranted
     */
    public static boolean isRetryable(Code code, List<Map<String, Object>> details) {
        if (code == Code.UNAVAILABLE) return true;
        if (code == Code.RESOURCE_EXHAUSTED) {
            for (ErrorDetail d : typed(details)) {
                if (d instanceof ErrorDetail.RetryInfo) return true;
            }
        }
        return false;
    }
}
