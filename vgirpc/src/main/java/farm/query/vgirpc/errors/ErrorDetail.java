// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.errors;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * One member of the fixed detail catalog (WIRE_PROTOCOL.md §8).
 *
 * <p>Each detail is a JSON object naming its type in {@code @type}, mirroring protobuf's JSON form
 * for {@code Any}; field names follow gRPC's, in snake_case. The catalog is closed: a protocol may
 * define its own detail type only under its own protocol name, and should prefer
 * {@link ErrorInfo#metadata()}. {@code DebugInfo} is deliberately absent -- tracebacks are a
 * server setting, not a detail.
 *
 * <p>Absent string fields read as {@code ""} and absent arrays as empty, as the spec requires.
 */
public sealed interface ErrorDetail {

    /**
     * The {@code @type} naming this detail on the wire.
     *
     * @return the qualified type name, e.g. {@code "vgi_rpc.RetryInfo"}
     */
    String type();

    /**
     * The JSON object form, {@code @type} included.
     *
     * @return a fresh mutable map, safe to serialize
     */
    Map<String, Object> toJson();

    /**
     * Extra context for the reason. The reason and its domain are already {@code error_kind} and
     * the protocol, so neither is repeated here.
     *
     * @param metadata string-to-string context; never credentials or user data
     */
    record ErrorInfo(Map<String, String> metadata) implements ErrorDetail {
        /** The wire type name. */
        public static final String TYPE = "vgi_rpc.ErrorInfo";

        /** Normalise a {@code null} map to empty and freeze it, preserving order. */
        public ErrorInfo {
            metadata = metadata == null
                    ? Map.of() : java.util.Collections.unmodifiableMap(new LinkedHashMap<>(metadata));
        }

        @Override public String type() { return TYPE; }

        @Override
        public Map<String, Object> toJson() {
            Map<String, Object> out = header(TYPE);
            out.put("metadata", new LinkedHashMap<>(metadata));
            return out;
        }

        static ErrorInfo fromJson(Map<?, ?> obj) {
            Object raw = obj.get("metadata");
            if (raw == null) return new ErrorInfo(Map.of());
            if (!(raw instanceof Map<?, ?> m)) throw new IllegalArgumentException("'metadata' must be an object");
            Map<String, String> out = new LinkedHashMap<>();
            for (Map.Entry<?, ?> e : m.entrySet()) {
                if (!(e.getValue() instanceof String s)) {
                    throw new IllegalArgumentException("'metadata' must be an object of strings");
                }
                out.put(String.valueOf(e.getKey()), s);
            }
            return new ErrorInfo(out);
        }
    }

    /**
     * How long to wait before retrying. A retry waits at least this long.
     *
     * @param retryDelaySeconds seconds; finite and non-negative
     */
    record RetryInfo(double retryDelaySeconds) implements ErrorDetail {
        /** The wire type name. */
        public static final String TYPE = "vgi_rpc.RetryInfo";

        /** Refuse a delay the wire cannot carry. */
        public RetryInfo {
            if (!Double.isFinite(retryDelaySeconds) || retryDelaySeconds < 0) {
                throw new IllegalArgumentException("retry_delay_seconds must be finite and non-negative");
            }
        }

        @Override public String type() { return TYPE; }

        @Override
        public Map<String, Object> toJson() {
            Map<String, Object> out = header(TYPE);
            // A whole number travels as an integer so the common case reads the same in every
            // language's JSON encoder ("7", not "7.0").
            out.put("retry_delay_seconds", retryDelaySeconds == Math.rint(retryDelaySeconds)
                    && Math.abs(retryDelaySeconds) < 1e15
                    ? (Object) (long) retryDelaySeconds : (Object) retryDelaySeconds);
            return out;
        }

        static RetryInfo fromJson(Map<?, ?> obj) {
            Object raw = obj.get("retry_delay_seconds");
            if (!(raw instanceof Number n) || raw instanceof Boolean) {
                throw new IllegalArgumentException("'retry_delay_seconds' must be a number");
            }
            return new RetryInfo(n.doubleValue());
        }
    }

    /**
     * One wrong input.
     *
     * @param field the input's name
     * @param description what was wrong with it
     */
    record FieldViolation(String field, String description) {
        /** Normalise nulls to empty strings. */
        public FieldViolation {
            field = field == null ? "" : field;
            description = description == null ? "" : description;
        }
    }

    /**
     * Which inputs were wrong.
     *
     * @param fieldViolations one entry per wrong input
     */
    record BadRequest(List<FieldViolation> fieldViolations) implements ErrorDetail {
        /** The wire type name. */
        public static final String TYPE = "vgi_rpc.BadRequest";

        /** Normalise and freeze. */
        public BadRequest {
            fieldViolations = fieldViolations == null ? List.of() : List.copyOf(fieldViolations);
        }

        @Override public String type() { return TYPE; }

        @Override
        public Map<String, Object> toJson() {
            Map<String, Object> out = header(TYPE);
            List<Object> list = new ArrayList<>();
            for (FieldViolation v : fieldViolations) {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("field", v.field());
                m.put("description", v.description());
                list.add(m);
            }
            out.put("field_violations", list);
            return out;
        }

        static BadRequest fromJson(Map<?, ?> obj) {
            List<FieldViolation> out = new ArrayList<>();
            for (Map<?, ?> v : objects(obj, "field_violations")) {
                out.add(new FieldViolation(str(v, "field"), str(v, "description")));
            }
            return new BadRequest(out);
        }
    }

    /**
     * One unmet precondition.
     *
     * @param type the kind of precondition, e.g. {@code "protocol_version"}
     * @param subject what it concerns
     * @param description how to resolve it
     */
    record PreconditionViolation(String type, String subject, String description) {
        /** Normalise nulls to empty strings. */
        public PreconditionViolation {
            type = type == null ? "" : type;
            subject = subject == null ? "" : subject;
            description = description == null ? "" : description;
        }
    }

    /**
     * What state must change before the call can succeed.
     *
     * @param violations one entry per unmet precondition
     */
    record PreconditionFailure(List<PreconditionViolation> violations) implements ErrorDetail {
        /** The wire type name. */
        public static final String TYPE = "vgi_rpc.PreconditionFailure";

        /** Normalise and freeze. */
        public PreconditionFailure {
            violations = violations == null ? List.of() : List.copyOf(violations);
        }

        @Override public String type() { return TYPE; }

        @Override
        public Map<String, Object> toJson() {
            Map<String, Object> out = header(TYPE);
            List<Object> list = new ArrayList<>();
            for (PreconditionViolation v : violations) {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("type", v.type());
                m.put("subject", v.subject());
                m.put("description", v.description());
                list.add(m);
            }
            out.put("violations", list);
            return out;
        }

        static PreconditionFailure fromJson(Map<?, ?> obj) {
            List<PreconditionViolation> out = new ArrayList<>();
            for (Map<?, ?> v : objects(obj, "violations")) {
                out.add(new PreconditionViolation(str(v, "type"), str(v, "subject"), str(v, "description")));
            }
            return new PreconditionFailure(out);
        }
    }

    /**
     * One exhausted limit.
     *
     * @param subject what the limit applies to
     * @param description which limit, and by how much
     */
    record QuotaViolation(String subject, String description) {
        /** Normalise nulls to empty strings. */
        public QuotaViolation {
            subject = subject == null ? "" : subject;
            description = description == null ? "" : description;
        }
    }

    /**
     * Which limit was hit.
     *
     * @param violations one entry per exhausted limit
     */
    record QuotaFailure(List<QuotaViolation> violations) implements ErrorDetail {
        /** The wire type name. */
        public static final String TYPE = "vgi_rpc.QuotaFailure";

        /** Normalise and freeze. */
        public QuotaFailure {
            violations = violations == null ? List.of() : List.copyOf(violations);
        }

        @Override public String type() { return TYPE; }

        @Override
        public Map<String, Object> toJson() {
            Map<String, Object> out = header(TYPE);
            List<Object> list = new ArrayList<>();
            for (QuotaViolation v : violations) {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("subject", v.subject());
                m.put("description", v.description());
                list.add(m);
            }
            out.put("violations", list);
            return out;
        }

        static QuotaFailure fromJson(Map<?, ?> obj) {
            List<QuotaViolation> out = new ArrayList<>();
            for (Map<?, ?> v : objects(obj, "violations")) {
                out.add(new QuotaViolation(str(v, "subject"), str(v, "description")));
            }
            return new QuotaFailure(out);
        }
    }

    /**
     * Which object the error concerns.
     *
     * @param resourceType the kind of resource, e.g. {@code "report"}
     * @param resourceName its name or identifier
     * @param owner its owner, when meaningful
     * @param description what went wrong with it
     */
    record ResourceInfo(String resourceType, String resourceName, String owner, String description)
            implements ErrorDetail {
        /** The wire type name. */
        public static final String TYPE = "vgi_rpc.ResourceInfo";

        /** Normalise nulls to empty strings. */
        public ResourceInfo {
            resourceType = resourceType == null ? "" : resourceType;
            resourceName = resourceName == null ? "" : resourceName;
            owner = owner == null ? "" : owner;
            description = description == null ? "" : description;
        }

        @Override public String type() { return TYPE; }

        @Override
        public Map<String, Object> toJson() {
            Map<String, Object> out = header(TYPE);
            out.put("resource_type", resourceType);
            out.put("resource_name", resourceName);
            out.put("owner", owner);
            out.put("description", description);
            return out;
        }

        static ResourceInfo fromJson(Map<?, ?> obj) {
            return new ResourceInfo(str(obj, "resource_type"), str(obj, "resource_name"),
                    str(obj, "owner"), str(obj, "description"));
        }
    }

    /**
     * One pointer to documentation.
     *
     * @param description what the link explains
     * @param url where it is
     */
    record HelpLink(String description, String url) {
        /** Normalise nulls to empty strings. */
        public HelpLink {
            description = description == null ? "" : description;
            url = url == null ? "" : url;
        }
    }

    /**
     * Where to read more.
     *
     * @param links documentation pointers
     */
    record Help(List<HelpLink> links) implements ErrorDetail {
        /** The wire type name. */
        public static final String TYPE = "vgi_rpc.Help";

        /** Normalise and freeze. */
        public Help {
            links = links == null ? List.of() : List.copyOf(links);
        }

        @Override public String type() { return TYPE; }

        @Override
        public Map<String, Object> toJson() {
            Map<String, Object> out = header(TYPE);
            List<Object> list = new ArrayList<>();
            for (HelpLink v : links) {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("description", v.description());
                m.put("url", v.url());
                list.add(m);
            }
            out.put("links", list);
            return out;
        }

        static Help fromJson(Map<?, ?> obj) {
            List<HelpLink> out = new ArrayList<>();
            for (Map<?, ?> v : objects(obj, "links")) {
                out.add(new HelpLink(str(v, "description"), str(v, "url")));
            }
            return new Help(out);
        }
    }

    /**
     * Text that is safe to show an end user. The error message stays developer-facing English,
     * as in gRPC; this is the one place user-facing text belongs.
     *
     * @param locale a BCP 47 tag, e.g. {@code "en-US"}
     * @param message the localized text
     */
    record LocalizedMessage(String locale, String message) implements ErrorDetail {
        /** The wire type name. */
        public static final String TYPE = "vgi_rpc.LocalizedMessage";

        /** Normalise nulls to empty strings. */
        public LocalizedMessage {
            locale = locale == null ? "" : locale;
            message = message == null ? "" : message;
        }

        @Override public String type() { return TYPE; }

        @Override
        public Map<String, Object> toJson() {
            Map<String, Object> out = header(TYPE);
            out.put("locale", locale);
            out.put("message", message);
            return out;
        }

        static LocalizedMessage fromJson(Map<?, ?> obj) {
            return new LocalizedMessage(str(obj, "locale"), str(obj, "message"));
        }
    }

    /**
     * Decode one detail object, or {@code null} when it is unknown or malformed.
     *
     * <p>Clients ignore detail types they do not know, and a malformed known type is treated as
     * absent rather than failing the error it rides on: the error is the news, the detail is
     * commentary.
     *
     * @param obj one element of the decoded {@code vgi_rpc.error_details} array
     * @return the typed detail, or {@code null}
     */
    static ErrorDetail parse(Object obj) {
        if (!(obj instanceof Map<?, ?> m)) return null;
        Object type = m.get("@type");
        if (!(type instanceof String t)) return null;
        try {
            return switch (t) {
                case ErrorInfo.TYPE -> ErrorInfo.fromJson(m);
                case RetryInfo.TYPE -> RetryInfo.fromJson(m);
                case BadRequest.TYPE -> BadRequest.fromJson(m);
                case PreconditionFailure.TYPE -> PreconditionFailure.fromJson(m);
                case QuotaFailure.TYPE -> QuotaFailure.fromJson(m);
                case ResourceInfo.TYPE -> ResourceInfo.fromJson(m);
                case Help.TYPE -> Help.fromJson(m);
                case LocalizedMessage.TYPE -> LocalizedMessage.fromJson(m);
                default -> null;
            };
        } catch (RuntimeException malformed) {
            return null;
        }
    }

    /**
     * Whether {@code type} is one of the eight catalog types.
     *
     * @param type an {@code @type} value
     * @return {@code true} for a catalog type
     */
    static boolean inCatalog(String type) {
        return switch (type) {
            case ErrorInfo.TYPE, RetryInfo.TYPE, BadRequest.TYPE, PreconditionFailure.TYPE,
                 QuotaFailure.TYPE, ResourceInfo.TYPE, Help.TYPE, LocalizedMessage.TYPE -> true;
            default -> false;
        };
    }

    private static Map<String, Object> header(String type) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("@type", type);
        return out;
    }

    private static String str(Map<?, ?> obj, String key) {
        Object v = obj.get(key);
        if (v == null) return "";
        if (!(v instanceof String s)) throw new IllegalArgumentException("'" + key + "' must be a string");
        return s;
    }

    private static List<Map<?, ?>> objects(Map<?, ?> obj, String key) {
        Object v = obj.get(key);
        if (v == null) return List.of();
        if (!(v instanceof List<?> list)) throw new IllegalArgumentException("'" + key + "' must be an array");
        List<Map<?, ?>> out = new ArrayList<>();
        for (Object o : list) {
            if (!(o instanceof Map<?, ?> m)) {
                throw new IllegalArgumentException("'" + key + "' must be an array of objects");
            }
            out.add(m);
        }
        return out;
    }
}
