// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.errors;

import farm.query.vgirpc.HasErrorKind;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * An application error carrying the full error model.
 *
 * <p>Throw it from a method body to choose the code, the reason and the details a client sees:
 *
 * <pre>{@code
 * throw new StatusError("report is being rebuilt", Code.UNAVAILABLE, "report_rebuilding",
 *         List.of(new ErrorDetail.RetryInfo(30)));
 * }</pre>
 *
 * <p>Any exception class may instead implement {@link HasErrorCode}, {@link HasErrorKind} and
 * {@link HasErrorDetails}; this is the convenience for the case where a dedicated class would add
 * nothing. The details are validated eagerly, so a rule violation fails in the code that made it
 * rather than being silently dropped on the way out.
 */
public class StatusError extends RuntimeException implements HasErrorCode, HasErrorKind, HasErrorDetails {

    private static final long serialVersionUID = 1L;

    private final Code code;
    private final String kind;
    private final transient List<Map<String, Object>> details;

    /**
     * An error with a code and no kind or details.
     *
     * @param message developer-facing text
     * @param code one of the sixteen canonical codes
     */
    public StatusError(String message, Code code) {
        this(message, code, null, List.of());
    }

    /**
     * An error with typed catalog details.
     *
     * @param message developer-facing text
     * @param code one of the sixteen canonical codes
     * @param kind the reason a client branches on, or {@code null} for none
     * @param details catalog details, at most one of each type
     * @throws IllegalArgumentException if the details break a catalog rule
     */
    public StatusError(String message, Code code, String kind, List<? extends ErrorDetail> details) {
        this(code, message, kind, ErrorModel.toJson(details));
    }

    /**
     * An error with details given as JSON objects -- the form a protocol-defined detail type,
     * named under the protocol's own name, has to take.
     *
     * @param code one of the sixteen canonical codes
     * @param message developer-facing text
     * @param kind the reason a client branches on, or {@code null} for none
     * @param details detail objects, each naming its {@code @type}
     * @throws IllegalArgumentException if the details break a catalog rule
     */
    public StatusError(Code code, String message, String kind, List<Map<String, Object>> details) {
        super(message);
        if (code == null) throw new IllegalArgumentException("code is required");
        this.code = code;
        this.kind = kind == null || kind.isEmpty() ? null : kind;
        List<Map<String, Object>> copy = new ArrayList<>();
        if (details != null) for (Map<String, Object> d : details) copy.add(new LinkedHashMap<>(d));
        ErrorModel.validate(copy);
        this.details = List.copyOf(copy);
    }

    @Override public Code errorCode() { return code; }

    @Override public String errorKind() { return kind; }

    @Override public List<Map<String, Object>> errorDetails() { return details; }
}
