// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.errors;

/**
 * The closed set of canonical error codes: gRPC's sixteen, minus {@code OK}.
 *
 * <p>The wire value is the constant's <em>name</em> -- {@code "UNAVAILABLE"}, never {@code 14} --
 * so a log line, a proxy rule and a client switch all read the same string. A client that
 * receives any other value treats it as {@link #UNKNOWN} ({@link #parse}).
 */
public enum Code {
    /** The operation was cancelled, typically by the caller. */
    CANCELLED,
    /** Unclassified. What an error with no declared code is sent as. */
    UNKNOWN,
    /** The caller supplied an invalid argument. */
    INVALID_ARGUMENT,
    /** The deadline expired before the operation could complete. */
    DEADLINE_EXCEEDED,
    /** A requested entity was not found. */
    NOT_FOUND,
    /** The entity the caller tried to create already exists. */
    ALREADY_EXISTS,
    /** The caller may not perform this operation. */
    PERMISSION_DENIED,
    /** A resource or quota is exhausted; retryable only with {@code RetryInfo}. */
    RESOURCE_EXHAUSTED,
    /** The system is not in a state required for the operation. */
    FAILED_PRECONDITION,
    /** Aborted; retry the whole operation at a higher level, not this call. */
    ABORTED,
    /** An argument was past the valid range. */
    OUT_OF_RANGE,
    /** The operation is not implemented, or the protocol is not hosted. */
    UNIMPLEMENTED,
    /** An invariant of the server's own machinery was broken. */
    INTERNAL,
    /** Transient: the service is unavailable. Retryable. */
    UNAVAILABLE,
    /** Unrecoverable data loss or corruption. */
    DATA_LOSS,
    /** The caller has no valid credentials. */
    UNAUTHENTICATED;

    /**
     * Read a wire value, mapping anything unrecognised -- including {@code null} and
     * {@code ""} -- to {@link #UNKNOWN}.
     *
     * @param value the {@code vgi_rpc.error_code} value, or {@code null} when absent
     * @return the matching constant, or {@link #UNKNOWN}
     */
    public static Code parse(String value) {
        if (value == null) return UNKNOWN;
        for (Code c : values()) {
            if (c.name().equals(value)) return c;
        }
        return UNKNOWN;
    }

    /**
     * Whether {@code value} names one of the sixteen codes exactly.
     *
     * @param value a candidate wire value
     * @return {@code true} for a canonical code name
     */
    public static boolean isCanonical(String value) {
        if (value == null) return false;
        for (Code c : values()) {
            if (c.name().equals(value)) return true;
        }
        return false;
    }
}
