// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.log;

/**
 * Severity of a log {@link Message}. {@code EXCEPTION} is reserved for error
 * batches; the rest map to ordinary log batches. The enum name is the wire
 * value carried in {@code vgi_rpc.log_level}.
 */
public enum Level {
    /** An error/exception batch (terminates the affected call). */
    EXCEPTION,
    /** Error-level log. */
    ERROR,
    /** Warning-level log. */
    WARN,
    /** Informational log. */
    INFO,
    /** Debug-level log. */
    DEBUG,
    /** Trace-level log. */
    TRACE;

    /**
     * Parse a wire log-level string.
     *
     * @param s the level name as carried in {@code vgi_rpc.log_level}
     * @return the matching {@link Level}
     * @throws IllegalArgumentException if {@code s} is not a known level
     */
    public static Level fromWire(String s) {
        return Level.valueOf(s);
    }
}
