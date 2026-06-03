// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.log;

/**
 * Severity of a log {@link Message}, from {@link #EXCEPTION} (reserved for error
 * batches that terminate the affected call) down to {@link #TRACE}. The enum name
 * is the wire token carried in {@code vgi_rpc.log_level}, matching the Python
 * reference; {@link #fromWire(String)} parses it.
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
