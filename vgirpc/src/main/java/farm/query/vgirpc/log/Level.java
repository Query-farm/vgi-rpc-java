// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.log;

/**
 * Severity of a log {@link Message}, from {@link #EXCEPTION} (an error batch
 * carrying a failure) down to {@link #TRACE}. The names match the wire tokens
 * used by the Python reference; {@link #fromWire(String)} parses them.
 */
public enum Level {
    EXCEPTION,
    ERROR,
    WARN,
    INFO,
    DEBUG,
    TRACE;

    public static Level fromWire(String s) {
        return Level.valueOf(s);
    }
}
