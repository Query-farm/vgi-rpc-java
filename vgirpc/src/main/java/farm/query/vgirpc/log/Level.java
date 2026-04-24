// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.log;

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
