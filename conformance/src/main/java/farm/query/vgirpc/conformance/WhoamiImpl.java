// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.conformance;

import farm.query.vgirpc.AuthContext;
import farm.query.vgirpc.CallContext;

import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

/**
 * Reference implementation of {@link Whoami}: the reference's
 * {@code json.dumps(..., sort_keys=True, separators=(",", ":"))}, by hand so the conformance
 * module needs no JSON library.
 */
public final class WhoamiImpl implements Whoami {

    @Override
    public String whoami(CallContext ctx) {
        AuthContext auth = ctx == null ? null : ctx.auth();
        Map<String, Object> out = new TreeMap<>();
        out.put("authenticated", auth != null && auth.authenticated());
        out.put("claims", auth == null || auth.claims() == null ? Map.of() : auth.claims());
        out.put("domain", auth == null || auth.domain() == null ? "" : auth.domain());
        out.put("principal", auth == null || auth.principal() == null ? "" : auth.principal());
        StringBuilder sb = new StringBuilder();
        write(sb, out);
        return sb.toString();
    }

    private static void write(StringBuilder sb, Object v) {
        if (v == null) {
            sb.append("null");
        } else if (v instanceof Boolean || v instanceof Number) {
            sb.append(v);
        } else if (v instanceof Map<?, ?> m) {
            sb.append('{');
            boolean first = true;
            for (Map.Entry<String, Object> e : sortedEntries(m)) {
                if (!first) sb.append(',');
                first = false;
                string(sb, e.getKey());
                sb.append(':');
                write(sb, e.getValue());
            }
            sb.append('}');
        } else if (v instanceof Collection<?> c) {
            sb.append('[');
            boolean first = true;
            for (Object o : c) {
                if (!first) sb.append(',');
                first = false;
                write(sb, o);
            }
            sb.append(']');
        } else {
            string(sb, v.toString());
        }
    }

    private static Iterable<Map.Entry<String, Object>> sortedEntries(Map<?, ?> m) {
        Map<String, Object> sorted = new TreeMap<>();
        for (Map.Entry<?, ?> e : m.entrySet()) sorted.put(String.valueOf(e.getKey()), e.getValue());
        return sorted.entrySet();
    }

    private static void string(StringBuilder sb, String s) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                case '\b' -> sb.append("\\b");
                case '\f' -> sb.append("\\f");
                default -> {
                    if (c < 0x20 || c > 0x7e) sb.append(String.format("\\u%04x", (int) c));
                    else sb.append(c);
                }
            }
        }
        sb.append('"');
    }
}
