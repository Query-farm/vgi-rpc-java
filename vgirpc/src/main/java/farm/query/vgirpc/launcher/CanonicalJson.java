// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.launcher;

import java.util.List;
import java.util.Map;

/**
 * A purpose-built canonical-JSON encoder for exactly the launcher hash payload's
 * shape ({@code {"cmd": [...], "cwd": "...", "env": {...}}}) — not a general JSON
 * library. Byte-identical to Python's {@code json.dumps(payload, sort_keys=True,
 * separators=(",", ":"))}: no whitespace, {@code ,}/{@code :} separators only, and
 * — for string values — Python's exact escaping rules, verified empirically against
 * a live Python interpreter rather than assumed:
 *
 * <ul>
 *   <li>{@code "}, a literal backslash, and the named control escapes for backspace,
 *       form-feed, newline, carriage-return, and tab
 *   <li>every other control character {@code U+0000..U+001F} as a lowercase-hex
 *       {@code &#92;u00xx} escape
 *   <li><b>{@code U+007F} and above</b> as a lowercase-hex {@code &#92;uXXXX} escape —
 *       Python's {@code ensure_ascii=True} default. {@code U+007F} itself is included
 *       in this range (confirmed via {@code python3 -c "json.dumps({'a': chr(0x7f)})"}
 *       returning {@code '&#92;u007f'}), not just strictly-non-ASCII {@code >= 0x80} —
 *       matching the protocol doc's own wording ("bytes &ge; 0x7F").
 * </ul>
 *
 * <p>The one documented cross-language divergence (astral/surrogate-pair encoding
 * for code points above the BMP) is out of scope here: the hash domain is {@code
 * cmd}/{@code cwd}/{@code VGI_RPC_*} env values, which in every realistic case are
 * plain paths and ASCII flag names.
 */
final class CanonicalJson {

    private CanonicalJson() {}

    // Built by concatenating two separate literals rather than writing the two characters
    // adjacently — see appendString's use below for why.
    private static final String BACKSLASH_U = "\\" + "u";

    /** Encodes the exact {@code {"cmd":...,"cwd":...,"env":...}} hash payload; {@code env} must already be
     *  filtered to {@code VGI_RPC_*} keys and iterate in ascending key order (a {@link java.util.SortedMap}). */
    static String encodeHashPayload(List<String> cmd, String cwd, Map<String, String> sortedEnv) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"cmd\":");
        appendStringArray(sb, cmd);
        sb.append(",\"cwd\":");
        appendString(sb, cwd);
        sb.append(",\"env\":{");
        boolean first = true;
        for (Map.Entry<String, String> e : sortedEnv.entrySet()) {
            if (!first) sb.append(',');
            first = false;
            appendString(sb, e.getKey());
            sb.append(':');
            appendString(sb, e.getValue());
        }
        sb.append("}}");
        return sb.toString();
    }

    private static void appendStringArray(StringBuilder sb, List<String> items) {
        sb.append('[');
        for (int i = 0; i < items.size(); i++) {
            if (i > 0) sb.append(',');
            appendString(sb, items.get(i));
        }
        sb.append(']');
    }

    static void appendString(StringBuilder sb, String s) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\b' -> sb.append("\\b");
                case '\f' -> sb.append("\\f");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20 || c >= 0x7F) {
                        // Split across two literals so "\" immediately followed by "u" never
                        // appears as raw source text — javac's unicode-escape preprocessor runs
                        // before tokenization and would otherwise try (and fail) to parse "%04x"
                        // as four hex digits.
                        sb.append(BACKSLASH_U).append(String.format("%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        sb.append('"');
    }
}
