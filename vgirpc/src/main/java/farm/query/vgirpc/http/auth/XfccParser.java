// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http.auth;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parse Envoy-style {@code x-forwarded-client-cert} headers.
 *
 * <p>The header has {@code ,}-separated elements; each element has
 * {@code ;}-separated {@code key=value} pairs with values possibly
 * double-quoted. URL-encoded values are decoded for {@code Cert}, {@code URI},
 * and {@code By}. Multiple {@code DNS=} entries are collected into a list.</p>
 */
public final class XfccParser {

    private static final Set<String> URL_DECODE_KEYS = Set.of("cert", "uri", "by");
    private static final Pattern ESCAPE_PATTERN = Pattern.compile("\\\\(.)");

    private XfccParser() {}

    public static List<XfccElement> parse(String headerValue) {
        List<XfccElement> out = new ArrayList<>();
        if (headerValue == null) return out;
        for (String rawElem : splitRespectingQuotes(headerValue, ',')) {
            rawElem = rawElem.trim();
            if (rawElem.isEmpty()) continue;
            out.add(parseElement(rawElem));
        }
        return out;
    }

    private static XfccElement parseElement(String element) {
        String hash = null, cert = null, subject = null, uri = null, by = null;
        List<String> dns = new ArrayList<>();
        for (String pair : splitRespectingQuotes(element, ';')) {
            pair = pair.trim();
            if (pair.isEmpty()) continue;
            int eq = pair.indexOf('=');
            if (eq < 0) continue;
            String key = pair.substring(0, eq).trim().toLowerCase();
            String value = pair.substring(eq + 1).trim();
            if (value.length() >= 2 && value.charAt(0) == '"' && value.charAt(value.length() - 1) == '"') {
                value = unescapeQuoted(value.substring(1, value.length() - 1));
            }
            if (URL_DECODE_KEYS.contains(key)) {
                value = URLDecoder.decode(value, StandardCharsets.UTF_8);
            }
            switch (key) {
                case "hash" -> hash = value;
                case "cert" -> cert = value;
                case "subject" -> subject = value;
                case "uri" -> uri = value;
                case "dns" -> dns.add(value);
                case "by" -> by = value;
                default -> { /* ignore unknown keys */ }
            }
        }
        return new XfccElement(hash, cert, subject, uri, dns, by);
    }

    /** Split {@code text} on {@code delimiter}, leaving double-quoted runs intact. */
    static List<String> splitRespectingQuotes(String text, char delimiter) {
        List<String> parts = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        int i = 0;
        int n = text.length();
        while (i < n) {
            char ch = text.charAt(i);
            if (ch == '"') {
                inQuotes = !inQuotes;
                current.append(ch);
            } else if (ch == '\\' && inQuotes && i + 1 < n) {
                current.append(ch).append(text.charAt(i + 1));
                i += 1;
            } else if (ch == delimiter && !inQuotes) {
                parts.add(current.toString());
                current = new StringBuilder();
            } else {
                current.append(ch);
            }
            i += 1;
        }
        parts.add(current.toString());
        return parts;
    }

    private static String unescapeQuoted(String s) {
        Matcher m = ESCAPE_PATTERN.matcher(s);
        return m.replaceAll("$1");
    }

    /** Extract the CN component from an RFC-4514 DN (or similar). */
    public static String extractCn(String subject) {
        if (subject == null || subject.isEmpty()) return "";
        for (String part : subject.split("(?<!\\\\),")) {
            String p = part.trim();
            if (p.length() >= 3 && p.substring(0, 3).equalsIgnoreCase("CN=")) {
                return p.substring(3);
            }
        }
        return "";
    }
}
