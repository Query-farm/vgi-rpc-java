// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.transport;

import java.util.HexFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Canonical {@code iroh://} or {@code httpi://} VGI endpoint. */
public record IrohEndpoint(Scheme scheme, String endpointId, byte[] endpointIdBytes,
                           String basePath, String alpn) {
    public static final String ARROW_MUX_ALPN = "vgi-rpc/arrow-mux/1";
    public static final String HTTP_ALPN = "iroh-http/2";
    private static final Pattern URI = Pattern.compile("^(iroh|httpi)://([0-9a-f]{64})(/.*)?$");

    public enum Scheme { IROH, HTTPI }

    public IrohEndpoint {
        endpointIdBytes = endpointIdBytes.clone();
    }

    @Override public byte[] endpointIdBytes() { return endpointIdBytes.clone(); }

    /** Parse without {@link java.net.URI} hostname case or path normalization. */
    public static IrohEndpoint parse(String raw) {
        if (raw == null || raw.isEmpty() || raw.indexOf('\\') >= 0 || raw.indexOf('?') >= 0
                || raw.indexOf('#') >= 0 || raw.chars().anyMatch(c -> c <= 0x20 || c == 0x7f)) {
            throw new IrohUriException("invalid VGI Iroh endpoint URI");
        }
        Matcher match = URI.matcher(raw);
        if (!match.matches()) {
            throw new IrohUriException(
                    "Iroh endpoint ID must be exactly 64 lowercase hexadecimal characters");
        }
        Scheme scheme = match.group(1).equals("iroh") ? Scheme.IROH : Scheme.HTTPI;
        String path = match.group(3) == null ? "" : match.group(3);
        if (scheme == Scheme.IROH && !path.isEmpty()) {
            throw new IrohUriException("iroh:// endpoints cannot contain a path");
        }
        if (path.length() > 1 && path.endsWith("/")) {
            throw new IrohUriException("httpi:// base paths cannot have a trailing empty segment");
        }
        if (path.contains("//")) {
            throw new IrohUriException("httpi:// base paths cannot contain empty segments");
        }
        for (String segment : path.split("/", -1)) {
            if (segment.equals(".") || segment.equals("..")) {
                throw new IrohUriException("httpi:// base paths cannot contain dot segments");
            }
        }
        for (int i = 0; i < path.length(); i++) {
            if (path.charAt(i) == '%' && (i + 2 >= path.length()
                    || Character.digit(path.charAt(i + 1), 16) < 0
                    || Character.digit(path.charAt(i + 2), 16) < 0)) {
                throw new IrohUriException("httpi:// base path contains an invalid percent escape");
            }
            if (path.charAt(i) == '%') {
                int decoded = Integer.parseInt(path.substring(i + 1, i + 3), 16);
                if (decoded == '.' || decoded == '/' || decoded == '\\'
                        || decoded <= 0x20 || decoded == 0x7f) {
                    throw new IrohUriException(
                            "httpi:// base path contains an encoded dot, separator, or control");
                }
                i += 2;
            }
        }
        String basePath = path.equals("/") ? "" : path;
        String id = match.group(2);
        return new IrohEndpoint(scheme, id, HexFormat.of().parseHex(id), basePath,
                scheme == Scheme.IROH ? ARROW_MUX_ALPN : HTTP_ALPN);
    }
}
