// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

/** Media-type / content-encoding tokens used on the HTTP transport. */
public final class MediaTypes {

    private MediaTypes() {}

    public static final String APPLICATION_JSON = "application/json";
    public static final String APPLICATION_OCTET_STREAM = "application/octet-stream";

    /** zstd Content-Encoding token (response body compression negotiation). */
    public static final String ZSTD = "zstd";

    /** gzip Content-Encoding token (fallback when zstd is unavailable/disabled). */
    public static final String GZIP = "gzip";

    /** No compression. Every server can produce it, so a client that names it
     *  ahead of the compressed codecs in an accept header is explicitly asking
     *  for an uncompressed response — useful for benchmarking and for
     *  deployments that want compression off per request. An identity body
     *  carries no content-encoding header at all. */
    public static final String IDENTITY = "identity";
}
