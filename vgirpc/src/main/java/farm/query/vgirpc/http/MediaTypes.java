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
}
