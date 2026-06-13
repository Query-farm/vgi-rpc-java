// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

/**
 * Thrown when a request's {@code Content-Encoding} names a codec this server
 * cannot (or will not) decode — an unknown codec, or one that is disabled (e.g.
 * zstd when {@code VGI_HTTP_DISABLE_ZSTD} is set). Maps to HTTP 415, with a
 * {@code VGI-Supported-Encodings} header listing the codecs the client should
 * use instead; the client refreshes its codec choice and retries.
 */
public final class UnsupportedContentEncodingException extends RuntimeException {
    private final String supportedEncodings;

    /**
     * @param message            diagnostic message
     * @param supportedEncodings comma-separated codec tokens the server accepts (for {@code VGI-Supported-Encodings})
     */
    public UnsupportedContentEncodingException(String message, String supportedEncodings) {
        super(message);
        this.supportedEncodings = supportedEncodings;
    }

    /** @return the comma-separated codec tokens the server accepts */
    public String supportedEncodings() { return supportedEncodings; }
}
