// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.vendor;

/**
 * A blob-vendor or signed-URL request failed.
 *
 * <p>Messages never contain the bearer token or a signed query string: URLs
 * are reduced to scheme, host, port and path before they reach a message.</p>
 */
public final class VendorError extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /** Code for a downloaded blob whose length differs from its reference. */
    public static final String SIZE_MISMATCH = "size_mismatch";
    /** Code for a downloaded blob whose SHA-256 differs from its reference. */
    public static final String SHA256_MISMATCH = "sha256_mismatch";

    private final Integer status;
    private final String code;

    /**
     * Create the error.
     *
     * @param message diagnostic text (already redacted)
     * @param status  HTTP status of the failing response, or {@code null}
     * @param code    machine-readable error code, or {@code null}
     */
    public VendorError(String message, Integer status, String code) {
        super(message);
        this.status = status;
        this.code = code;
    }

    /**
     * Create an error with no status and no code.
     *
     * @param message diagnostic text (already redacted)
     */
    public VendorError(String message) {
        this(message, null, null);
    }

    /**
     * The HTTP status of the failing response.
     *
     * @return the status, or {@code null} for a transport failure or a
     *     malformed response
     */
    public Integer status() {
        return status;
    }

    /**
     * The machine-readable error code: the vendor's {@code error} field (e.g.
     * {@code not_found}, {@code not_uploaded}, {@code quota_exceeded},
     * {@code idempotency_conflict}), the storage's XML {@code <Code>} (e.g.
     * {@code BadDigest}, {@code NoSuchKey}), or {@link #SIZE_MISMATCH} /
     * {@link #SHA256_MISMATCH} for checks made by
     * {@link VendorBlobClient#getBytes(BlobRef)} itself.
     *
     * @return the code, or {@code null} when there was none
     */
    public String code() {
        return code;
    }
}
