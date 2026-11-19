// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.vendor;

import java.time.Instant;

/**
 * The vendor's answer to {@code get}: where one object can be downloaded.
 *
 * @param downloadUrl     pre-signed GET URL
 * @param size            declared byte length
 * @param sha256B64       declared SHA-256, standard base64
 * @param contentEncoding {@code Content-Encoding} stored with the object, or {@code null}
 * @param expiresAt       when {@code downloadUrl} stops working
 */
public record BlobLocation(String downloadUrl, long size, String sha256B64, String contentEncoding,
                           Instant expiresAt) {
    @Override
    public String toString() {
        // Never print the signed URL.
        return "BlobLocation[size=" + size + ", contentEncoding=" + contentEncoding
                + ", expiresAt=" + expiresAt + "]";
    }
}
