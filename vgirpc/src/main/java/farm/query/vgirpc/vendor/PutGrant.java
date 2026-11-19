// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.vendor;

import java.time.Instant;
import java.util.Map;

/**
 * The vendor's answer to {@code put}: where and how to upload one object.
 *
 * @param objectId          the server-chosen object id
 * @param uploadUrl         pre-signed PUT URL
 * @param requiredHeaders   headers the PUT must carry exactly as given (the
 *                          signature covers them)
 * @param downloadUrl       pre-signed GET URL, or {@code null} (only {@code rpc}
 *                          objects get one)
 * @param expiresAt         when {@code uploadUrl} stops working
 * @param region            the region the object lives in, or {@code null}
 * @param downloadExpiresAt when {@code downloadUrl} stops working, or {@code null}
 */
public record PutGrant(String objectId, String uploadUrl, Map<String, String> requiredHeaders,
                       String downloadUrl, Instant expiresAt, String region, Instant downloadExpiresAt) {
    /**
     * Copies the header map.
     *
     * @param objectId          the server-chosen object id
     * @param uploadUrl         pre-signed PUT URL
     * @param requiredHeaders   headers the PUT must carry
     * @param downloadUrl       pre-signed GET URL, or {@code null}
     * @param expiresAt         when {@code uploadUrl} stops working
     * @param region            the region, or {@code null}
     * @param downloadExpiresAt when {@code downloadUrl} stops working, or {@code null}
     */
    public PutGrant {
        requiredHeaders = Map.copyOf(requiredHeaders);
    }

    @Override
    public String toString() {
        // Never print the signed URLs.
        return "PutGrant[objectId=" + objectId + ", region=" + region + ", expiresAt=" + expiresAt + "]";
    }
}
