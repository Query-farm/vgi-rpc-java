// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * One entry of an {@code __upload_url__/init} request (upload-URL protocol v2):
 * the exact length and SHA-256 of a body the client is about to PUT.
 *
 * <p>The worker vends a URL for exactly that body, signed for the length and,
 * where the backend can, the checksum.</p>
 *
 * @param size exact byte length of the body; at least 1
 * @param sha256 SHA-256 of the body, 64 lowercase hex characters
 */
public record UploadRequest(long size, String sha256) {

    /**
     * Validates the entry the way the worker will.
     *
     * @param size exact byte length
     * @param sha256 lowercase hex digest
     */
    public UploadRequest {
        if (size < 1) throw new IllegalArgumentException("upload size must be >= 1");
        if (sha256 == null || !sha256.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException("upload sha256 must be 64 lowercase hex characters");
        }
    }

    /**
     * Describe {@code body}.
     *
     * @param body the bytes that will be PUT
     * @return its size and lowercase hex SHA-256
     */
    public static UploadRequest of(byte[] body) {
        return new UploadRequest(body.length, sha256Hex(body));
    }

    static String sha256Hex(byte[] data) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(data));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is required of every JVM", impossible);
        }
    }
}
