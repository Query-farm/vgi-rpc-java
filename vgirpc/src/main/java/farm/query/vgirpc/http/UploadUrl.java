// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

/**
 * One pre-signed upload slot vended by a worker's {@code __upload_url__}
 * endpoint: where a client PUTs a payload too large to send inline, and the URL
 * it then names in the pointer batch it sends instead.
 *
 * <p>The two URLs are separate because they are not interchangeable — the
 * upload URL usually carries a signature scoped to {@code PUT} and expires
 * quickly, while the download URL is what the <em>worker</em> will fetch. A
 * client that echoes the upload URL back as the location hands the worker a
 * credential it cannot use.</p>
 *
 * <p>The counterpart of {@code farm.query.vgirpc.external.UploadUrlProvider.UploadUrl}
 * on the server side, with the expiry flattened to whole Unix seconds: it
 * crosses the wire as {@code timestamp[us, UTC]}, but sub-second precision on a
 * deadline minutes away is noise, and an integer is what every other port's
 * client surface reports.</p>
 *
 * @param uploadUrl the URL to PUT the payload to
 * @param downloadUrl the URL to name as {@code vgi_rpc.location} afterwards
 * <p>Upload-URL protocol v2: each slot is vended for one announced
 * {@code (size, sha256)}, and {@code uploadHeaders} are the headers the PUT must
 * carry verbatim -- on S3/R2 the signed {@code x-amz-checksum-sha256}, which is
 * how storage itself refuses a body other than the one announced.</p>
 *
 * @param uploadUrl the URL to PUT the payload to
 * @param downloadUrl the URL to name as {@code vgi_rpc.location} afterwards
 * @param expiresAtUnixSeconds when the slot stops being usable, in seconds
 *     since the Unix epoch
 * @param uploadHeaders headers the PUT must carry verbatim; never {@code null}
 * @param objectId the storage id of the object the PUT creates (protocol 3.4 {@code object_id}
 *     column); empty when the server did not send one
 * @param ref the sealed argument reference for that object (protocol 3.4 {@code ref} column);
 *     empty when the server did not send one
 */
public record UploadUrl(String uploadUrl, String downloadUrl, long expiresAtUnixSeconds,
                        java.util.Map<String, String> uploadHeaders, String objectId, byte[] ref) {

    /** Copy the header map, preserving its order. */
    public UploadUrl {
        uploadHeaders = uploadHeaders == null ? java.util.Map.of()
                : java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(uploadHeaders));
        objectId = objectId != null ? objectId : "";
        ref = ref != null ? ref.clone() : new byte[0];
    }

    /**
     * A slot from a server that sends no {@code object_id} / {@code ref}.
     *
     * @param uploadUrl the URL to PUT the payload to
     * @param downloadUrl the URL to name as {@code vgi_rpc.location} afterwards
     * @param expiresAtUnixSeconds expiry, seconds since the epoch
     * @param uploadHeaders headers the PUT must carry verbatim
     */
    public UploadUrl(String uploadUrl, String downloadUrl, long expiresAtUnixSeconds,
                     java.util.Map<String, String> uploadHeaders) {
        this(uploadUrl, downloadUrl, expiresAtUnixSeconds, uploadHeaders, "", null);
    }

    @Override
    public byte[] ref() {
        return ref.clone();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof UploadUrl u && java.util.Objects.equals(uploadUrl, u.uploadUrl)
                && java.util.Objects.equals(downloadUrl, u.downloadUrl)
                && expiresAtUnixSeconds == u.expiresAtUnixSeconds && uploadHeaders.equals(u.uploadHeaders)
                && objectId.equals(u.objectId) && java.util.Arrays.equals(ref, u.ref);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(uploadUrl, downloadUrl, expiresAtUnixSeconds, uploadHeaders, objectId,
                java.util.Arrays.hashCode(ref));
    }

    @Override
    public String toString() {
        // Never print the signed URLs.
        return "UploadUrl[expiresAtUnixSeconds=" + expiresAtUnixSeconds
                + ", uploadHeaders=" + uploadHeaders.keySet() + "]";
    }
}
