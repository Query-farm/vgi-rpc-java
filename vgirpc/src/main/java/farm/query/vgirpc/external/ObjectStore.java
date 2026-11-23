// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.external;

/**
 * Storage that holds opaque objects by id: the protocol-3.4 storage contract
 * (STATE_REF_V1.md §10.1), {@code put_object} / {@code get(object_id)}.
 *
 * <p>A separate interface next to {@link ExternalStorage} rather than new members of it,
 * so a third-party {@code ExternalStorage} keeps compiling. Every in-tree backend
 * implements both: {@link farm.query.vgirpc.vendor.VendorStorage}, the S3 and GCS
 * storages and the conformance fake storage. "Storage is configured" for
 * {@link farm.query.vgirpc.stateref.StateRef} means the server's storage implements this
 * interface; a storage that does not keeps every ref inline and cannot resolve argument
 * references.</p>
 *
 * <p>Objects carry no TTL and the server never deletes them. <strong>Operators must set a
 * bucket lifecycle rule of at least one day on the {@code rpc} prefix.</strong></p>
 *
 * <p>Implementations must be thread-safe.</p>
 */
public interface ObjectStore {

    /**
     * Store {@code data} as one {@code rpc}-purpose object and return its id.
     *
     * <p>The write carries the size and SHA-256 of {@code data} so storage verifies the
     * body (a vendor sized grant, an S3 PUT signing {@code x-amz-checksum-sha256}).</p>
     *
     * @param data the bytes to store
     * @return the backend's object id (an S3/GCS key, the vendor's {@code object_id}), 1-1024
     *     bytes of UTF-8
     * @throws Exception if the write fails
     */
    String putObject(byte[] data) throws Exception;

    /**
     * Fetch the object {@code objectId} names.
     *
     * <p>The download is minted server-side from the id, pinned to the same hosts as other
     * storage fetches, and capped at {@code maxBytes}. Callers verify size and SHA-256
     * themselves.</p>
     *
     * @param objectId an id returned by {@link #putObject} or carried by an
     *     {@link UploadUrlProvider.UploadUrl}
     * @param maxBytes refuse objects larger than this (the server's {@code max_fetch_bytes})
     * @return the object bytes
     * @throws ObjectMissingException if the object does not exist (never uploaded, deleted,
     *     or expired by lifecycle)
     * @throws Exception on any other (transient) failure
     */
    byte[] get(String objectId, long maxBytes) throws Exception;
}
