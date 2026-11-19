// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.vendor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

/**
 * Raw byte blobs through the vendor, referenced by {@link BlobRef}.
 *
 * <p>For callers that store references themselves (e.g. a key-value store
 * that externalizes large values). Objects use purpose {@code fs} by default:
 * no download URL at {@code put}; {@link #getBytes} asks the vendor for a
 * fresh one, downloads it on a bare client pinned to the vendor's storage
 * hosts, and verifies the bytes against the reference's size and SHA-256.</p>
 *
 * <p>Thread-safe.</p>
 */
public final class VendorBlobClient extends VendorClient {

    /** Default cap on a blob {@link #getBytes} will download (the vendor's per-object cap). */
    public static final long DEFAULT_MAX_FETCH_BYTES = 4L * 1024 * 1024 * 1024;

    private final String purpose;
    private final long maxFetchBytes;

    /**
     * Create a blob client.
     *
     * @param url            vendor base URL ({@code https}; {@code http} only for loopback)
     * @param token          bearer token
     * @param purpose        object purpose (normally {@code "fs"})
     * @param timeout        per-request timeout
     * @param maxAttempts    attempts per request
     * @param backoffSeconds base backoff between attempts
     * @param infoTtlSeconds how long the {@code /v1/info} host pin is cached
     * @param maxFetchBytes  largest blob {@link #getBytes} will fetch
     */
    public VendorBlobClient(String url, String token, String purpose, Duration timeout, int maxAttempts,
                            double backoffSeconds, double infoTtlSeconds, long maxFetchBytes) {
        super(url, token, timeout, maxAttempts, backoffSeconds, infoTtlSeconds);
        if (!"rpc".equals(purpose) && !"fs".equals(purpose)) {
            throw new IllegalArgumentException("purpose must be rpc or fs");
        }
        this.purpose = purpose;
        // A Java array holds at most Integer.MAX_VALUE - 8 bytes.
        this.maxFetchBytes = Math.min(maxFetchBytes, Integer.MAX_VALUE - 8L);
    }

    /**
     * Create a blob client with purpose {@code fs} and the defaults.
     *
     * @param url   vendor base URL
     * @param token bearer token
     */
    public VendorBlobClient(String url, String token) {
        this(url, token, "fs", BlobVendor.DEFAULT_TIMEOUT, 3, 0.25, BlobVendor.DEFAULT_INFO_TTL_SECONDS,
                DEFAULT_MAX_FETCH_BYTES);
    }

    /**
     * Build from {@value BlobVendor#ENV_URL} and the vendor token variables.
     *
     * @param purpose object purpose
     * @param env     environment mapping ({@code null} for the process environment)
     * @return the client
     * @throws IllegalArgumentException if the URL or token is missing
     */
    public static VendorBlobClient fromEnv(String purpose, Map<String, String> env) {
        String[] urlToken = BlobVendor.envUrlAndToken(BlobVendor.resolveEnv(env));
        return new VendorBlobClient(urlToken[0], urlToken[1], purpose, BlobVendor.DEFAULT_TIMEOUT, 3, 0.25,
                BlobVendor.DEFAULT_INFO_TTL_SECONDS, DEFAULT_MAX_FETCH_BYTES);
    }

    /**
     * Store {@code data} and return its reference.
     *
     * @param data           the bytes to store
     * @param scope          the execution the blob belongs to, for {@link #deleteScope}; may be {@code null}
     * @param idempotencyKey makes a retried logical write reuse the same object;
     *                       may be {@code null}. The same key with other bytes is
     *                       refused ({@link VendorError#code()} {@code idempotency_conflict}).
     * @return a reference naming the object, its size and SHA-256
     * @throws VendorError if the vendor or the signed upload fails
     */
    public BlobRef putBytes(byte[] data, BlobScope scope, String idempotencyKey) {
        byte[] digest = VendorStorage.sha256(data);
        PutGrant grant = api.put(data.length, Base64.getEncoder().encodeToString(digest), purpose, scope, null,
                idempotencyKey);
        check(grant.uploadUrl());
        api.putObject(grant, data, null);
        return new BlobRef(grant.objectId(), data.length, HexFormat.of().formatHex(digest));
    }

    /**
     * Fetch and verify the blob {@code ref} names.
     *
     * <p>The download is pinned to the vendor's storage hosts, capped at the
     * client's maximum fetch size, and checked against the size and SHA-256 in
     * {@code ref} (not the vendor's own report).</p>
     *
     * @param ref the reference from {@link #putBytes}
     * @return the blob bytes
     * @throws VendorError if the vendor fails, the object is missing (the
     *     vendor's {@code not_found}/{@code not_uploaded} or a storage 404), the
     *     download fails, or the bytes do not match {@code ref}
     *     ({@link VendorError#SIZE_MISMATCH} / {@link VendorError#SHA256_MISMATCH})
     */
    public byte[] getBytes(BlobRef ref) {
        if (ref.size() > maxFetchBytes) {
            throw new VendorError("Blob " + ref.objectId() + " is " + ref.size() + " bytes, over max_fetch_bytes ("
                    + maxFetchBytes + ")");
        }
        BlobLocation location = api.get(ref.objectId());
        if (location.contentEncoding() != null && !"identity".equals(location.contentEncoding())) {
            throw new VendorError("Blob " + ref.objectId() + " has unsupported content_encoding '"
                    + location.contentEncoding() + "'");
        }
        if (location.size() != ref.size()) {
            throw new VendorError("Blob " + ref.objectId() + " size mismatch: vendor reports " + location.size()
                    + ", ref has " + ref.size(), null, VendorError.SIZE_MISMATCH);
        }
        byte[] data = api.fetch(location.downloadUrl(), validator, maxFetchBytes, ref.objectId());
        if (data.length != ref.size()) {
            throw new VendorError("Blob " + ref.objectId() + " size mismatch: expected " + ref.size() + ", got "
                    + data.length, null, VendorError.SIZE_MISMATCH);
        }
        String actual = HexFormat.of().formatHex(VendorStorage.sha256(data));
        if (!actual.equalsIgnoreCase(ref.sha256())) {
            throw new VendorError("Blob " + ref.objectId() + " SHA-256 mismatch: expected " + ref.sha256()
                    + ", got " + actual, null, VendorError.SHA256_MISMATCH);
        }
        return data;
    }

    /**
     * Delete objects by id (missing objects are ignored), in requests of at
     * most 1,000 ids.
     *
     * @param objectIds the object ids
     * @return how many objects the vendor deleted
     * @throws VendorError if the vendor fails
     */
    public long delete(List<String> objectIds) {
        long deleted = 0;
        List<String> ids = new ArrayList<>(objectIds);
        for (int start = 0; start < ids.size(); start += VendorApi.MAX_DELETE_IDS) {
            deleted += api.delete(ids.subList(start, Math.min(ids.size(), start + VendorApi.MAX_DELETE_IDS)));
        }
        return deleted;
    }

    /**
     * Delete objects by reference (missing objects are ignored).
     *
     * @param refs the references
     * @return how many objects the vendor deleted
     * @throws VendorError if the vendor fails
     */
    public long deleteRefs(List<BlobRef> refs) {
        return delete(refs.stream().map(BlobRef::objectId).toList());
    }

    /**
     * Delete every object put under {@code (shard, executionId)}.
     *
     * @param shard       the storage shard key
     * @param executionId the execution id, encoded as in {@link BlobScope}
     * @return how many objects the vendor deleted
     * @throws VendorError if the vendor fails
     */
    public long deleteScope(String shard, String executionId) {
        return api.deleteScope(new BlobScope(shard, executionId));
    }
}
