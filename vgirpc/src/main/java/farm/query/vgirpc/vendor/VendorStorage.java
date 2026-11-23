// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.vendor;

import farm.query.vgirpc.external.ExternalStorage;
import farm.query.vgirpc.external.LocationResolver;
import farm.query.vgirpc.external.ObjectMissingException;
import farm.query.vgirpc.external.ObjectStore;
import farm.query.vgirpc.external.UploadUrlProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Map;

/**
 * {@link ExternalStorage} and {@link UploadUrlProvider} backed by the blob vendor.
 *
 * <p>{@link #upload} (server-side externalization of large results) asks the
 * vendor for a sized, checksummed grant ({@code POST /v1/blobs/put}, purpose
 * {@code rpc} by default), PUTs the bytes with exactly the grant's required
 * headers, and returns the grant's download URL. The {@code Externalizer} puts
 * that URL in the pointer batch as {@code vgi_rpc.location}, next to
 * {@code vgi_rpc.location.sha256}. {@link #generateUploadUrl} (client uploads,
 * upload-URL protocol v2) asks the same sized endpoint, always purpose
 * {@code rpc}, for a grant covering the client's announced size and SHA-256,
 * and hands the client the grant's PUT URL, its required headers and its GET
 * URL: storage refuses any other body. That grant request carries the
 * advisory {@code origin: "client_upload"} ({@link #CLIENT_UPLOAD_ORIGIN}),
 * which the vendor ignores and test fakes use to tell client uploads from the
 * server's own writes. The unsized {@code /v1/upload_urls} path is not
 * used.</p>
 *
 * <p>{@link #putObject} / {@link #get(String, long)} are the protocol-3.4 object API
 * (STATE_REF_V1.md §10.1): a sized {@code rpc} grant with the advisory
 * {@code origin: "state_ref"} ({@link #STATE_REF_ORIGIN}), and a download minted
 * server-side from the {@code object_id} ({@code POST /v1/blobs/get}), pinned and capped.</p>
 *
 * <p>Thread-safe. Objects are cleaned up by the vendor (registry alarm plus
 * storage lifecycle rules); vgi-rpc never deletes them.</p>
 */
public final class VendorStorage extends VendorClient implements ExternalStorage, UploadUrlProvider, ObjectStore {

    private static final Logger LOG = LoggerFactory.getLogger(VendorStorage.class);

    private final String purpose;
    private final boolean pinHosts;

    /**
     * The advisory {@code origin} on a {@code /v1/blobs/put} grant minted for a
     * client to upload a request ({@link #generateUploadUrl}). The vendor ignores
     * it; test fakes count and fault client uploads by it. Server result and
     * storage writes never send it. Mirrors vgi-rpc-python
     * {@code vgi_rpc.vendor.CLIENT_UPLOAD_ORIGIN}.
     */
    public static final String CLIENT_UPLOAD_ORIGIN = "client_upload";

    /**
     * The advisory {@code origin} on a {@code /v1/blobs/put} grant for a stored
     * {@code StateRef} object ({@link #putObject}). The vendor ignores it; the fake vendor
     * counts those grants ({@code puts_state_ref}) and their reads. Mirrors vgi-rpc-python
     * {@code vgi_rpc.vendor.STATE_REF_ORIGIN}.
     */
    public static final String STATE_REF_ORIGIN = "state_ref";

    /**
     * Create a vendor-backed storage.
     *
     * @param url            vendor base URL ({@code https}; {@code http} only for loopback)
     * @param token          bearer token
     * @param purpose        object purpose, {@code "rpc"} (gets a download URL and
     *                       the short lifecycle) or {@code "fs"}
     * @param timeout        per-request timeout
     * @param maxAttempts    attempts per request (transport errors, 429 and 5xx retry)
     * @param backoffSeconds base backoff between attempts
     * @param infoTtlSeconds how long the {@code /v1/info} host pin is cached
     * @param pinHosts       check vended URLs against the vendor's hosts before use
     */
    public VendorStorage(String url, String token, String purpose, Duration timeout, int maxAttempts,
                         double backoffSeconds, double infoTtlSeconds, boolean pinHosts) {
        super(url, token, timeout, maxAttempts, backoffSeconds, infoTtlSeconds);
        if (!"rpc".equals(purpose) && !"fs".equals(purpose)) {
            throw new IllegalArgumentException("purpose must be rpc or fs");
        }
        this.purpose = purpose;
        this.pinHosts = pinHosts;
    }

    /**
     * Create a vendor-backed storage with purpose {@code rpc} and the defaults
     * (30 s timeout, 3 attempts, 0.25 s backoff, 600 s host pin, pinning on).
     *
     * @param url   vendor base URL
     * @param token bearer token
     */
    public VendorStorage(String url, String token) {
        this(url, token, "rpc", BlobVendor.DEFAULT_TIMEOUT, 3, 0.25, BlobVendor.DEFAULT_INFO_TTL_SECONDS, true);
    }

    /**
     * Build from {@value BlobVendor#ENV_URL} and the vendor token variables.
     *
     * @param purpose object purpose
     * @param env     environment mapping ({@code null} for the process environment)
     * @return the storage
     * @throws IllegalArgumentException if the URL or token is missing
     */
    public static VendorStorage fromEnv(String purpose, Map<String, String> env) {
        String[] urlToken = BlobVendor.envUrlAndToken(BlobVendor.resolveEnv(env));
        return new VendorStorage(urlToken[0], urlToken[1], purpose, BlobVendor.DEFAULT_TIMEOUT, 3, 0.25,
                BlobVendor.DEFAULT_INFO_TTL_SECONDS, true);
    }

    @Override
    public URI upload(byte[] body, String contentEncoding) {
        PutGrant grant = api.put(body.length, b64Sha256(body), purpose, null, contentEncoding, null, null);
        if (pinHosts) check(grant.uploadUrl());
        api.putObject(grant, body, contentEncoding);
        String downloadUrl = grant.downloadUrl();
        if (downloadUrl == null) downloadUrl = api.get(grant.objectId()).downloadUrl();
        if (pinHosts) check(downloadUrl);
        LOG.debug("Vendored {} bytes as {}", body.length, LocationResolver.redactUrl(downloadUrl));
        return URI.create(downloadUrl);
    }

    @Override
    public UploadUrl generateUploadUrl(long size, String sha256Hex) {
        byte[] digest;
        try {
            digest = HexFormat.of().parseHex(sha256Hex);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("sha256 must be 64 lowercase hex characters");
        }
        if (digest.length != 32) throw new IllegalArgumentException("sha256 must be 64 lowercase hex characters");
        // Always rpc: a client upload is a request payload the server fetches
        // once, which is what rpc objects (a GET URL, the short lifecycle) are.
        PutGrant grant = api.put(size, Base64.getEncoder().encodeToString(digest), "rpc", null, null, null,
                CLIENT_UPLOAD_ORIGIN);
        String downloadUrl = grant.downloadUrl();
        if (downloadUrl == null) downloadUrl = api.get(grant.objectId()).downloadUrl();
        if (pinHosts) {
            check(grant.uploadUrl());
            check(downloadUrl);
        }
        Instant expires = grant.expiresAt();
        if (grant.downloadExpiresAt() != null && (expires == null || grant.downloadExpiresAt().isBefore(expires))) {
            expires = grant.downloadExpiresAt();
        }
        return new UploadUrl(grant.uploadUrl(), downloadUrl, expires, grant.requiredHeaders(), grant.objectId());
    }

    /**
     * Store {@code data} as one {@code rpc} object and return its {@code object_id}.
     *
     * <p>A sized, checksummed grant ({@code /v1/blobs/put}, purpose {@code rpc}, advisory
     * {@code origin: "state_ref"}) and a PUT with exactly its required headers, so storage
     * verifies the body. Objects carry no TTL; the bucket lifecycle removes them.</p>
     *
     * @param data the bytes to store (a sealed {@code StateRef} object)
     * @return the vendor's {@code object_id}
     * @throws VendorError if the vendor or the signed upload fails
     */
    @Override
    public String putObject(byte[] data) {
        PutGrant grant = api.put(data.length, b64Sha256(data), "rpc", null, null, null, STATE_REF_ORIGIN);
        if (pinHosts) check(grant.uploadUrl());
        api.putObject(grant, data, null);
        return grant.objectId();
    }

    /**
     * Fetch an object by id: the download is minted server-side ({@code /v1/blobs/get}),
     * pinned to the vendor's download hosts and capped at {@code maxBytes}. The caller verifies
     * size and digest.
     *
     * @param objectId a vendor {@code object_id}
     * @param maxBytes the largest object accepted
     * @return the object bytes
     * @throws ObjectMissingException the vendor reports {@code not_found} / {@code not_uploaded},
     *     or storage answers 404
     * @throws VendorError any other vendor or download failure
     */
    @Override
    public byte[] get(String objectId, long maxBytes) throws ObjectMissingException {
        BlobLocation location;
        try {
            location = api.get(objectId);
        } catch (VendorError e) {
            if (isMissing(e)) {
                throw new ObjectMissingException("blob vendor object is gone ("
                        + (e.code() != null ? e.code() : e.status()) + ")");
            }
            throw e;
        }
        if (location.size() > maxBytes) {
            throw new VendorError("Blob vendor object is " + location.size() + " bytes, over max_fetch_bytes ("
                    + maxBytes + ")");
        }
        String encoding = location.contentEncoding();
        if (encoding != null && !"identity".equals(encoding)) {
            throw new VendorError("Blob vendor object has unsupported content_encoding '" + encoding + "'");
        }
        try {
            return api.fetch(location.downloadUrl(), uri -> {
                if (pinHosts) urlValidator().accept(uri);
            }, maxBytes, "object");
        } catch (VendorError e) {
            if (e.status() != null && e.status() == 404) {
                throw new ObjectMissingException("blob vendor object is gone (storage 404)");
            }
            throw e;
        }
    }

    private static boolean isMissing(VendorError e) {
        return "not_found".equals(e.code()) || "not_uploaded".equals(e.code())
                || (e.status() != null && e.status() == 404);
    }

    static String b64Sha256(byte[] data) {
        return Base64.getEncoder().encodeToString(sha256(data));
    }

    static byte[] sha256(byte[] data) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(data);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }
}
