// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.vendor;

import farm.query.vgirpc.external.ExternalStorage;
import farm.query.vgirpc.external.LocationResolver;
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
 * URL: storage refuses any other body. The unsized {@code /v1/upload_urls}
 * path is not used.</p>
 *
 * <p>Thread-safe. Objects are cleaned up by the vendor (registry alarm plus
 * storage lifecycle rules); vgi-rpc never deletes them.</p>
 */
public final class VendorStorage extends VendorClient implements ExternalStorage, UploadUrlProvider {

    private static final Logger LOG = LoggerFactory.getLogger(VendorStorage.class);

    private final String purpose;
    private final boolean pinHosts;

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
        PutGrant grant = api.put(body.length, b64Sha256(body), purpose, null, contentEncoding, null);
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
        PutGrant grant = api.put(size, Base64.getEncoder().encodeToString(digest), "rpc", null, null, null);
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
        return new UploadUrl(grant.uploadUrl(), downloadUrl, expires, grant.requiredHeaders());
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
