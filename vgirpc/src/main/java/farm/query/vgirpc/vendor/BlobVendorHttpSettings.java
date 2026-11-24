// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.vendor;

import farm.query.vgirpc.RpcServer;
import farm.query.vgirpc.external.ExternalLocationConfig;
import farm.query.vgirpc.external.LocationResolver;
import farm.query.vgirpc.external.UploadUrlProvider;
import farm.query.vgirpc.http.HttpServer;

import java.util.Map;

/**
 * HTTP server settings derived from the blob-vendor environment (the Java
 * analogue of vgi-python's {@code BlobVendorHttpConfig}).
 *
 * <p>{@value BlobVendor#ENV_URL} (with {@value BlobVendor#ENV_TOKEN}, falling
 * back to {@value BlobVendor#ENV_TOKEN_FALLBACK}) turns on result
 * externalization ({@value BlobVendor#ENV_THRESHOLD}, default 1 MiB;
 * {@value BlobVendor#ENV_COMPRESSION}) and client upload URLs
 * ({@value BlobVendor#ENV_MAX_UPLOAD_BYTES}). {@value BlobVendor#ENV_MAX_REQUEST_BYTES}
 * is read either way: it is an explicit operator setting, advertised and
 * enforced.</p>
 *
 * @param externalLocation  server external-location config (large results go
 *                          to the vendor; inbound pointers are fetched only
 *                          from its storage hosts), or {@code null} without a vendor
 * @param uploadUrlProvider the vendor, vending upload URLs, or {@code null}
 * @param maxUploadBytes    {@value BlobVendor#ENV_MAX_UPLOAD_BYTES} (advertised only), or {@code null}
 * @param maxRequestBytes   {@value BlobVendor#ENV_MAX_REQUEST_BYTES}, or {@code null}
 */
public record BlobVendorHttpSettings(ExternalLocationConfig externalLocation, UploadUrlProvider uploadUrlProvider,
                                     Long maxUploadBytes, Long maxRequestBytes) {

    /**
     * Read the settings from the environment.
     *
     * @param env environment mapping ({@code null} for the process environment)
     * @return the settings; every field {@code null} when nothing is configured
     * @throws IllegalArgumentException if a variable is malformed, or the vendor
     *     URL is set without a token
     */
    public static BlobVendorHttpSettings fromEnv(Map<String, String> env) {
        Map<String, String> environ = BlobVendor.resolveEnv(env);
        Long maxRequest = BlobVendor.maxRequestBytesFromEnv(environ);
        ExternalLocationConfig external = BlobVendor.externalConfigFromEnv(environ);
        if (external == null) return new BlobVendorHttpSettings(null, null, null, maxRequest);
        return new BlobVendorHttpSettings(external, (VendorStorage) external.storage(),
                BlobVendor.maxUploadBytesFromEnv(environ), maxRequest);
    }

    /**
     * Settings for storage other than the vendor (direct S3 / GCS, or any
     * {@link farm.query.vgirpc.external.ExternalStorage}): results over
     * {@value BlobVendor#ENV_THRESHOLD} (default 1 MiB) go to {@code storage}, compressed per
     * {@value BlobVendor#ENV_COMPRESSION}; inbound pointers are fetched only where
     * {@code urlValidator} allows, up to {@value BlobVendor#ENV_MAX_FETCH_BYTES}; and, when
     * {@code uploadUrlProvider} is not {@code null}, client upload URLs are vended (advertised
     * up to {@value BlobVendor#ENV_MAX_UPLOAD_BYTES}). {@value BlobVendor#ENV_MAX_REQUEST_BYTES}
     * is read as for the vendor.
     *
     * @param storage           where large results (and, for an
     *                          {@link farm.query.vgirpc.external.ObjectStore}, StateRef and
     *                          argument objects) go
     * @param uploadUrlProvider vends client upload URLs, or {@code null} for none (no upload
     *                          grants, no argument references)
     * @param urlValidator      accepts inbound pointer URLs (normally pinned to the storage's
     *                          signing host)
     * @param env               environment mapping ({@code null} for the process environment)
     * @return the settings
     * @throws IllegalArgumentException if a variable is malformed
     */
    public static BlobVendorHttpSettings forStorage(farm.query.vgirpc.external.ExternalStorage storage,
                                                    UploadUrlProvider uploadUrlProvider,
                                                    java.util.function.Consumer<java.net.URI> urlValidator,
                                                    Map<String, String> env) {
        Map<String, String> environ = BlobVendor.resolveEnv(env);
        ExternalLocationConfig.Builder b = ExternalLocationConfig.builder()
                .storage(storage)
                .thresholdBytes(BlobVendor.positiveLongFromEnv(environ, BlobVendor.ENV_THRESHOLD, 1_048_576L))
                .compression(BlobVendor.parseCompression(environ.get(BlobVendor.ENV_COMPRESSION)))
                .urlValidator(urlValidator)
                .stallTimeout(farm.query.vgirpc.external.StorageHttp.stallTimeoutFromEnv(environ));
        Long maxFetch = BlobVendor.positiveLongFromEnv(environ, BlobVendor.ENV_MAX_FETCH_BYTES, null);
        if (maxFetch != null) b.maxFetchBytes(maxFetch);
        return new BlobVendorHttpSettings(b.build(), uploadUrlProvider,
                uploadUrlProvider != null ? BlobVendor.maxUploadBytesFromEnv(environ) : null,
                BlobVendor.maxRequestBytesFromEnv(environ));
    }

    /**
     * Whether a vendor is configured.
     *
     * @return {@code true} when {@link #externalLocation()} is set
     */
    public boolean vendorEnabled() {
        return externalLocation != null;
    }

    /**
     * Apply the HTTP side: the request cap (advertised), the upload-URL
     * provider and the advertised upload size. Leaves the builder unchanged for
     * the settings that are {@code null}.
     *
     * @param builder the HTTP config builder
     * @return {@code builder}
     */
    public HttpServer.Config.Builder applyTo(HttpServer.Config.Builder builder) {
        if (maxRequestBytes != null) {
            builder.maxRequestBytes(maxRequestBytes).advertiseMaxRequestBytes(true);
        }
        if (uploadUrlProvider != null) {
            builder.uploadUrlProvider(uploadUrlProvider);
            if (maxUploadBytes != null) builder.maxUploadBytes(maxUploadBytes);
        }
        return builder;
    }

    /**
     * Apply the RPC side: externalize results through the vendor and resolve
     * inbound pointer batches (client uploads) from its storage hosts. A no-op
     * without a vendor.
     *
     * @param server the RPC server
     * @return {@code server}
     */
    public RpcServer applyTo(RpcServer server) {
        if (externalLocation != null) {
            server.setExternalConfig(externalLocation);
            server.setLocationResolver(new LocationResolver(externalLocation));
        }
        return server;
    }
}
