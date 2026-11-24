// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.vendor;

import java.net.URI;
import java.time.Duration;
import java.util.function.Consumer;

/**
 * Lifecycle and host pinning shared by {@link VendorStorage} and
 * {@link VendorBlobClient}. Thread-safe.
 */
public abstract sealed class VendorClient implements AutoCloseable permits VendorStorage, VendorBlobClient {

    final VendorApi api;
    final VendorHostsValidator validator;

    VendorClient(String url, String token, Duration timeout, Duration stallTimeout, int maxAttempts,
                 double backoffSeconds, double infoTtlSeconds) {
        this.api = new VendorApi(url, token, timeout, stallTimeout, maxAttempts, backoffSeconds);
        this.validator = new VendorHostsValidator(api, infoTtlSeconds);
    }

    /**
     * A {@code urlValidator} pinned to the vendor's storage hosts
     * ({@code /v1/info}, cached).
     *
     * @return the validator; it throws {@link IllegalArgumentException} for a
     *     URL off the vendor's hosts
     */
    public Consumer<URI> urlValidator() {
        return validator;
    }

    /**
     * Fetch the vendor's region and storage hosts (uncached).
     *
     * @return the vendor's {@link VendorInfo}
     * @throws VendorError if the vendor fails
     */
    public VendorInfo info() {
        return api.info();
    }

    /** Release the HTTP clients. */
    @Override
    public void close() {
        api.close();
    }

    void check(String url) {
        try {
            validator.accept(URI.create(url));
        } catch (IllegalArgumentException e) {
            throw new VendorError("Blob vendor returned a URL off its pinned hosts: " + e.getMessage());
        }
    }
}
