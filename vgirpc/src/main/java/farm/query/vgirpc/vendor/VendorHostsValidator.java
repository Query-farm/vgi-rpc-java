// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.vendor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * A {@code urlValidator} backed by the vendor's {@code /v1/info}, cached for a
 * TTL. A failed refresh keeps the last known-good pin.
 */
final class VendorHostsValidator implements Consumer<URI> {

    private static final Logger LOG = LoggerFactory.getLogger(VendorHostsValidator.class);

    private final VendorApi api;
    private final long ttlNanos;
    private Consumer<URI> validator;
    private long fetchedAt;

    VendorHostsValidator(VendorApi api, double ttlSeconds) {
        this.api = api;
        this.ttlNanos = (long) (ttlSeconds * 1e9);
    }

    private synchronized Consumer<URI> current() {
        long now = System.nanoTime();
        if (validator != null && now - fetchedAt < ttlNanos) return validator;
        Consumer<URI> fresh;
        try {
            VendorInfo info = api.info();
            List<String> hosts = new ArrayList<>(info.uploadHosts());
            hosts.addAll(info.downloadHosts());
            fresh = BlobVendor.pinnedHostsValidator(hosts, api.loopbackHttp, info.urlPrefixes());
        } catch (VendorError | IllegalArgumentException e) {
            if (validator != null) {
                LOG.warn("Blob vendor info refresh failed; keeping cached hosts: {}", e.getMessage());
                fetchedAt = now;
                return validator;
            }
            throw new IllegalArgumentException("Cannot pin storage hosts: " + e.getMessage());
        }
        validator = fresh;
        fetchedAt = now;
        return fresh;
    }

    @Override
    public void accept(URI uri) {
        current().accept(uri);
    }
}
