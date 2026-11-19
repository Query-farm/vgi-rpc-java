// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.vendor;

import java.util.List;
import java.util.Map;

/**
 * The vendor's {@code GET /v1/info}: storage hosts for this tenant's region.
 *
 * @param region        the tenant's region name
 * @param uploadHosts   hosts upload URLs may point at
 * @param downloadHosts hosts download URLs may point at
 * @param tenant        the tenant the token resolves to
 * @param jurisdiction  the storage jurisdiction ({@code eu}, {@code us}), or {@code null}
 * @param urlPrefixes   URL prefixes this tenant's objects live under
 * @param limits        numeric limits ({@code max_object_bytes}, {@code quota_bytes}, ...)
 * @param ttlSeconds    URL lifetimes ({@code put}, {@code rpc_download}, {@code get})
 */
public record VendorInfo(String region, List<String> uploadHosts, List<String> downloadHosts, String tenant,
                         String jurisdiction, List<String> urlPrefixes, Map<String, Long> limits,
                         Map<String, Long> ttlSeconds) {
    /**
     * Copies the collections.
     *
     * @param region        the tenant's region name
     * @param uploadHosts   hosts upload URLs may point at
     * @param downloadHosts hosts download URLs may point at
     * @param tenant        the tenant
     * @param jurisdiction  the jurisdiction, or {@code null}
     * @param urlPrefixes   URL prefixes
     * @param limits        numeric limits
     * @param ttlSeconds    URL lifetimes
     */
    public VendorInfo {
        uploadHosts = List.copyOf(uploadHosts);
        downloadHosts = List.copyOf(downloadHosts);
        urlPrefixes = List.copyOf(urlPrefixes);
        limits = Map.copyOf(limits);
        ttlSeconds = Map.copyOf(ttlSeconds);
    }
}
