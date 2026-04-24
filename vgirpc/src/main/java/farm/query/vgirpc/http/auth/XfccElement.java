// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http.auth;

import java.util.Collections;
import java.util.List;

/**
 * A single element from an Envoy {@code x-forwarded-client-cert} header.
 * {@code cert}, {@code uri}, and {@code by} are URL-decoded when present.
 */
public record XfccElement(
        String hash,
        String cert,
        String subject,
        String uri,
        List<String> dns,
        String by
) {
    public XfccElement {
        dns = dns != null ? List.copyOf(dns) : Collections.emptyList();
    }
}
