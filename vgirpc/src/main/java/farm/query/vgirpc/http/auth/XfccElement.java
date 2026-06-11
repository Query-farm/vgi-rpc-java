// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http.auth;

import java.util.Collections;
import java.util.List;

/**
 * A single element from an Envoy {@code x-forwarded-client-cert} header.
 * {@code cert}, {@code uri}, and {@code by} are URL-decoded when present.
 *
 * @param hash    {@code Hash=} field: hex SHA-256 fingerprint of the client
 *                certificate, or {@code null} if absent.
 * @param cert    {@code Cert=} field: URL-decoded PEM of the client
 *                certificate, or {@code null} if absent.
 * @param subject {@code Subject=} field: the client certificate's subject DN
 *                (quotes stripped), or {@code null} if absent.
 * @param uri     {@code URI=} field: URL-decoded SAN URI (e.g. a SPIFFE ID),
 *                or {@code null} if absent.
 * @param dns     {@code DNS=} fields: SAN DNS names; empty list if none.
 * @param by      {@code By=} field: URL-decoded SAN URI of the proxy that
 *                validated the certificate, or {@code null} if absent.
 */
public record XfccElement(
        String hash,
        String cert,
        String subject,
        String uri,
        List<String> dns,
        String by
) {
    /** Defensively copies {@code dns}, substituting an empty list for {@code null}. */
    public XfccElement {
        dns = dns != null ? List.copyOf(dns) : Collections.emptyList();
    }
}
