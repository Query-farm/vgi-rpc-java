// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.transport;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Bounded HTTP response returned by an {@code iroh-http/2} provider. Ownership
 * of the body buffer transfers to the synchronous caller; consumers must treat
 * it as read-only. Avoiding defensive copies matters at VGI's negotiated
 * hundreds-of-megabytes response sizes.
 */
public record IrohHttpResponse(int status, Map<String, List<String>> headers, byte[] body) {
    public IrohHttpResponse {
        if (status < 100 || status > 999) {
            throw new IrohConfigurationException("invalid Iroh HTTP response status");
        }
        Map<String, List<String>> copied = new LinkedHashMap<>();
        if (headers != null) headers.forEach((name, values) -> copied.put(
                name.toLowerCase(Locale.ROOT), values == null ? List.of() : List.copyOf(values)));
        headers = Collections.unmodifiableMap(copied);
        body = body == null ? new byte[0] : body;
    }

    /** All values for a case-insensitive HTTP field name. */
    public List<String> headerValues(String name) {
        return headers.getOrDefault(name.toLowerCase(Locale.ROOT), List.of());
    }

    /** First value for a case-insensitive HTTP field name, or an empty string. */
    public String firstHeader(String name) {
        List<String> values = headerValues(name);
        return values.isEmpty() ? "" : values.getFirst();
    }
}
