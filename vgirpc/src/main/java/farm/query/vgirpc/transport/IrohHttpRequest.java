// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.transport;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * One complete HTTP/1.1 request carried on an {@code iroh-http/2} stream.
 * The body buffer is borrowed for the synchronous {@link IrohHttpTransport#execute}
 * call and must not be mutated or retained by a provider.
 */
public record IrohHttpRequest(String method, String path, Map<String, List<String>> headers,
                              byte[] body, Duration timeout, long maxResponseBytes) {
    public IrohHttpRequest {
        if (method == null || method.isBlank() || path == null || !path.startsWith("/")) {
            throw new IrohConfigurationException("invalid Iroh HTTP request method or path");
        }
        if (timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new IrohConfigurationException("Iroh HTTP request timeout must be positive");
        }
        if (maxResponseBytes < 1) {
            throw new IrohConfigurationException("Iroh HTTP response limit must be positive");
        }
        Map<String, List<String>> copied = new LinkedHashMap<>();
        if (headers != null) headers.forEach((name, values) ->
                copied.put(name, values == null ? List.of() : List.copyOf(values)));
        headers = Collections.unmodifiableMap(copied);
        body = body == null ? new byte[0] : body;
    }
}
