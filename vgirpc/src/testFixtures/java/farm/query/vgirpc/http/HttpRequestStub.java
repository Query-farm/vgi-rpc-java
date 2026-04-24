// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import jakarta.servlet.http.HttpServletRequest;

import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;

/**
 * Test helper: a minimal {@link HttpServletRequest} backed by a header map.
 * All other methods return safe defaults ({@code null}, {@code 0}, {@code false},
 * etc.). Header lookup is case-insensitive per the servlet spec.
 */
public final class HttpRequestStub {

    private HttpRequestStub() {}

    public static HttpServletRequest withHeaders(Map<String, String> headers) {
        Map<String, String> h = new HashMap<>(headers);
        return (HttpServletRequest) Proxy.newProxyInstance(
                HttpRequestStub.class.getClassLoader(),
                new Class<?>[]{HttpServletRequest.class},
                (proxy, method, args) -> {
                    if ("getHeader".equals(method.getName()) && args != null && args.length == 1) {
                        String key = (String) args[0];
                        for (Map.Entry<String, String> e : h.entrySet()) {
                            if (e.getKey().equalsIgnoreCase(key)) return e.getValue();
                        }
                        return null;
                    }
                    return switch (method.getName()) {
                        case "hashCode" -> 0;
                        case "toString" -> "stub";
                        case "equals"   -> proxy == args[0];
                        default         -> null;
                    };
                });
    }

    /** Shortcut: a stub with a single {@code Authorization: Bearer <token>} header. */
    public static HttpServletRequest withBearer(String token) {
        return withHeaders(Map.of("Authorization", "Bearer " + token));
    }
}
