// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The body half of the standardized 401 ({@code docs/unauthorized-spec.md} §4).
 *
 * <p>This port always answers JSON. §4.2 permits that — a service may skip the
 * HTML page entirely; what it must never do is answer a non-HTML request with
 * HTML — and it keeps the one part clients parse, the reason code, identical
 * for browsers and RPC clients alike.</p>
 */
final class Unauthorized {

    private Unauthorized() {}

    /**
     * Compose the operator-facing proxy note for §5.
     *
     * <p>The wording is not normative. It must convey that the service is only
     * reachable through its proxy, which headers that proxy has to set, and
     * that a rejection here is at least as likely to be a proxy
     * misconfiguration as a bad credential — which is the deployment where
     * <em>every</em> request 401s and rotating credentials fixes nothing.</p>
     */
    static String proxyHint(List<String> headers) {
        if (headers.isEmpty()) return "";
        String listed = String.join(", ", headers);
        String noun = headers.size() == 1 ? "header" : "headers";
        return "This service only accepts requests that arrive through its configured reverse proxy, "
                + "which must set the " + listed + " " + noun + ". A rejection here is at least as likely "
                + "to be a proxy that is not forwarding " + (headers.size() == 1 ? "that header" : "those headers")
                + " — or a request that reached the service without passing through the proxy at all — as it "
                + "is a bad credential. Check the proxy configuration before rotating credentials.";
    }

    /**
     * Build the §4.3 envelope.
     *
     * <p>{@code proxy_hint} is absent, not empty, when it does not apply: its
     * presence alone has to be a usable signal.</p>
     */
    static Map<String, String> envelope(AuthReason reason, String detail, String proxyHint) {
        Map<String, String> body = new LinkedHashMap<>();
        body.put("error", "unauthorized");
        body.put("reason", reason.code());
        body.put("detail", detail != null ? detail : "");
        if (proxyHint != null && !proxyHint.isEmpty()) body.put("proxy_hint", proxyHint);
        return body;
    }
}
