// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

/**
 * Well-known HTTP header names used by the vgi-rpc transport. Kept in one
 * place so the wire surface is greppable, matching the discipline
 * {@code wire.Metadata} applies to Arrow custom metadata keys.
 */
public final class HttpHeaders {

    private HttpHeaders() {}

    public static final String AUTHORIZATION    = "Authorization";
    public static final String WWW_AUTHENTICATE = "WWW-Authenticate";
    /**
     * Per-request correlation id, echoed from the caller when supplied and
     * generated otherwise. It is what ties a failure a client saw to this
     * server's own log line for the same call, so it rides every response —
     * including the error paths, which are the ones anybody looks up.
     */
    public static final String REQUEST_ID       = "X-Request-ID";
    public static final String API_KEY          = "X-API-Key";
    public static final String USER_AGENT       = "User-Agent";
    public static final String CONTENT_TYPE     = "Content-Type";
    public static final String CONTENT_ENCODING = "Content-Encoding";
    public static final String ACCEPT_ENCODING  = "Accept-Encoding";

    /** VGI's own response-codec preference, in the client's preferred order.
     *  Takes precedence over {@link #ACCEPT_ENCODING}, which HTTP clients
     *  (e.g. cpp-httplib, used by the DuckDB extension) inject with an order
     *  of their own. Browsers/WASM can only use this header — {@code fetch()}
     *  cannot set {@code Accept-Encoding} (a forbidden header name). */
    public static final String X_VGI_ACCEPT_ENCODING = "X-VGI-Accept-Encoding";

    /** Response codec, stamped here instead of {@link #CONTENT_ENCODING} when
     *  the client's choice came only from {@link #X_VGI_ACCEPT_ENCODING} — such
     *  a client's fetch/proxy layer would mangle or auto-decode a standard
     *  {@code Content-Encoding}, so the response must not claim one. */
    public static final String X_VGI_CONTENT_ENCODING = "X-VGI-Content-Encoding";

    /** Carries one {@link AuthReason} on every 401. */
    public static final String VGI_AUTH_REASON = "VGI-Auth-Reason";

    /** {@code "true"} on 401s from a service whose auth depends on a reverse
     *  proxy. Omitted otherwise — never {@code "false"}, since a note that
     *  appears everywhere is one operators learn to skip. */
    public static final String VGI_AUTH_PROXY_REQUIRED = "VGI-Auth-Proxy-Required";

    /** XFCC (Envoy / Istio forwarded client certificate) header. */
    public static final String X_FORWARDED_CLIENT_CERT = "x-forwarded-client-cert";
    /** Nginx/ingress alternative mTLS client-cert header. */
    public static final String X_SSL_CLIENT_CERT       = "X-SSL-Client-Cert";

    public static final String BEARER_PREFIX = "Bearer ";
}
