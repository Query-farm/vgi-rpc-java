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
    public static final String API_KEY          = "X-API-Key";
    public static final String USER_AGENT       = "User-Agent";
    public static final String CONTENT_TYPE     = "Content-Type";
    public static final String CONTENT_ENCODING = "Content-Encoding";
    public static final String ACCEPT_ENCODING  = "Accept-Encoding";

    /** XFCC (Envoy / Istio forwarded client certificate) header. */
    public static final String X_FORWARDED_CLIENT_CERT = "x-forwarded-client-cert";
    /** Nginx/ingress alternative mTLS client-cert header. */
    public static final String X_SSL_CLIENT_CERT       = "X-SSL-Client-Cert";

    public static final String BEARER_PREFIX = "Bearer ";
}
