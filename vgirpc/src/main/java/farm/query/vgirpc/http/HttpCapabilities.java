// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import java.util.List;

/**
 * What an HTTP worker advertises about itself, read from the response headers
 * of {@code OPTIONS {endpoint}/health}.
 *
 * <p>These are the numbers a client has to know <em>before</em> it sends
 * anything, because every one of them turns a request that would have failed
 * into one that succeeds: a body above {@link #maxRequestBytes()} has to be
 * externalized through the upload-URL flow rather than sent inline, a codec the
 * server does not list in {@link #supportedEncodings()} makes a compressed body
 * a 415, and a sticky session cannot be opened at all against a worker whose
 * {@link #stickyEnabled()} is false. Discovering them from a failed call
 * instead costs a round trip and, for the request-size case, an upload of bytes
 * the server was always going to refuse.</p>
 *
 * <h2>Absent is not zero, and absent is not empty</h2>
 *
 * <p>Every cap is a boxed type so that "the server advertises no limit"
 * ({@code null}) stays distinguishable from "the server advertises a limit of
 * zero", which would mean nothing can ever be sent. The same distinction is
 * what makes {@link #supportedEncodings()} subtle: a <em>missing</em>
 * {@code VGI-Supported-Encodings} header is a worker predating the header, and
 * every such worker decodes zstd — so it is read as {@code ["zstd"]}, not as
 * "none". A header that is <em>present but empty</em> is a worker positively
 * stating it speaks no compression, and is read as an empty list. Collapsing
 * the two would either send zstd to a server that answers 415, or refuse to
 * compress for every server built before the header existed.</p>
 *
 * <p>Tokens in {@link #supportedEncodings()} are the lowercase wire spellings
 * ({@code zstd}, {@code gzip}, {@code identity}) rather than any enum's name,
 * because they are compared against {@code Content-Encoding} values and against
 * the same list as reported by other language ports.</p>
 *
 * @param stickyEnabled whether the worker honours {@code VGI-Session} affinity
 * @param stickyDefaultTtl default session lifetime in seconds, or {@code null}
 *     when the worker advertises none
 * @param stickyEchoHeaders names (without the {@code VGI-Echo-} prefix) the
 *     worker asks a session-bound client to echo on later requests; never
 *     {@code null}, possibly empty
 * @param uploadUrlSupport whether {@code __upload_url__/init} is routed, i.e.
 *     whether {@link HttpRpcConnection#requestUploadUrls(int)} can succeed
 * @param maxRequestBytes largest inline request body the worker accepts, or
 *     {@code null} for no advertised limit
 * @param maxResponseBytes largest response body the worker will produce, or
 *     {@code null} for no advertised limit
 * @param maxExternalizedResponseBytes cap on the bytes one response may push
 *     through external storage, or {@code null} for no advertised limit
 * @param externalizationEnabled whether the worker has storage wired up at all;
 *     when false, externalization cannot rescue an oversize response
 * @param maxUploadBytes largest body a client-vended upload URL accepts, or
 *     {@code null} for no advertised limit
 * @param supportedEncodings lowercase content-coding tokens the worker can
 *     decode on requests and produce on responses; never {@code null}
 */
public record HttpCapabilities(
        boolean stickyEnabled,
        Integer stickyDefaultTtl,
        List<String> stickyEchoHeaders,
        boolean uploadUrlSupport,
        Long maxRequestBytes,
        Long maxResponseBytes,
        Long maxExternalizedResponseBytes,
        boolean externalizationEnabled,
        Long maxUploadBytes,
        List<String> supportedEncodings) {

    /**
     * Canonicalise the two list components so a caller cannot mutate a snapshot
     * the connection caches and hands to every later caller.
     */
    public HttpCapabilities {
        stickyEchoHeaders = stickyEchoHeaders == null ? List.of() : List.copyOf(stickyEchoHeaders);
        supportedEncodings = supportedEncodings == null ? List.of() : List.copyOf(supportedEncodings);
    }

    /**
     * Whether the worker can decode {@code encoding} on a request body.
     *
     * @param encoding a lowercase content-coding token
     * @return {@code true} when the worker advertises it
     */
    public boolean supports(String encoding) {
        return supportedEncodings.contains(encoding);
    }
}
