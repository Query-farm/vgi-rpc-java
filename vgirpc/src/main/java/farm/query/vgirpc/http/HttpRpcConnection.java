// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import farm.query.vgirpc.ClientMarshalling;
import farm.query.vgirpc.MethodType;
import farm.query.vgirpc.RpcError;
import farm.query.vgirpc.RpcMethodInfo;
import farm.query.vgirpc.ServiceIntrospector;
import farm.query.vgirpc.external.LocationResolver;
import farm.query.vgirpc.log.Message;
import farm.query.vgirpc.marshal.Marshalling;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.wire.Allocators;
import farm.query.vgirpc.wire.IpcStreamReader;
import farm.query.vgirpc.wire.Metadata;
import farm.query.vgirpc.wire.Wire;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * A client-side RPC connection that speaks the vgi-rpc <em>HTTP</em> transport,
 * offering the same {@link #proxy(Class)} surface as
 * {@link farm.query.vgirpc.RpcConnection} so a caller can move a service
 * between transports without touching a call site.
 *
 * <pre>{@code
 * try (HttpRpcConnection conn = HttpRpcConnection.builder("http://host:8080/vgi")
 *         .bearerToken(token)
 *         .build()) {
 *     MyService svc = conn.proxy(MyService.class);
 *     long answer = svc.add(2, 3);                       // unary
 *     try (RpcStream<?> s = svc.scan(1000)) {            // producer stream
 *         for (AnnotatedBatch b : s.batches()) { ... }
 *     }
 * }
 * }</pre>
 *
 * <h2>Why this is not an {@code RpcTransport}</h2>
 *
 * <p>{@link farm.query.vgirpc.RpcConnection} is built on
 * {@link farm.query.vgirpc.transport.RpcTransport}, a duplex <em>byte stream</em>:
 * a call writes a request IPC stream into it and reads the response back out of
 * the same never-ending pair of pipes. HTTP has no such object. A stream over
 * HTTP is a chain of independent request/response pairs — {@code POST
 * /{method}/init} and then one {@code POST /{method}/exchange} per turn — whose
 * continuity is carried by a state token in the response body's Arrow
 * {@code custom_metadata}, not by a socket that stays open. Dressing that up as
 * an {@code RpcTransport} would mean inventing a fictional byte stream and then
 * guessing, from bytes alone, where one turn ends and the next request begins.
 * So the transports are siblings rather than one wrapping the other, and what
 * they genuinely share — request framing, result decoding, {@code @StreamHeader}
 * resolution — lives in {@link ClientMarshalling}.</p>
 *
 * <h2>Scope</h2>
 *
 * <p>Bodies are sent and accepted as {@code identity} (uncompressed): the
 * connection states {@code Accept-Encoding: identity}, which the server honours
 * as an explicit per-request "compression off". Content-encoding negotiation,
 * OAuth/PKCE, sticky-session affinity, upload-URL externalization and resumable
 * scan tokens are deliberately not implemented here. An externalized
 * ({@code vgi_rpc.location}) batch is reported as an {@link RpcError} rather
 * than silently delivered as an empty batch — see
 * {@link #failOnPointerBatch(Map)}.</p>
 *
 * <p>Instances are safe to share for <em>independent</em> calls (the underlying
 * {@link HttpClient} is thread-safe and no per-connection state is kept), but an
 * individual {@link HttpRpcStream} is not thread-safe, exactly like
 * {@code ClientStreamSession}.</p>
 */
public final class HttpRpcConnection implements AutoCloseable {

    private final HttpClient http;
    /** Whether {@link #close()} owns the {@link HttpClient}; false when the caller supplied one. */
    private final boolean ownsHttpClient;
    /** Endpoint prefix with no trailing slash, e.g. {@code http://host:8080/vgi}. */
    private final String endpoint;
    private final Map<String, String> headers;
    private final Consumer<Message> onLog;
    private final Duration requestTimeout;
    /** Caller-supplied override for the service interface's own declared version, or {@code null}. */
    private final String protocolVersion;

    private HttpRpcConnection(Builder b) {
        this.endpoint = b.endpoint;
        this.headers = Map.copyOf(b.headers);
        this.onLog = b.onLog != null ? b.onLog : m -> {};
        this.requestTimeout = b.requestTimeout;
        this.protocolVersion = b.protocolVersion;
        if (b.httpClient != null) {
            this.http = b.httpClient;
            this.ownsHttpClient = false;
        } else {
            this.http = HttpClient.newBuilder()
                    .connectTimeout(b.connectTimeout)
                    .build();
            this.ownsHttpClient = true;
        }
    }

    /**
     * Start building a connection against a worker's RPC endpoint.
     *
     * @param endpoint the full URL prefix the worker's methods hang off, i.e.
     *     scheme, authority and the server's configured path prefix
     *     ({@code http://127.0.0.1:8080/vgi}). A trailing slash is tolerated.
     * @return a new builder
     */
    public static Builder builder(String endpoint) { return new Builder(endpoint); }

    /**
     * Create a typed dynamic proxy that implements {@code serviceInterface},
     * dispatching each call over HTTP.
     *
     * <p>Deliberately the same signature and behaviour as
     * {@link farm.query.vgirpc.RpcConnection#proxy(Class)}: unary methods return
     * the decoded result, streaming methods return an {@link HttpRpcStream}
     * (a {@link farm.query.vgirpc.RpcStream}), so swapping transports is a
     * one-line change at construction and nothing at the call sites.</p>
     *
     * @param serviceInterface the RPC service interface to implement
     * @param <T> the service type
     * @return a proxy instance bound to this connection
     */
    @SuppressWarnings("unchecked")
    public <T> T proxy(Class<T> serviceInterface) {
        Map<String, RpcMethodInfo> methods = ServiceIntrospector.describe(serviceInterface);
        // The interface's own @ProtocolVersion unless the builder overrode it:
        // a versioned worker rejects a request with no vgi_rpc.protocol_version,
        // so getting this from the contract rather than from the call site is
        // what makes proxy(X.class) work against one.
        String version = protocolVersion != null
                ? protocolVersion
                : ServiceIntrospector.protocolVersion(serviceInterface);
        return (T) Proxy.newProxyInstance(
                serviceInterface.getClassLoader(),
                new Class<?>[]{serviceInterface},
                new ClientHandler(methods, version));
    }

    /**
     * Release the underlying {@link HttpClient}, if this connection created it.
     *
     * <p>A client supplied via {@link Builder#httpClient(HttpClient)} is left
     * alone — it belongs to the caller and may be shared with other
     * connections.</p>
     */
    @Override
    public void close() {
        if (ownsHttpClient) http.close();
    }

    // ------------------------------------------------------------------
    // Internals shared with HttpRpcStream
    // ------------------------------------------------------------------

    Consumer<Message> onLog() { return onLog; }

    String urlFor(String method, String suffix) { return endpoint + "/" + method + suffix; }

    /**
     * POST an Arrow IPC body and return the response body, having established
     * that it really is one.
     *
     * <p>An HTTP-level failure must not reach the caller as a bare
     * {@link IOException}: the whole point of the RPC layer is that a remote
     * failure arrives as {@link RpcError}, whichever transport carried it. So a
     * non-Arrow response (a 401 JSON envelope, a 413/415 error object, a proxy's
     * HTML page) is translated here, and an Arrow response is handed on
     * unexamined — a worker-level error rides <em>inside</em> it as an EXCEPTION
     * batch, which the reader paths surface with the server's own type and
     * message.</p>
     *
     * @param url absolute request URL
     * @param body the request IPC stream bytes
     * @param what a short description of the call, used in transport-error messages
     * @return the response body bytes (an Arrow IPC stream)
     */
    byte[] post(String url, byte[] body, String what) {
        HttpRequest.Builder req = HttpRequest.newBuilder(URI.create(url))
                .header(HttpHeaders.CONTENT_TYPE, HttpServer.ARROW_CONTENT_TYPE)
                // Explicitly opt out of response compression. The server treats
                // an identity-first accept list as a per-request "compression
                // off" switch, so this is a supported choice and not a gap the
                // server has to guess at.
                .header(HttpHeaders.ACCEPT_ENCODING, MediaTypes.IDENTITY)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body));
        if (requestTimeout != null) req.timeout(requestTimeout);
        headers.forEach(req::header);

        HttpResponse<byte[]> resp;
        try {
            resp = http.send(req.build(), HttpResponse.BodyHandlers.ofByteArray());
        } catch (IOException e) {
            throw new RpcError("TransportError", what + ": " + e, "");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RpcError("TransportError", what + ": interrupted", "");
        }
        return requireArrowBody(resp, what);
    }

    private static byte[] requireArrowBody(HttpResponse<byte[]> resp, String what) {
        String contentType = resp.headers().firstValue(HttpHeaders.CONTENT_TYPE).orElse("");
        if (contentType.startsWith(HttpServer.ARROW_CONTENT_TYPE)) {
            // 200 or 500 alike: the body is a well-formed Arrow stream and any
            // error is in it. HttpServer answers a failed stream turn with 500
            // plus an error stream, so status alone is not the signal.
            return resp.body();
        }
        int status = resp.statusCode();
        String detail = preview(resp.body());
        if (status == 401) {
            throw new RpcError("AuthenticationError",
                    what + ": unauthorized (HTTP 401)"
                            + (detail.isEmpty() ? "" : " — " + detail), "", "", "unauthorized");
        }
        throw new RpcError("HttpError",
                what + ": HTTP " + status + " with a non-Arrow body"
                        + (detail.isEmpty() ? "" : " — " + detail), "");
    }

    /** First 200 bytes of a non-Arrow body, for an error message a human can act on. */
    private static String preview(byte[] body) {
        if (body == null || body.length == 0) return "";
        int n = Math.min(body.length, 200);
        return new String(body, 0, n, StandardCharsets.UTF_8).replace('\n', ' ').trim();
    }

    /**
     * Refuse an externalized ({@code vgi_rpc.location}) pointer batch.
     *
     * <p>This connection does not fetch external payloads. Handing the caller
     * the pointer's zero-row body would be silent row loss on exactly the path
     * externalization exists for — large results — so it is a hard failure with
     * the same {@code ExternalLocationError} type
     * {@code ClientStreamSession} raises, letting one catch cover both clients.</p>
     *
     * @param meta the pointer batch's custom metadata
     */
    static void failOnPointerBatch(Map<String, String> meta) {
        throw new RpcError("ExternalLocationError",
                "the worker returned an externalized batch (" + Metadata.LOCATION + "="
                        + LocationResolver.redactUrl(meta.get(Metadata.LOCATION))
                        + "); HttpRpcConnection does not resolve "
                        + "external locations", "");
    }

    /**
     * Handle one batch's log/error/pointer semantics.
     *
     * @return {@code true} when the batch was a log line and the caller should read on
     */
    static boolean dispatchNonData(Map<String, String> meta, int rowCount, Consumer<Message> onLog) {
        Wire.BatchKind kind = Wire.classify(rowCount, meta);
        if (kind == Wire.BatchKind.LOG) {
            onLog.accept(Wire.messageFromMetadata(meta));
            return true;
        }
        if (kind == Wire.BatchKind.ERROR) throw Wire.errorFromMetadata(meta);
        if (LocationResolver.isPointer(rowCount, meta)) failOnPointerBatch(meta);
        return false;
    }

    // ------------------------------------------------------------------

    private final class ClientHandler implements InvocationHandler {

        private final Map<String, RpcMethodInfo> methods;
        private final String protocolVersion;

        ClientHandler(Map<String, RpcMethodInfo> methods, String protocolVersion) {
            this.methods = methods;
            this.protocolVersion = protocolVersion;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (method.getDeclaringClass() == Object.class) {
                return method.invoke(this, args);
            }
            RpcMethodInfo info = methods.get(method.getName());
            if (info == null) throw new RpcError("AttributeError", "Unknown method: " + method.getName(), "");
            return info.methodType() == MethodType.STREAM
                    ? doStream(info, method, args)
                    : doUnary(info, method, args);
        }

        private Object doUnary(RpcMethodInfo info, Method m, Object[] args) throws IOException {
            byte[] response = post(urlFor(info.name(), ""), requestBody(info, m, args), info.name());
            try (IpcStreamReader r = new IpcStreamReader(
                    new ByteArrayInputStream(response), Allocators.root())) {
                while (true) {
                    Map<String, String> md = r.readNextBatch();
                    if (md == null) {
                        throw new RpcError("ProtocolError",
                                info.name() + ": response stream ended without a result batch", "");
                    }
                    VectorSchemaRoot root = r.root();
                    if (dispatchNonData(md, root.getRowCount(), onLog)) continue;
                    return ClientMarshalling.decodeResult(info, root);
                }
            }
        }

        private Object doStream(RpcMethodInfo info, Method m, Object[] args) throws IOException {
            byte[] response = post(urlFor(info.name(), "/init"),
                    requestBody(info, m, args), info.name() + "/init");
            // The init response is a *sequence* of IPC streams when the method
            // declares a header: the header stream, then the stream body. One
            // reader per stream, each picking up where the last one's
            // end-of-stream marker left off.
            ByteArrayInputStream in = new ByteArrayInputStream(response);
            ArrowSerializableRecord header = null;
            Class<?> headerType = ClientMarshalling.resolveHeaderType(info);
            if (headerType != null) header = readHeaderStream(in, headerType);
            return new HttpRpcStream<>(HttpRpcConnection.this, info.name(), in, header);
        }

        private byte[] requestBody(RpcMethodInfo info, Method m, Object[] args) throws IOException {
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            ClientMarshalling.writeRequest(buf, info, m, args, protocolVersion);
            return buf.toByteArray();
        }

        /**
         * Read the declared {@code @StreamHeader} record off the front of an
         * init response.
         *
         * <p>Also the error path for an {@code /init} that failed before the
         * header was written: the server then answers a single error stream, so
         * the EXCEPTION batch is read here and raised with the worker's own type
         * and message rather than being mistaken for a header.</p>
         */
        @SuppressWarnings("unchecked")
        private ArrowSerializableRecord readHeaderStream(ByteArrayInputStream in, Class<?> headerType)
                throws IOException {
            try (IpcStreamReader r = new IpcStreamReader(in, Allocators.root())) {
                while (true) {
                    Map<String, String> md = r.readNextBatch();
                    if (md == null) {
                        throw new RpcError("ProtocolError", "stream header missing from init response", "");
                    }
                    if (dispatchNonData(md, r.root().getRowCount(), onLog)) continue;
                    Map<String, Object> row = Marshalling.decodeRow(r.root(), r.dictionaryProvider(), r.wireSchema());
                    ArrowSerializableRecord header = RecordCodec.fromRowMap(
                            (Class<? extends ArrowSerializableRecord>) headerType, row);
                    // Consume the header stream's trailing EOS so the body
                    // stream that follows starts at a clean boundary.
                    r.drain();
                    return header;
                }
            }
        }
    }

    // ------------------------------------------------------------------

    /** Builder for {@link HttpRpcConnection}. */
    public static final class Builder {

        private final String endpoint;
        private final Map<String, String> headers = new LinkedHashMap<>();
        private Consumer<Message> onLog;
        private Duration requestTimeout = Duration.ofMinutes(5);
        private Duration connectTimeout = Duration.ofSeconds(10);
        private HttpClient httpClient;
        private String protocolVersion;

        private Builder(String endpoint) {
            if (endpoint == null || endpoint.isBlank()) {
                throw new IllegalArgumentException("endpoint must not be blank");
            }
            String trimmed = endpoint.trim();
            while (trimmed.endsWith("/")) trimmed = trimmed.substring(0, trimmed.length() - 1);
            this.endpoint = trimmed;
        }

        /**
         * Authenticate every request with a static bearer token.
         *
         * <p>Sent verbatim as {@code Authorization: Bearer <token>} on each
         * request — HTTP is stateless, so there is no "login" turn to hang it
         * off. Interactive flows (OAuth device code / PKCE) are the
         * {@code vgirpc-oauth} module's concern; supply their resulting access
         * token here.</p>
         *
         * @param token the bearer token, without the {@code Bearer } prefix
         * @return this builder
         */
        public Builder bearerToken(String token) {
            headers.put(HttpHeaders.AUTHORIZATION, HttpHeaders.BEARER_PREFIX + token);
            return this;
        }

        /**
         * Add a static header sent on every request (API keys, tracing, tenant
         * routing).
         *
         * @param name header name
         * @param value header value
         * @return this builder
         */
        public Builder header(String name, String value) {
            headers.put(name, value);
            return this;
        }

        /**
         * Receive the log batches the worker interleaves into responses.
         *
         * <p>Without a sink they are read and discarded — never mistaken for
         * data — so this is purely about surfacing them.</p>
         *
         * @param sink the log consumer, or {@code null} to discard
         * @return this builder
         */
        public Builder onLog(Consumer<Message> sink) {
            this.onLog = sink;
            return this;
        }

        /**
         * Per-request timeout.
         *
         * <p>Generous by default (5 minutes) because one request can be a whole
         * producer turn: an HTTP worker may do heavy server-side compute before
         * the first byte of the response exists.</p>
         *
         * @param timeout the timeout, or {@code null} for none
         * @return this builder
         */
        public Builder requestTimeout(Duration timeout) {
            this.requestTimeout = timeout;
            return this;
        }

        /**
         * TCP connect timeout for the {@link HttpClient} this builder creates.
         * Ignored when {@link #httpClient(HttpClient)} supplies one.
         *
         * @param timeout the connect timeout
         * @return this builder
         */
        public Builder connectTimeout(Duration timeout) {
            this.connectTimeout = timeout;
            return this;
        }

        /**
         * Use a caller-supplied {@link HttpClient} — for a shared connection
         * pool, a custom executor, TLS material, or a proxy selector.
         *
         * <p>Ownership stays with the caller: {@link HttpRpcConnection#close()}
         * will not close it.</p>
         *
         * @param client the client to dispatch through
         * @return this builder
         */
        /**
         * Override the application protocol version stamped on every request.
         *
         * <p>Normally unnecessary — the version comes from the service
         * interface's {@link farm.query.vgirpc.schema.ProtocolVersion}, which
         * is where the wire contract is declared. Set it to speak a different
         * revision of a protocol than the interface declares, or {@code ""} to
         * send no version key at all.
         *
         * @param version the version to send, or {@code ""} for none
         * @return this builder
         */
        public Builder protocolVersion(String version) {
            this.protocolVersion = version == null ? "" : version;
            return this;
        }

        public Builder httpClient(HttpClient client) {
            this.httpClient = client;
            return this;
        }

        /**
         * Build the connection.
         *
         * @return a ready connection
         */
        public HttpRpcConnection build() { return new HttpRpcConnection(this); }
    }
}
