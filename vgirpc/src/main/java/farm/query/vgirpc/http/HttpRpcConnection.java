// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import com.github.luben.zstd.Zstd;
import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.ClientMarshalling;
import farm.query.vgirpc.MethodType;
import farm.query.vgirpc.RawStream;
import farm.query.vgirpc.RpcError;
import farm.query.vgirpc.RpcMethodInfo;
import farm.query.vgirpc.ServiceIntrospector;
import farm.query.vgirpc.StreamState;
import farm.query.vgirpc.external.ExternalFetcher;
import farm.query.vgirpc.external.ExternalLocationConfig;
import farm.query.vgirpc.external.LocationResolver;
import farm.query.vgirpc.log.Message;
import farm.query.vgirpc.marshal.Marshalling;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.transport.IrohDispatchCertainty;
import farm.query.vgirpc.transport.IrohEndpoint;
import farm.query.vgirpc.transport.IrohErrorCategory;
import farm.query.vgirpc.transport.IrohErrorStage;
import farm.query.vgirpc.transport.IrohHttpRequest;
import farm.query.vgirpc.transport.IrohHttpResponse;
import farm.query.vgirpc.transport.IrohHttpTransport;
import farm.query.vgirpc.transport.IrohTransportException;
import farm.query.vgirpc.transport.IrohTransportOptions;
import farm.query.vgirpc.transport.IrohTransportProvider;
import farm.query.vgirpc.wire.Allocators;
import farm.query.vgirpc.wire.IpcStreamReader;
import farm.query.vgirpc.wire.IpcStreamWriter;
import farm.query.vgirpc.wire.Metadata;
import farm.query.vgirpc.wire.Wire;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.eclipse.jetty.client.BytesRequestContent;
import org.eclipse.jetty.client.InputStreamResponseListener;
import org.eclipse.jetty.client.Response;
import org.eclipse.jetty.client.Socks5Proxy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
 * <p>Bodies are sent and accepted as {@code identity} (uncompressed) unless
 * {@link Builder#compressionLevel(Integer)} turns request compression on: the
 * connection then states the codecs it can decode and compresses uphill with
 * zstd, but only when the worker <em>advertises</em> zstd. Otherwise it states
 * {@code Accept-Encoding: identity}, which the server honours as an explicit
 * per-request "compression off". OAuth/PKCE remains the {@code vgirpc-oauth}
 * module's concern.</p>
 *
 * <p>Beyond the typed {@link #proxy(Class)} surface this connection also offers
 * what a client that is not calling a Java interface needs: an untyped
 * {@linkplain #callUnaryRaw call} / {@linkplain #openStreamRaw stream} surface
 * that relays framed Arrow rather than decoded values,
 * {@linkplain #capabilities() capability discovery},
 * {@linkplain #requestUploadUrls(int) upload URLs}, and
 * {@linkplain #beginSession(String) sticky sessions}. An externalized
 * ({@code vgi_rpc.location}) batch is resolved transparently when the builder
 * was given an {@link ExternalLocationConfig}, and is reported as an
 * {@link RpcError} rather than silently delivered as an empty batch when it was
 * not — see {@link #failOnPointerBatch(Map)}.</p>
 *
 * <p>Instances are safe to share for <em>independent</em> calls (the underlying
 * {@link HttpClient} is thread-safe), but an individual {@link HttpRpcStream}
 * is not thread-safe, exactly like {@code ClientStreamSession}. A sticky
 * session is connection-wide state by definition — every request on the
 * connection carries it — so opening one is not an independent call.</p>
 */
public final class HttpRpcConnection implements AutoCloseable {

    /** Native-client default receive budget: 256 MiB of decoded Arrow IPC. */
    public static final long DEFAULT_ACCEPTED_MAX_RESPONSE_BYTES = 256L << 20;

    private final HttpClient http;
    /** Jetty is used only for explicit SOCKS5h because the JDK HTTP client supports HTTP proxies only. */
    private final org.eclipse.jetty.client.HttpClient socksHttp;
    /** Typed HTTP/1.1-over-Iroh transport; mutually exclusive with both HTTP clients. */
    private final IrohHttpTransport irohHttp;
    /** Whether {@link #close()} owns the {@link HttpClient}; false when the caller supplied one. */
    private final boolean ownsHttpClient;
    /** Endpoint prefix with no trailing slash, e.g. {@code http://host:8080/vgi}. */
    private final String endpoint;
    private final Map<String, String> headers;
    private final Consumer<Message> onLog;
    private final Duration requestTimeout;
    /** Caller-supplied override for the service interface's own declared version, or {@code null}. */
    private final String protocolVersion;
    private final Long acceptedMaxResponseBytes;
    /** External-storage configuration, or null when pointer batches are refused. */
    private final ExternalLocationConfig externalConfig;
    /** Fetcher for externalized ({@code vgi_rpc.location}) batches, or null. */
    private final ExternalFetcher externalFetcher;
    /** zstd level for request bodies, or null when request compression is off. */
    private final Integer compressionLevel;
    private volatile boolean responseBudgetSupportVerified;
    /** Strictly validated decoded cap advertised by OPTIONS /health, or null. */
    private volatile Long advertisedMaxResponseBytes;
    /** The cached OPTIONS /health reply both capability readers share. */
    private volatile HttpReply healthProbe;
    /** Leniently parsed view of {@link #healthProbe}. */
    private volatile HttpCapabilities capabilities;
    /**
     * Open sticky-session scopes, innermost first. Empty when none is open;
     * {@link #session} mirrors the top so the request path reads one volatile
     * rather than taking the connection lock on every header build.
     */
    private final java.util.Deque<SessionState> sessions = new java.util.ArrayDeque<>();
    /** The innermost open scope, or null — always {@code sessions.peek()}. */
    private volatile SessionState session;

    /**
     * Content codings this client can decode, in preference order — what it
     * offers on {@code Accept-Encoding} once compression is enabled at all.
     * gzip rides along because {@link ContentCodec} decodes it and a worker may
     * prefer it; only zstd is ever <em>produced</em> here.
     */
    private static final String DECODABLE_RESPONSE_ENCODINGS =
            MediaTypes.ZSTD + ", " + MediaTypes.GZIP;

    private HttpRpcConnection(Builder b) { this(b, null); }

    private HttpRpcConnection(Builder b, IrohHttpTransport irohHttp) {
        this.endpoint = b.endpoint;
        this.headers = Map.copyOf(b.headers);
        this.onLog = b.onLog != null ? b.onLog : m -> {};
        this.requestTimeout = b.requestTimeout;
        this.protocolVersion = b.protocolVersion;
        this.acceptedMaxResponseBytes = b.acceptedMaxResponseBytes;
        this.externalConfig = b.externalConfig;
        this.externalFetcher = b.externalConfig != null ? new ExternalFetcher(b.externalConfig) : null;
        this.compressionLevel = b.compressionLevel;
        this.irohHttp = irohHttp;
        if (irohHttp != null) {
            this.http = null;
            this.socksHttp = null;
            this.ownsHttpClient = true;
        } else if (b.socksProxy != null) {
            if (b.httpClient != null) {
                throw new IllegalArgumentException("socks5hProxy cannot be combined with httpClient");
            }
            this.http = null;
            this.socksHttp = new org.eclipse.jetty.client.HttpClient();
            this.socksHttp.setConnectTimeout(b.connectTimeout.toMillis());
            this.socksHttp.getProxyConfiguration().addProxy(
                    new Socks5Proxy(b.socksProxy.host(), b.socksProxy.port()));
            try {
                this.socksHttp.start();
            } catch (Exception e) {
                try { this.socksHttp.stop(); } catch (Exception ignored) {}
                throw new IllegalStateException("could not start SOCKS5h HTTP client", e);
            }
            this.ownsHttpClient = true;
        } else if (b.httpClient != null) {
            this.http = b.httpClient;
            this.socksHttp = null;
            this.ownsHttpClient = false;
        } else {
            this.http = HttpClient.newBuilder()
                    .connectTimeout(b.connectTimeout)
                    .build();
            this.socksHttp = null;
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
     * Build a configurable HTTP-semantics connection over {@code iroh-http/2}
     * using the installed native provider.
     *
     * <p>After applying ordinary builder options, call {@link Builder#buildIroh()}.
     * The resulting connection owns and closes the provider transport.</p>
     */
    public static Builder irohBuilder(String endpoint, IrohTransportOptions options)
            throws IOException {
        IrohTransportProvider provider = ServiceLoader.load(IrohTransportProvider.class)
                .findFirst().orElseThrow(() -> new IrohTransportException(
                        "httpi:// requires the optional official Kotlin/JVM Iroh provider",
                        IrohErrorStage.BIND, IrohErrorCategory.UNSUPPORTED,
                        IrohDispatchCertainty.NOT_SENT));
        return irohBuilder(endpoint, options, provider);
    }

    /** Build an Iroh HTTP connection with default endpoint and timeout options. */
    public static Builder irohBuilder(String endpoint) throws IOException {
        return irohBuilder(endpoint, IrohTransportOptions.defaults());
    }

    /** Build a configurable HTTP-semantics connection with an explicit provider. */
    public static Builder irohBuilder(String rawEndpoint, IrohTransportOptions options,
                                      IrohTransportProvider provider) {
        if (provider == null) throw new NullPointerException("provider");
        IrohEndpoint parsed = IrohEndpoint.parse(rawEndpoint);
        if (parsed.scheme() != IrohEndpoint.Scheme.HTTPI) {
            throw new IllegalArgumentException("Iroh HTTP connection requires httpi://");
        }
        return new Builder("http://iroh.invalid" + parsed.basePath(), parsed,
                options == null ? IrohTransportOptions.defaults() : options, provider);
    }

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
        // The routing key rides both carriers: the path segment, so an edge can act on it, and
        // vgi_rpc.protocol, which stays canonical. They are derived from one place here so they
        // cannot disagree -- a disagreement is refused at the worker's dispatch boundary.
        String protocol = ServiceIntrospector.protocolName(serviceInterface);
        return (T) Proxy.newProxyInstance(
                serviceInterface.getClassLoader(),
                new Class<?>[]{serviceInterface},
                new ClientHandler(methods, protocol, version));
    }

    /**
     * Release the underlying {@link HttpClient}, if this connection created it.
     *
     * <p>A client supplied via {@link Builder#httpClient(HttpClient)} is left
     * alone — it belongs to the caller and may be shared with other
     * connections.</p>
     *
     * <p>Every open sticky session is ended first — the whole stack, innermost
     * out — because the worker's side of one holds handles (a cursor, a file, a
     * warm cache) that nothing will release until the token's TTL expires. A
     * session the caller {@linkplain #detachSession() detached} is left alone:
     * it was detached precisely so it would outlive this connection.</p>
     */
    @Override
    public void close() {
        while (session != null) endSession();
        if (!ownsHttpClient) return;
        if (irohHttp != null) {
            try {
                irohHttp.close();
            } catch (IOException e) {
                throw new IllegalStateException("could not close Iroh HTTP transport", e);
            }
            return;
        }
        if (http != null) http.close();
        if (socksHttp != null) {
            try {
                socksHttp.stop();
            } catch (Exception e) {
                throw new IllegalStateException("could not stop SOCKS5h HTTP client", e);
            }
        }
    }

    // ------------------------------------------------------------------
    // Untyped surface
    // ------------------------------------------------------------------

    /**
     * Call a unary method named by string, relaying a caller-supplied batch.
     *
     * <p>The untyped counterpart of a call made through {@link #proxy(Class)},
     * and deliberately the same bytes on the wire: the same request framing,
     * the same {@code {endpoint}/{protocol}/{method}} route, the same read loop
     * — log batches to the {@code onLog} sink, an {@code EXCEPTION} batch
     * raised as {@link RpcError}, an externalized pointer resolved (or refused
     * when this connection has no {@code ExternalLocationConfig}) — differing
     * only in that nothing is decoded into a Java value at either end.</p>
     *
     * <p>Mirrors {@link farm.query.vgirpc.RpcConnection#callUnaryRaw} so a
     * caller that relays somebody else's batches — a gateway, a bridge, a
     * conformance driver — can be written once against both transports.</p>
     *
     * @param protocol the routing key to address; it rides both the path and
     *     {@code vgi_rpc.protocol}, and a worker refuses a call whose two
     *     copies disagree
     * @param protocolVersion the application protocol version to stamp, or
     *     {@code null} to send none
     * @param method the RPC method name
     * @param request the request batch and the Arrow custom metadata to send
     *     with it; the metadata is layered last and relayed verbatim
     * @return the reply batch framed as a one-batch Arrow IPC stream
     * @throws RpcError on a transport failure or a peer-reported error
     */
    public byte[] callUnaryRaw(String protocol, String protocolVersion, String method,
                               AnnotatedBatch request) {
        byte[] response = post(urlFor(protocol, method, ""),
                rawRequestBody(method, protocol, protocolVersion, request), method);
        try (IpcStreamReader r = new IpcStreamReader(
                new ByteArrayInputStream(response), Allocators.root())) {
            while (true) {
                Map<String, String> md = r.readNextBatch();
                if (md == null) {
                    throw new RpcError("ProtocolError",
                            method + ": response stream ended without a result batch", "");
                }
                VectorSchemaRoot root = r.root();
                if (dispatchLogOrError(md, root.getRowCount())) continue;
                if (LocationResolver.isPointer(root.getRowCount(), md)) {
                    try (ResolvedBatch resolved = resolvePointer(md)) {
                        return Wire.writeOneBatch(resolved.root(), resolved.customMetadata(),
                                resolved.dictionaries());
                    }
                }
                return Wire.writeOneBatch(root, md, r.dictionaryProvider());
            }
        } catch (IOException e) {
            throw new RpcError("TransportError", method + ": " + e.getMessage(), "");
        }
    }

    /**
     * Open a streaming call named by string, relaying a caller-supplied batch.
     *
     * <p>Two properties of the method's <em>declaration</em> travel as
     * parameters because neither is recoverable from the wire, and guessing
     * either loses data:</p>
     *
     * <ul>
     *   <li>{@code hasHeader} — a worker writes a header stream ahead of the
     *       body exactly when the method declares one. A client that guesses
     *       wrong reads the header batch as the stream's first data batch.</li>
     *   <li>{@code isExchange} — a producer response carries its continuation
     *       cursor in a zero-row batch <em>after</em> the data, while an
     *       exchange response piggy-backs it on the data batch itself. The two
     *       are byte-identical when an exchange legitimately answers with zero
     *       rows, so the shape decides: a tick skips cursor-only markers, an
     *       exchange takes the first batch as the answer it asked for. It is
     *       never inferred from the method name — a name-prefix heuristic is a
     *       fixture-specific accident that does not survive the next method
     *       somebody adds.</li>
     * </ul>
     *
     * <p>An error raised while opening surfaces on the stream's first
     * {@code tick}/{@code exchange} rather than from this call, unless the
     * method declares a header (in which case the error stream arrives where
     * the header was expected and is raised here). That is the same lazy
     * boundary the typed HTTP stream has: the init response is buffered, and
     * nothing in it is parsed until the caller asks for a batch.</p>
     *
     * @param protocol the routing key to address
     * @param protocolVersion the application protocol version to stamp, or {@code null}
     * @param method the RPC method name
     * @param request the request batch and the Arrow custom metadata to send with it
     * @param hasHeader whether the method declares a {@code @StreamHeader}
     * @param isExchange whether this is an exchange stream rather than a producer
     * @return the open stream
     * @throws RpcError on a transport failure or a peer-reported error while opening
     */
    public RawStream openStreamRaw(String protocol, String protocolVersion, String method,
                                   AnnotatedBatch request, boolean hasHeader, boolean isExchange) {
        byte[] response = post(urlFor(protocol, method, "/init"),
                rawRequestBody(method, protocol, protocolVersion, request), method + "/init");
        // The init response is a *sequence* of IPC streams when the method
        // declares a header: the header stream, then the stream body. One
        // reader per stream, each picking up where the last one's
        // end-of-stream marker left off.
        ByteArrayInputStream in = new ByteArrayInputStream(response);
        try {
            byte[] header = hasHeader ? readRawHeaderStream(in, method) : null;
            HttpRpcStream<StreamState> stream =
                    new HttpRpcStream<>(this, protocol, method, in, null);
            return new HttpRawStream(stream, header, isExchange);
        } catch (IOException e) {
            throw new RpcError("TransportError", method + "/init: " + e.getMessage(), "");
        }
    }

    // ------------------------------------------------------------------
    // Capability discovery, upload URLs, sticky sessions
    // ------------------------------------------------------------------

    /**
     * What this worker advertises on {@code OPTIONS {endpoint}/health}.
     *
     * <p>Discovered once and cached for the life of the connection: the values
     * describe a deployment's configuration, not a request, and re-probing per
     * call would double the request count of every short-lived connection to
     * learn the same answer. The probe is the same one the response-budget
     * contract already performs, so asking for capabilities on a connection
     * that has made a call costs nothing.</p>
     *
     * <p>Failure to reach the endpoint, or a non-2xx answer, is an
     * {@link RpcError} rather than an empty capability set: "the server
     * advertises no limits" and "nobody answered" lead to opposite client
     * decisions, and silently conflating them makes a misconfigured prefix look
     * like a permissive worker.</p>
     *
     * @return the advertised capabilities
     * @throws RpcError if the probe cannot be completed
     */
    public HttpCapabilities capabilities() {
        HttpCapabilities cached = capabilities;
        if (cached != null) return cached;
        synchronized (this) {
            if (capabilities == null) capabilities = parseCapabilities(probeHealth());
            return capabilities;
        }
    }

    /**
     * Ask the worker for pre-signed upload slots for payloads too large to send
     * inline.
     *
     * <p>Goes to the <em>flat</em> {@code {endpoint}/__upload_url__/init}:
     * upload vending is a property of the server, not of any hosted protocol,
     * so namespacing it would be asking the caller to route a thing that is not
     * routed — and the worker's route table does not have it under a protocol
     * to find.</p>
     *
     * <p>A worker configured without an upload-URL provider does not route the
     * endpoint at all, which arrives as an {@code RpcError} typed
     * {@code NotSupported} — the same type the reference client reports, so a
     * caller can branch on "this deployment cannot externalize" without
     * matching on message text or HTTP status.</p>
     *
     * @param count how many slots to request; the worker caps this at
     *     {@link HttpServer#MAX_UPLOAD_URL_COUNT}
     * @return the vended slots, in the order the worker returned them
     * @throws IllegalArgumentException if {@code count} is not positive
     * @throws RpcError if the worker refuses, does not support upload URLs, or
     *     the call fails in transport
     */
    public java.util.List<UploadUrl> requestUploadUrls(int count) {
        if (count <= 0) throw new IllegalArgumentException("count must be positive");
        byte[] body;
        try (VectorSchemaRoot params = VectorSchemaRoot.create(
                HttpServer.UPLOAD_URL_PARAMS_SCHEMA, Allocators.root())) {
            params.allocateNew();
            ((org.apache.arrow.vector.BigIntVector) params.getVector("count")).setSafe(0, count);
            params.setRowCount(1);
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            // No routing key: this endpoint is owned by no protocol, and
            // stamping one would make the metadata disagree with the path.
            ClientMarshalling.writeRawRequest(buf, HttpServer.UPLOAD_URL_METHOD, params,
                    null, null, null, null);
            body = buf.toByteArray();
        } catch (IOException e) {
            throw new RpcError("TransportError",
                    HttpServer.UPLOAD_URL_METHOD + ": could not frame request: " + e.getMessage(), "");
        }
        String what = HttpServer.UPLOAD_URL_METHOD;
        HttpReply reply = postExchange(endpoint + "/" + HttpServer.UPLOAD_URL_METHOD + "/init",
                body, what, false);
        if (reply.status() == 404) {
            throw new RpcError("NotSupported", "Server does not support upload URLs", "");
        }
        byte[] response = arrowBodyOf(reply, what);
        java.util.List<UploadUrl> urls = new java.util.ArrayList<>();
        try (IpcStreamReader r = new IpcStreamReader(
                new ByteArrayInputStream(response), Allocators.root())) {
            while (true) {
                Map<String, String> md = r.readNextBatch();
                if (md == null) break;
                VectorSchemaRoot root = r.root();
                if (dispatchLogOrError(md, root.getRowCount())) continue;
                org.apache.arrow.vector.VarCharVector upload =
                        (org.apache.arrow.vector.VarCharVector) root.getVector("upload_url");
                org.apache.arrow.vector.VarCharVector download =
                        (org.apache.arrow.vector.VarCharVector) root.getVector("download_url");
                org.apache.arrow.vector.TimeStampMicroTZVector expires =
                        (org.apache.arrow.vector.TimeStampMicroTZVector) root.getVector("expires_at");
                for (int i = 0; i < root.getRowCount(); i++) {
                    urls.add(new UploadUrl(
                            upload.isNull(i) ? null : new String(upload.get(i), StandardCharsets.UTF_8),
                            download.isNull(i) ? null : new String(download.get(i), StandardCharsets.UTF_8),
                            // Whole seconds: the wire carries microseconds, but
                            // sub-second precision on a deadline minutes away is
                            // noise, and floor keeps a slot from looking valid
                            // for a microsecond longer than it is.
                            expires.isNull(i) ? 0L : Math.floorDiv(expires.get(i), 1_000_000L)));
                }
            }
        } catch (IOException e) {
            throw new RpcError("TransportError", what + ": " + e.getMessage(), "");
        }
        return java.util.List.copyOf(urls);
    }

    /**
     * Open a sticky-session scope: from here until {@link #endSession()}, every
     * request on this connection carries {@code VGI-Session-Accept: true}, the
     * session token once the worker mints one, and any {@code VGI-Echo-*}
     * values the worker asked to have echoed back.
     *
     * <p>The opt-in header is the leak-prevention half of the contract, not a
     * formality: a worker will not open a session for a client that has not
     * asked for one, because a session it opened unilaterally would be a
     * server-side resource — a cursor, a file handle, a warm cache — that no
     * client knows to release, evicted only by its TTL.</p>
     *
     * <h4>Scopes nest</h4>
     *
     * <p>Opening a scope while another is open <em>pushes</em>: the inner scope
     * takes over the connection's requests, and {@link #endSession()} restores
     * the enclosing one. Replacing the outer scope instead — or clearing the
     * single scope on exit — makes the inner block's exit silently unsession
     * every later call on the outer one, which the worker answers by refusing
     * to find the state that call was about ("no sticky counter bound to this
     * request"). It is a failure that appears only <em>after</em> the nested
     * block, in code that never mentioned a session, which is why it is worth
     * designing out rather than documenting.</p>
     *
     * <p>The reference client models concurrent scopes rather than nested ones:
     * each of its {@code with_session_token()} blocks carries its own token on
     * its own view, and two may be live at once without either shadowing the
     * other. That shape is not expressible here, because the surface a
     * conformance driver drives — {@code session_begin} / {@code session_end}
     * — is connection-scoped and carries no scope id, so "which scope does this
     * request belong to" has exactly one answer: the innermost. Strict nesting
     * is the most faithful reading of that surface, and it is the only shape
     * the suite exercises.</p>
     *
     * <p>Passing a token resumes a session opened earlier (possibly by another
     * process). Passing {@code null} or a blank string lets the worker mint
     * one.</p>
     *
     * @param tokenOrNull an existing session token to resume, or {@code null}
     *     to let the worker mint one
     */
    public void beginSession(String tokenOrNull) {
        SessionState state = new SessionState();
        state.token = tokenOrNull == null || tokenOrNull.isBlank() ? null : tokenOrNull;
        synchronized (this) {
            sessions.push(state);
            session = state;
        }
    }

    /**
     * The session token currently in flight.
     *
     * @return the token, or {@code null} when no session is open, the worker
     *     minted none, or it has since closed the session
     */
    public String currentSessionToken() {
        SessionState state = session;
        if (state == null) return null;
        synchronized (state) {
            return state.token;
        }
    }

    /**
     * The {@code VGI-Echo-*} values captured when the session opened, keyed
     * without the prefix.
     *
     * <p>A client stashing a token for later resumption should stash these too:
     * on platforms whose routing depends on a client-supplied header (a
     * Fly.io instance id, a sticky backend name) a resumed session without them
     * lands on the wrong node, and there is no {@code Set-Cookie} analogue for
     * headers to carry them automatically.</p>
     *
     * @return an immutable snapshot; empty when there are none
     */
    public Map<String, String> currentEchoHeaders() {
        SessionState state = session;
        if (state == null) return Map.of();
        synchronized (state) {
            return Map.copyOf(state.echoHeaders);
        }
    }

    /**
     * Hand the session token to the caller and suppress the exit-time delete.
     *
     * <p>Without this, {@link #endSession()} releases the worker's session
     * state promptly — which is the right default, because the state holds
     * handles. A caller stashing the token to resume the same session later
     * must say so, or the token it stashed names a session that was deleted the
     * moment this connection finished with it.</p>
     *
     * @return the detached token, or {@code null} if no session was live
     */
    public String detachSession() {
        SessionState state = session;
        if (state == null) return null;
        synchronized (state) {
            String token = state.token;
            state.token = null;
            state.detached = true;
            return token;
        }
    }

    /**
     * End the innermost session scope, releasing the worker's session state
     * with a best-effort {@code DELETE {endpoint}/__session__}, and restore the
     * scope that enclosed it.
     *
     * <p>Only the popped scope is released. An enclosing scope keeps its own
     * token and echo headers and resumes carrying them on the next request —
     * the whole point of {@linkplain #beginSession(String) nesting}, since the
     * outer block's later calls are about the outer session and the worker will
     * refuse to serve them unsessioned.</p>
     *
     * <p>Best-effort because the caller has already decided it is done: a
     * network failure on the release must not become an exception it has to
     * handle, and the worker's TTL reclaims the state regardless. Nothing is
     * sent when the session was {@link #detachSession() detached}, when the
     * worker already announced the session closed, or when no token was ever
     * minted. With no scope open it is a no-op.</p>
     */
    public void endSession() {
        SessionState state;
        synchronized (this) {
            state = sessions.poll();
            session = sessions.peek();
        }
        if (state == null) return;
        String token;
        Map<String, String> echo;
        synchronized (state) {
            if (state.detached || state.closedByServer || state.token == null) return;
            token = state.token;
            echo = Map.copyOf(state.echoHeaders);
        }
        Map<String, String> requestHeaders = new LinkedHashMap<>();
        requestHeaders.put(StickyHeaders.SESSION_ACCEPT, "true");
        requestHeaders.put(StickyHeaders.SESSION, token);
        echo.forEach(requestHeaders::putIfAbsent);
        headers.forEach(requestHeaders::putIfAbsent);
        try {
            execute("DELETE", endpoint + "/" + StickyHeaders.SESSION_PATH, requestHeaders,
                    new byte[0], "session delete", 64L << 10, (h, status, what) -> 64L << 10);
        } catch (RuntimeException ignore) {
            // The worker's TTL is the safety net; a failed release is not the
            // caller's problem to handle.
        }
    }

    // ------------------------------------------------------------------
    // Internals shared with HttpRpcStream
    // ------------------------------------------------------------------

    Consumer<Message> onLog() { return onLog; }

    /**
     * Build the URL for one RPC call: {@code {endpoint}/{protocol}/{method}{suffix}}.
     *
     * <p>Namespaced by protocol so an edge device can act on the routing key without an Arrow
     * parser, and so co-hosted protocols -- reflection, identity -- are reachable over HTTP at
     * all rather than only on the raw transports. The single source of truth for the route shape,
     * shared with {@link HttpRpcStream}'s continuations.
     *
     * <p>Server-level reserved endpoints ({@code __upload_url__}) are owned by no protocol and
     * stay flat; they are built at their own call sites.
     *
     * @param protocol the routing key of the hosted protocol
     * @param method the RPC method name
     * @param suffix {@code ""}, {@code "/init"} or {@code "/exchange"}
     * @return the absolute request URL
     */
    String urlFor(String protocol, String method, String suffix) {
        return endpoint + "/" + protocol + "/" + method + suffix;
    }


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
        return arrowBodyOf(postExchange(url, body, what), what);
    }

    /**
     * POST and hand back the whole reply — status, headers and decoded body —
     * without judging whether the body is Arrow.
     *
     * <p>Separate from {@link #post} because two callers need the status: the
     * upload-URL endpoint, whose absence is a 404 that means "this deployment
     * cannot externalize" rather than "the call failed", and any future probe
     * with the same shape. Everything else — the response budget, request
     * compression, session headers, content decoding — happens here exactly
     * once, so no second code path can be written that forgets one of them.</p>
     */
    private HttpReply postExchange(String url, byte[] body, String what) {
        return postExchange(url, body, what, true);
    }

    /**
     * @param externalizable whether an oversized body may be replaced by a
     *     pointer. False for {@code __upload_url__} itself, which is how the
     *     pointer is obtained and therefore cannot be one
     */
    private HttpReply postExchange(String url, byte[] body, String what, boolean externalizable) {
        ensureResponseBudgetSupport();
        byte[] outgoing = externalizable ? maybeExternalizeRequest(body, what) : body;
        HttpReply reply = send(url, outgoing, what);
        if (reply.status() == 413 && externalizable && outgoing == body) {
            // The worker enforces a cap it did not advertise — or advertised one
            // this connection had not read when the body was framed. Its refusal
            // is the discovery, so externalize and send once more.
            //
            // Safe to send again where an ordinary retry would not be: a 413 is
            // a refusal to read the body at all, so the method never ran and no
            // side effect it might have had can be repeated. And it is one more
            // attempt, not a loop — a second 413 is the worker saying something
            // this client cannot fix by moving the bytes elsewhere.
            reply = send(url, externalizeRequest(body, what), what);
        }
        return reply;
    }

    /** One POST: compression, session capture and content decoding around {@link #execute}. */
    private HttpReply send(String url, byte[] body, String what) {
        String encoding = requestEncoding();
        byte[] payload = encoding == null ? body : compressRequest(body, encoding, what);
        HttpReply reply = execute("POST", url, postHeaders(encoding), payload, what,
                effectiveDecodedResponseLimit(null), this::responseLimit);
        captureSessionHeaders(reply);
        return reply.withBody(decodeBody(reply, responseLimit(reply.headers(), reply.status(), what), what));
    }

    /** Insist the reply is an Arrow IPC stream, translating every other shape. */
    private static byte[] arrowBodyOf(HttpReply reply, String what) {
        return requireArrowBody(reply.status(), reply.first(HttpHeaders.CONTENT_TYPE), reply.body(), what);
    }

    /**
     * The decoded-byte ceiling for one response, validated against the
     * response's own headers.
     *
     * <p>Evaluated from the headers <em>before</em> the body is read, which is
     * what makes the cap a refusal rather than an autopsy: a body the client
     * will not accept is never fully buffered. Run again after the body arrives
     * to bound decompression, since a compressed body under the cap can expand
     * past it.</p>
     */
    private long responseLimit(Map<String, List<String>> responseHeaders, int status, String what) {
        requireExactBudgetSupport(
                valuesOf(responseHeaders, HttpServer.ACCEPT_MAX_RESPONSE_BYTES_SUPPORT_HEADER), what, status);
        Long currentAdvertised = parseAdvertisedMaxResponseBytes(
                valuesOf(responseHeaders, HttpServer.MAX_RESPONSE_BYTES_HEADER), what, status);
        return effectiveDecodedResponseLimit(currentAdvertised);
    }

    /**
     * One HTTP round trip's outcome. Header lookup is case-insensitive because
     * the three transports below hand their headers back with three different
     * ideas of case, and a session or capability header missed on one of them
     * is a feature that silently does not work there.
     */
    private record HttpReply(int status, Map<String, List<String>> headers, byte[] body) {

        List<String> values(String name) {
            List<String> found = headers.get(name);
            return found == null ? List.of() : found;
        }

        String first(String name) {
            List<String> found = values(name);
            return found.isEmpty() ? null : found.getFirst();
        }

        HttpReply withBody(byte[] replacement) {
            return replacement == body ? this : new HttpReply(status, headers, replacement);
        }
    }

    /** Computes a response's byte ceiling once its headers are known. */
    private interface LimitPolicy {
        long limitFor(Map<String, List<String>> headers, int status, String what);
    }

    private static Map<String, List<String>> caseInsensitive(Map<String, List<String>> raw) {
        Map<String, List<String>> out = new java.util.TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        raw.forEach((name, values) -> out.put(name, values == null ? List.of() : List.copyOf(values)));
        return out;
    }

    private static List<String> valuesOf(Map<String, List<String>> headers, String name) {
        List<String> found = headers.get(name);
        return found == null ? List.of() : found;
    }

    /**
     * Issue one HTTP request over whichever of the three clients this
     * connection was built with.
     *
     * <p>The three differ only in how bytes reach the wire; everything that is
     * protocol — which headers ride, what the byte ceiling is, what a failure
     * turns into — is decided by the caller and applied identically to all of
     * them. That is deliberate: the SOCKS and Iroh paths exist for deployments
     * nobody runs the test suite against, so any behaviour that lives only in
     * one of them is behaviour that is never exercised until it breaks in
     * production.</p>
     *
     * @param preflightLimit ceiling applied by transports that must be told the
     *     bound before the response exists (Iroh), where headers cannot be
     *     inspected first
     */
    private HttpReply execute(String method, String url, Map<String, String> requestHeaders, byte[] body,
                              String what, long preflightLimit, LimitPolicy policy) {
        if (irohHttp != null) {
            return executeIroh(method, url, requestHeaders, body, what, preflightLimit, policy);
        }
        if (socksHttp != null) return executeSocks(method, url, requestHeaders, body, what, policy);
        return executeJdk(method, url, requestHeaders, body, what, policy);
    }

    private HttpReply executeJdk(String method, String url, Map<String, String> requestHeaders,
                                 byte[] body, String what, LimitPolicy policy) {
        HttpRequest.Builder req = HttpRequest.newBuilder(URI.create(url));
        req.method(method, body.length == 0
                ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofByteArray(body));
        if (requestTimeout != null) req.timeout(requestTimeout);
        requestHeaders.forEach(req::header);

        HttpResponse<InputStream> resp;
        try {
            resp = http.send(req.build(), HttpResponse.BodyHandlers.ofInputStream());
        } catch (IOException e) {
            throw new RpcError("TransportError", what + ": " + e, "");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RpcError("TransportError", what + ": interrupted", "");
        }
        Map<String, List<String>> responseHeaders = caseInsensitive(resp.headers().map());
        try (InputStream input = resp.body()) {
            byte[] responseBody = readBounded(input,
                    policy.limitFor(responseHeaders, resp.statusCode(), what), what);
            return new HttpReply(resp.statusCode(), responseHeaders, responseBody);
        } catch (IOException e) {
            throw new RpcError("TransportError", what + ": " + e, "");
        }
    }

    private HttpReply executeIroh(String method, String url, Map<String, String> requestHeaders,
                                  byte[] body, String what, long preflightLimit, LimitPolicy policy) {
        Map<String, List<String>> outgoing = new LinkedHashMap<>();
        requestHeaders.forEach((name, value) -> outgoing.put(name, List.of(value)));
        IrohHttpResponse response;
        try {
            response = irohHttp.execute(new IrohHttpRequest(method, URI.create(url).getRawPath(),
                    outgoing, body, effectiveRequestTimeout(), preflightLimit));
        } catch (IOException e) {
            throw transportRpcError(what, e);
        }
        Map<String, List<String>> responseHeaders = caseInsensitive(response.headers());
        long limit = policy.limitFor(responseHeaders, response.status(), what);
        byte[] responseBody = response.body();
        if (responseBody.length > limit) {
            throw new RpcError("ResponseTooLargeError",
                    what + " exceeds max_response_bytes (" + responseBody.length + " > " + limit + ")", "");
        }
        return new HttpReply(response.status(), responseHeaders, responseBody);
    }

    private HttpReply executeSocks(String method, String url, Map<String, String> requestHeaders,
                                   byte[] body, String what, LimitPolicy policy) {
        var request = socksHttp.newRequest(URI.create(url)).method(method);
        request.headers(fields -> requestHeaders.forEach(fields::put));
        if (body.length > 0) {
            request.body(new BytesRequestContent(
                    requestHeaders.getOrDefault(HttpHeaders.CONTENT_TYPE, HttpServer.ARROW_CONTENT_TYPE),
                    body));
        }
        if (requestTimeout != null) {
            request.timeout(requestTimeout.toNanos(), TimeUnit.NANOSECONDS);
        }
        try {
            InputStreamResponseListener listener = new InputStreamResponseListener();
            request.send(listener);
            Response response = awaitHeaders(listener);
            Map<String, List<String>> responseHeaders = new java.util.TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            for (String name : response.getHeaders().getFieldNamesCollection()) {
                responseHeaders.put(name, List.copyOf(response.getHeaders().getValuesList(name)));
            }
            try (InputStream input = listener.getInputStream()) {
                byte[] responseBody = readBounded(input,
                        policy.limitFor(responseHeaders, response.getStatus(), what), what);
                return new HttpReply(response.getStatus(), responseHeaders, responseBody);
            }
        } catch (IOException e) {
            throw new RpcError("TransportError", what + ": " + e, "");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RpcError("TransportError", what + ": interrupted", "");
        } catch (TimeoutException e) {
            throw new RpcError("TransportError", what + ": timed out", "");
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            throw new RpcError("TransportError", what + ": " + cause, "");
        }
    }

    // ------------------------------------------------------------------
    // Request shaping: headers, compression, sessions
    // ------------------------------------------------------------------

    /**
     * Headers common to every request this connection makes, in precedence
     * order: framework defaults, then the caller's static headers, then the
     * session's — which are last because a session that does not announce
     * itself is not a session, and a caller cannot usefully override the token
     * the worker minted.
     */
    private Map<String, String> commonHeaders(Map<String, String> framework) {
        Map<String, String> out = new LinkedHashMap<>(framework);
        if (acceptedMaxResponseBytes != null) {
            out.put(HttpServer.ACCEPT_MAX_RESPONSE_BYTES_HEADER, Long.toString(acceptedMaxResponseBytes));
        }
        headers.forEach(out::put);
        applySessionHeaders(out);
        return out;
    }

    /** Headers for a POST carrying an Arrow body, with the codec choice applied. */
    private Map<String, String> postHeaders(String requestEncoding) {
        Map<String, String> framework = new LinkedHashMap<>();
        framework.put(HttpHeaders.CONTENT_TYPE, HttpServer.ARROW_CONTENT_TYPE);
        if (requestEncoding != null) {
            framework.put(HttpHeaders.CONTENT_ENCODING, requestEncoding);
            // Having asked the server to decompress, accept a compressed answer
            // too: a client that compresses uphill and refuses it downhill is
            // paying for CPU on the half of the exchange that is usually the
            // smaller one.
            framework.put(HttpHeaders.ACCEPT_ENCODING, DECODABLE_RESPONSE_ENCODINGS);
        } else {
            // Explicitly opt out of response compression. The server treats an
            // identity-first accept list as a per-request "compression off"
            // switch, so this is a supported choice and not a gap the server
            // has to guess at.
            framework.put(HttpHeaders.ACCEPT_ENCODING, MediaTypes.IDENTITY);
        }
        return commonHeaders(framework);
    }

    /**
     * The codec for this request's body, or {@code null} to send it as-is.
     *
     * <p>Compression is off unless {@link Builder#compressionLevel(Integer)}
     * turned it on, and even then it is only applied when the worker
     * <em>advertises</em> the codec. Compressing regardless would turn a
     * capability mismatch into a 415 on every call — and the advertisement
     * distinguishes a worker that speaks no compression (present-but-empty
     * header) from one built before the header existed (absent, so zstd),
     * which is exactly the case a client cannot guess.</p>
     */
    private String requestEncoding() {
        if (compressionLevel == null) return null;
        HttpCapabilities caps = capabilities();
        return caps.supports(MediaTypes.ZSTD) ? MediaTypes.ZSTD : null;
    }

    private byte[] compressRequest(byte[] body, String encoding, String what) {
        if (!MediaTypes.ZSTD.equals(encoding)) return body;
        try {
            return Zstd.compress(body, compressionLevel);
        } catch (RuntimeException e) {
            throw new RpcError("TransportError", what + ": could not compress request body: " + e, "");
        }
    }

    /**
     * Decode a response body per its content coding, bounded by the same
     * ceiling the encoded bytes were read under.
     *
     * <p>The bound has to be re-applied after decompression, not just before:
     * the cap is on what the client will hold in memory, and a few hundred
     * compressed kilobytes can expand past any of them.</p>
     */
    private static byte[] decodeBody(HttpReply reply, long limit, String what) {
        String encoding = reply.first(HttpHeaders.CONTENT_ENCODING);
        if (encoding == null || encoding.isBlank()) {
            // A client whose fetch/proxy layer mangles standard content coding
            // gets the codec on the custom header instead; honour both, since
            // an intermediary may have rewritten one into the other.
            encoding = reply.first(HttpHeaders.X_VGI_CONTENT_ENCODING);
        }
        if (encoding == null || encoding.isBlank()) return reply.body();
        try {
            byte[] decoded = ContentCodec.decode(reply.body(), encoding, limit);
            if (decoded.length > limit) {
                throw new RpcError("ResponseTooLargeError",
                        what + " exceeds max_response_bytes (" + decoded.length + " > " + limit + ")", "");
            }
            return decoded;
        } catch (ContentCodec.OutputTooLargeException e) {
            throw new RpcError("ResponseTooLargeError",
                    what + " exceeds max_response_bytes (" + e.getMessage() + ")", "");
        } catch (IOException e) {
            throw new RpcError("TransportError",
                    what + ": could not decode a " + encoding + " response body: " + e.getMessage(), "");
        }
    }

    // ------------------------------------------------------------------
    // Request externalization (the client-to-server half of the protocol)
    // ------------------------------------------------------------------

    /**
     * Replace an oversized request body with a pointer to storage, when the
     * worker has advertised a cap this body exceeds.
     *
     * <p>A worker refuses an inline body over its {@code VGI-Max-Request-Bytes}
     * with a 413 and no way to recover the call, so the client is the only
     * party that can act: it asks for an upload slot, PUTs the batch there, and
     * sends a zero-row pointer batch naming the download URL in its place. The
     * worker fetches the real batch behind it. Without this, the only way to
     * call a method with a large argument is to hope the deployment set no cap.
     *
     * <p>No speculative probe: if the capability set is not already known this
     * returns the body unchanged and the worker's own 413 does the discovery.
     * A capability probe on the off-chance that a body <em>might</em> be too
     * large would add a round trip to every small call ever made.
     */
    private byte[] maybeExternalizeRequest(byte[] body, String what) {
        HttpCapabilities caps = capabilities;
        if (caps == null && healthProbe != null) {
            // The probe already happened for the response budget; parsing what
            // it said costs nothing and buys pre-emption from the first call.
            caps = capabilities();
        }
        if (caps == null) return body;
        Long max = caps.maxRequestBytes();
        if (max == null || !caps.uploadUrlSupport() || body.length <= max) return body;
        return externalizeRequest(body, what);
    }

    /**
     * Upload {@code body} to a worker-vended slot and return the pointer batch
     * that replaces it on the wire.
     *
     * <p>Fails loudly when the worker vends no upload URLs: the caller's
     * request cannot be delivered either way, and {@code RequestTooLarge} says
     * what is wrong where an opaque 413 does not.</p>
     *
     * <p>The uploaded object is the framed request stream <em>uncompressed</em>,
     * and the pointer carries its SHA-256 so the worker can prove that what it
     * fetched is what was sent — the bytes take a path through third-party
     * storage that the RPC connection itself never sees.</p>
     */
    private byte[] externalizeRequest(byte[] body, String what) {
        HttpCapabilities caps = capabilities();
        if (!caps.uploadUrlSupport()) {
            throw new RpcError("RequestTooLarge",
                    "Request exceeds max_request_bytes and the server does not advertise "
                            + "upload_url_support", "");
        }
        java.util.List<UploadUrl> urls = requestUploadUrls(1);
        if (urls.isEmpty()) throw new RpcError("ProtocolError", "Server returned no upload URLs", "");
        UploadUrl slot = urls.getFirst();
        validateExternalUrl(slot.uploadUrl());
        validateExternalUrl(slot.downloadUrl());
        HttpReply put = execute("PUT", slot.uploadUrl(), uploadHeaders(slot.uploadUrl()), body,
                what + " (upload)", 64L << 10, (h, status, w) -> 64L << 10);
        if (put.status() < 200 || put.status() >= 300) {
            throw new RpcError("ExternalUploadFailed",
                    "PUT to upload URL failed: HTTP " + put.status(), "");
        }
        return pointerRequestBody(body, slot.downloadUrl(), what);
    }

    /**
     * Headers for the upload PUT.
     *
     * <p>This request leaves the RPC protocol: the target is storage, and a
     * signed URL carries its own authorization in the URL itself. So the
     * connection's own credentials ride only when the slot is on the
     * <em>same origin</em> as the worker — where they were going anyway, and
     * where an in-process blob endpoint may well require them. Sending a bearer
     * token to whatever third-party host a worker happens to name would hand
     * that host a credential for the worker, and some object stores reject a
     * request that carries an {@code Authorization} header beside a signed
     * query string. Session headers never ride: they are not this exchange's.
     */
    private Map<String, String> uploadHeaders(String uploadUrl) {
        Map<String, String> out = new LinkedHashMap<>();
        out.put(HttpHeaders.CONTENT_TYPE, HttpServer.ARROW_CONTENT_TYPE);
        if (sameOrigin(uploadUrl, endpoint)) headers.forEach(out::put);
        return out;
    }

    private static boolean sameOrigin(String a, String b) {
        try {
            URI left = URI.create(a);
            URI right = URI.create(b);
            return left.getScheme() != null && left.getScheme().equalsIgnoreCase(right.getScheme())
                    && left.getHost() != null && left.getHost().equalsIgnoreCase(right.getHost())
                    && left.getPort() == right.getPort();
        } catch (RuntimeException notAUrl) {
            return false;
        }
    }

    /** Apply the caller's URL policy, when one was configured, to a vended URL. */
    private void validateExternalUrl(String url) {
        if (externalConfig == null) return;
        java.util.function.Consumer<URI> validator = externalConfig.urlValidator();
        if (validator == null) return;
        try {
            validator.accept(URI.create(url));
        } catch (RuntimeException refused) {
            throw new RpcError("ExternalLocationError",
                    "the worker vended an upload URL this client's policy refuses ("
                            + LocationResolver.redactUrl(url) + ")", "");
        }
    }

    /**
     * Rewrite a framed request as a zero-row pointer batch naming
     * {@code locationUrl}.
     *
     * <p>The outer batch keeps the request's own metadata — the method, the
     * routing key, the version, and on a stream turn the cursor and call tokens
     * — because that is what the worker dispatches on <em>before</em> it
     * resolves the pointer. Replacing the payload must not replace the
     * envelope.</p>
     */
    private byte[] pointerRequestBody(byte[] body, String locationUrl, String what) {
        try (IpcStreamReader r = new IpcStreamReader(
                new ByteArrayInputStream(body), Allocators.root())) {
            Map<String, String> md = r.readNextBatch();
            if (md == null) {
                throw new RpcError("ProtocolError",
                        what + ": request body carried no batch to externalize", "");
            }
            Schema schema = r.root().getSchema();
            Map<String, String> merged = new LinkedHashMap<>(md);
            merged.put(Metadata.LOCATION, locationUrl);
            merged.put(Metadata.LOCATION_SHA256, sha256Hex(body));
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            try (IpcStreamWriter w = new IpcStreamWriter(buf)) {
                w.writeSchema(schema);
                Wire.writeZeroBatch(w, schema, merged);
            }
            return buf.toByteArray();
        } catch (IOException e) {
            throw new RpcError("TransportError",
                    what + ": could not build the pointer request: " + e.getMessage(), "");
        }
    }

    private static String sha256Hex(byte[] data) {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is required of every JVM", impossible);
        }
        byte[] hash = digest.digest(data);
        StringBuilder out = new StringBuilder(hash.length * 2);
        for (byte b : hash) {
            out.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
        }
        return out.toString();
    }

    /** Sticky-session state for this connection; {@code null} when no scope is open. */
    private static final class SessionState {
        String token;
        final Map<String, String> echoHeaders = new LinkedHashMap<>();
        /** The caller took ownership of the token; the scope must not delete it. */
        boolean detached;
        /** The worker announced the session closed, so there is nothing left to delete. */
        boolean closedByServer;
    }

    private void applySessionHeaders(Map<String, String> target) {
        SessionState state = session;
        if (state == null) return;
        synchronized (state) {
            target.put(StickyHeaders.SESSION_ACCEPT, "true");
            if (state.token != null) target.put(StickyHeaders.SESSION, state.token);
            // Echo headers only fill gaps: a caller that set one by hand has a
            // reason (forcing a node during a drain, say) and must win.
            state.echoHeaders.forEach(target::putIfAbsent);
        }
    }

    /**
     * Harvest session state from a response.
     *
     * <p>Done on every response rather than only on the session-opening one:
     * the worker may rotate the token, and it announces a closed session
     * whenever it decides the session is over — on a response to a call that
     * had nothing to do with opening it.</p>
     */
    private void captureSessionHeaders(HttpReply reply) {
        SessionState state = session;
        if (state == null) return;
        synchronized (state) {
            String token = reply.first(StickyHeaders.SESSION);
            if (token != null && !token.isBlank()) state.token = token;
            int prefixLength = StickyHeaders.ECHO_PREFIX.length();
            reply.headers().forEach((name, values) -> {
                if (values.isEmpty()) return;
                if (name.regionMatches(true, 0, StickyHeaders.ECHO_PREFIX, 0, prefixLength)) {
                    state.echoHeaders.put(name.substring(prefixLength), values.getFirst());
                }
            });
            String close = reply.first(StickyHeaders.SESSION_CLOSE);
            if (close != null && "true".equalsIgnoreCase(close.trim())) {
                state.token = null;
                state.echoHeaders.clear();
                state.closedByServer = true;
            }
        }
    }

    // ------------------------------------------------------------------
    // Response validation and capability discovery
    // ------------------------------------------------------------------

    private static byte[] requireArrowBody(int status, String contentType, byte[] body, String what) {
        if (contentType == null) contentType = "";
        if (contentType.startsWith(HttpServer.ARROW_CONTENT_TYPE)) {
            // 200 or 500 alike: the body is a well-formed Arrow stream and any
            // error is in it. HttpServer answers a failed stream turn with 500
            // plus an error stream, so status alone is not the signal.
            return body;
        }
        String detail = preview(body);
        if (status == 401) {
            throw new RpcError("AuthenticationError",
                    what + ": unauthorized (HTTP 401)"
                            + (detail.isEmpty() ? "" : " — " + detail), "", "", "unauthorized");
        }
        throw new RpcError("HttpError",
                what + ": HTTP " + status + " with a non-Arrow body"
                        + (detail.isEmpty() ? "" : " — " + detail), "");
    }

    private static byte[] readBounded(InputStream input, long limit, String what) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream((int) Math.min(limit, 64L << 10));
        byte[] chunk = new byte[8192];
        long total = 0;
        int n;
        while ((n = input.read(chunk)) != -1) {
            total += n;
            if (total > limit) {
                throw new RpcError("ResponseTooLargeError",
                        what + " exceeds max_response_bytes (" + total + " > " + limit + ")", "");
            }
            out.write(chunk, 0, n);
        }
        return out.toByteArray();
    }

    private static void requireExactBudgetSupport(java.util.List<String> values,
                                                   String what, int status) {
        if (values.size() != 1 || !"true".equals(values.getFirst())) {
            throw new RpcError("ProtocolError",
                    what + ": response does not advertise exactly one "
                            + HttpServer.ACCEPT_MAX_RESPONSE_BYTES_SUPPORT_HEADER + ": true",
                    "", "", "http_" + status);
        }
    }

    private Response awaitHeaders(InputStreamResponseListener listener)
            throws InterruptedException, TimeoutException, ExecutionException {
        long timeoutNanos = requestTimeout == null ? Long.MAX_VALUE : requestTimeout.toNanos();
        return listener.get(timeoutNanos, TimeUnit.NANOSECONDS);
    }

    /**
     * Probe {@code OPTIONS {endpoint}/health} once and cache the reply.
     *
     * <p>{@code /health} is the discovery target because it is mandatory in
     * every implementation and exempt from auth, while the capability headers
     * themselves ride every response. One probe serves both readers: the
     * response-budget contract below, which is strict, and
     * {@link #capabilities()}, which is not — so a connection never pays for
     * two OPTIONS requests that would ask the same question.</p>
     */
    private HttpReply probeHealth() {
        HttpReply cached = healthProbe;
        if (cached != null) return cached;
        synchronized (this) {
            if (healthProbe != null) return healthProbe;
            HttpReply reply = execute("OPTIONS", endpoint + "/health", commonHeaders(Map.of()),
                    new byte[0], "OPTIONS /health", 64L << 10, (h, status, what) -> 64L << 10);
            if (reply.status() < 200 || reply.status() >= 300) {
                // Named in terms of the budget contract when one is in play:
                // that is the caller whose request is about to be refused, and
                // "/health answered 502" is a symptom, not the rule it broke.
                throw new RpcError("ProtocolError",
                        acceptedMaxResponseBytes != null
                                ? "server does not advertise "
                                        + HttpServer.ACCEPT_MAX_RESPONSE_BYTES_SUPPORT_HEADER
                                        + ": true (HTTP " + reply.status() + ")"
                                : "capability discovery (OPTIONS /health) failed with HTTP "
                                        + reply.status(), "");
            }
            captureSessionHeaders(reply);
            healthProbe = reply;
            return reply;
        }
    }

    /**
     * Read the advertised capabilities out of a probe response.
     *
     * <p>Lenient on purpose, and the opposite of
     * {@link #parseAdvertisedMaxResponseBytes}: a malformed cap here means the
     * client does not know that limit, which is survivable, whereas a malformed
     * cap on a live response means the client cannot tell whether it is about
     * to exceed one, which is not.</p>
     */
    private static HttpCapabilities parseCapabilities(HttpReply reply) {
        return new HttpCapabilities(
                "true".equals(reply.first(StickyHeaders.STICKY_ENABLED)),
                parseIntOrNull(reply.first(StickyHeaders.STICKY_TTL)),
                parseNameList(reply.first(StickyHeaders.STICKY_ECHO)),
                "true".equals(reply.first(HttpServer.UPLOAD_URL_HEADER)),
                parseLongOrNull(reply.first(HttpServer.MAX_REQUEST_BYTES_HEADER)),
                parseLongOrNull(reply.first(HttpServer.MAX_RESPONSE_BYTES_HEADER)),
                parseLongOrNull(reply.first(HttpServer.MAX_EXTERNALIZED_RESPONSE_BYTES_HEADER)),
                "true".equals(reply.first(HttpServer.EXTERNALIZATION_ENABLED_HEADER)),
                parseLongOrNull(reply.first(HttpServer.MAX_UPLOAD_BYTES_HEADER)),
                parseSupportedEncodings(reply.first(HttpServer.SUPPORTED_ENCODINGS_HEADER)));
    }

    /**
     * Parse {@code VGI-Supported-Encodings}, where absent and empty mean
     * opposite things.
     *
     * <p>Absent is a worker predating the header, and every one of those
     * decodes zstd — so it reads as {@code ["zstd"]}. Present-but-empty is a
     * worker positively stating it speaks no compression, and reads as the
     * empty list. Collapsing the two either sends zstd to a server that answers
     * 415, or refuses to compress for every worker built before the header.</p>
     */
    private static List<String> parseSupportedEncodings(String raw) {
        if (raw == null) return List.of(MediaTypes.ZSTD);
        if (raw.isBlank()) return List.of();
        List<String> parsed = new java.util.ArrayList<>();
        for (String token : raw.split(",")) {
            String name = token.trim().toLowerCase(java.util.Locale.ROOT);
            // Strip an RFC 9110 q-value: the preference order is the server's
            // to state, and this client does not rank what it is offered.
            int semicolon = name.indexOf(';');
            if (semicolon >= 0) name = name.substring(0, semicolon).trim();
            if (!name.isEmpty() && !parsed.contains(name)) parsed.add(name);
        }
        // An advertisement of codecs none of which we recognise is treated the
        // same as no advertisement: it came from a worker newer than this
        // client, not from one that speaks nothing.
        return parsed.isEmpty() ? List.of(MediaTypes.ZSTD) : List.copyOf(parsed);
    }

    private static List<String> parseNameList(String raw) {
        if (raw == null || raw.isBlank()) return List.of();
        List<String> names = new java.util.ArrayList<>();
        for (String token : raw.split(",")) {
            String name = token.trim();
            if (!name.isEmpty()) names.add(name);
        }
        return List.copyOf(names);
    }

    private static Long parseLongOrNull(String raw) {
        if (raw == null || raw.isBlank()) return null;
        try {
            return Long.valueOf(raw.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Integer parseIntOrNull(String raw) {
        if (raw == null || raw.isBlank()) return null;
        try {
            return Integer.valueOf(raw.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * Establish, once, that the worker honours the response-budget contract
     * this client is about to rely on.
     *
     * <p>Only when a cap is actually advertised: a client that states no
     * ceiling has nothing to be broken by a worker that ignores the header.
     * When it does state one, a worker that silently ignores it would stream
     * back an unbounded body, which is the failure the cap exists to prevent —
     * so the absence of the acknowledgement is a hard error rather than a
     * downgrade.</p>
     */
    private void ensureResponseBudgetSupport() {
        if (acceptedMaxResponseBytes == null || responseBudgetSupportVerified) return;
        synchronized (this) {
            if (responseBudgetSupportVerified) return;
            HttpReply probe = probeHealth();
            requireExactBudgetSupport(probe.values(HttpServer.ACCEPT_MAX_RESPONSE_BYTES_SUPPORT_HEADER),
                    "OPTIONS /health", probe.status());
            advertisedMaxResponseBytes = parseAdvertisedMaxResponseBytes(
                    probe.values(HttpServer.MAX_RESPONSE_BYTES_HEADER),
                    "OPTIONS /health", probe.status());
            responseBudgetSupportVerified = true;
        }
    }

    private Duration effectiveRequestTimeout() {
        return requestTimeout == null ? Duration.ofDays(3650) : requestTimeout;
    }

    private static RpcError transportRpcError(String what, IOException cause) {
        RpcError error = new RpcError("TransportError", what + ": " + cause, "");
        error.initCause(cause);
        return error;
    }

    private long effectiveDecodedResponseLimit(Long currentAdvertised) {
        long local = acceptedMaxResponseBytes == null ? Long.MAX_VALUE : acceptedMaxResponseBytes;
        Long advertised = advertisedMaxResponseBytes;
        long effective = advertised == null ? local : Math.min(local, advertised);
        return currentAdvertised == null ? effective : Math.min(effective, currentAdvertised);
    }

    private static Long parseAdvertisedMaxResponseBytes(java.util.List<String> values,
                                                         String what, int status) {
        if (values.isEmpty()) return null;
        if (values.size() != 1) {
            throw invalidAdvertisedResponseBudget(what, status);
        }
        String value = values.getFirst();
        if (value == null || value.isEmpty() || value.indexOf(',') >= 0
                || !value.matches("[1-9][0-9]*")) {
            throw invalidAdvertisedResponseBudget(what, status);
        }
        try {
            long parsed = Long.parseLong(value);
            if (parsed < (64L << 10) || parsed > 9_007_199_254_740_991L) {
                throw new NumberFormatException();
            }
            return parsed;
        } catch (NumberFormatException e) {
            throw invalidAdvertisedResponseBudget(what, status);
        }
    }

    private static RpcError invalidAdvertisedResponseBudget(String what, int status) {
        return new RpcError("ProtocolError",
                what + ": response header " + HttpServer.MAX_RESPONSE_BYTES_HEADER
                        + " must be exactly one ASCII integer between 65536 and "
                        + "9007199254740991", "", "", "http_" + status);
    }

    /** First 200 bytes of a non-Arrow body, for an error message a human can act on. */
    private static String preview(byte[] body) {
        if (body == null || body.length == 0) return "";
        int n = Math.min(body.length, 200);
        return new String(body, 0, n, StandardCharsets.UTF_8).replace('\n', ' ').trim();
    }

    // ------------------------------------------------------------------
    // Batch semantics shared by every read path
    // ------------------------------------------------------------------

    /**
     * Refuse an externalized ({@code vgi_rpc.location}) pointer batch.
     *
     * <p>Reached when this connection was built without an
     * {@link farm.query.vgirpc.external.ExternalLocationConfig}. Handing the
     * caller the pointer's zero-row body would be silent row loss on exactly
     * the path externalization exists for — large results — so it is a hard
     * failure with the same {@code ExternalLocationError} type
     * {@code ClientStreamSession} raises, letting one catch cover both
     * clients.</p>
     *
     * @param meta the pointer batch's custom metadata
     */
    static void failOnPointerBatch(Map<String, String> meta) {
        throw new RpcError("ExternalLocationError",
                "the worker returned an externalized batch (" + Metadata.LOCATION + "="
                        + LocationResolver.redactUrl(meta.get(Metadata.LOCATION))
                        + ") but this connection has no ExternalLocationConfig; "
                        + "build it with externalLocation(...) to resolve it", "");
    }

    /**
     * Fetch what an external-location pointer names.
     *
     * <p>What comes back from storage is a whole IPC <em>stream</em>, not a
     * lone batch: the worker wrote the turn there, so the object holds the log
     * lines it emitted, the data batch, and — on a stream turn — the
     * continuation cursor riding that batch's metadata. All three are read here.
     * Taking the first data batch and discarding the rest is the tempting
     * shortcut and it is wrong twice over: a method's log output would depend on
     * whether its batch happened to cross the externalization threshold, and a
     * stream would lose its place after one turn.</p>
     *
     * <p>Both failure modes — no configuration, and a fetch that did not work —
     * raise {@code ExternalLocationError}, never a zero-row batch. The worker
     * has said explicitly that rows exist somewhere; a short result that looks
     * complete is strictly worse than a failure.</p>
     *
     * @param pointerMeta the pointer batch's custom metadata
     * @return the fetched batch, its dictionaries and the metadata it should
     *     travel with; the caller owns it and must close it
     */
    ResolvedBatch resolvePointer(Map<String, String> pointerMeta) {
        if (externalFetcher == null) failOnPointerBatch(pointerMeta);
        String url = pointerMeta.get(Metadata.LOCATION);
        String safeUrl = LocationResolver.redactUrl(url);
        byte[] fetched;
        try {
            fetched = externalFetcher.fetch(URI.create(url), pointerMeta.get(Metadata.LOCATION_SHA256));
        } catch (RpcError e) {
            throw e;
        } catch (Exception fe) {
            throw new RpcError("ExternalLocationError",
                    "failed to resolve " + safeUrl + " (" + fe.getClass().getSimpleName() + ")", "");
        }
        IpcStreamReader reader = new IpcStreamReader(
                new ByteArrayInputStream(fetched), Allocators.root());
        boolean handedOff = false;
        try {
            while (true) {
                Map<String, String> md = reader.readNextBatch();
                if (md == null) {
                    throw new RpcError("ExternalLocationError",
                            "the external stream at " + safeUrl + " contained no data batch", "");
                }
                VectorSchemaRoot root = reader.root();
                // Logs and errors ride *inside* the externalized stream, not
                // beside the pointer: the worker writes the whole turn to
                // storage. Walking past them without relaying would make a
                // method's log output depend on whether its batch happened to
                // cross the externalization threshold — the one thing
                // externalization is meant not to be observable in.
                if (dispatchLogOrError(md, root.getRowCount())) continue;
                Map<String, String> merged = new LinkedHashMap<>(pointerMeta);
                merged.remove(Metadata.LOCATION);
                merged.remove(Metadata.LOCATION_SHA256);
                merged.putAll(md);
                handedOff = true;
                return new ResolvedBatch(root, merged, reader.dictionaryProvider(), reader);
            }
        } catch (IOException e) {
            throw new RpcError("ExternalLocationError",
                    "could not read the external stream at " + safeUrl + ": " + e.getMessage(), "");
        } finally {
            if (!handedOff) {
                try {
                    reader.close();
                } catch (Exception ignore) {
                    // Nothing took ownership, so nothing else will free it.
                }
            }
        }
    }

    /**
     * Handle one batch's log/error semantics.
     *
     * <p>Pointer batches are deliberately not handled here: what to do with one
     * depends on what the caller is going to do with the batch it names — decode
     * it, reframe it, hand it to a stream — so each read path resolves its own
     * via {@link #resolvePointer(Map)}.</p>
     *
     * @return {@code true} when the batch was a log line and the caller should read on
     */
    boolean dispatchLogOrError(Map<String, String> meta, int rowCount) {
        Wire.BatchKind kind = Wire.classify(rowCount, meta);
        if (kind == Wire.BatchKind.LOG) {
            onLog.accept(Wire.messageFromMetadata(meta));
            return true;
        }
        if (kind == Wire.BatchKind.ERROR) throw Wire.errorFromMetadata(meta);
        return false;
    }

    /** Frame an untyped request the way the typed path frames a typed one. */
    private byte[] rawRequestBody(String method, String protocol, String protocolVersion,
                                  AnnotatedBatch request) {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        try {
            ClientMarshalling.writeRawRequest(buf, method, request.root(), request.dictionaryProvider(),
                    request.customMetadata(), protocol, protocolVersion);
        } catch (IOException e) {
            throw new RpcError("TransportError",
                    method + ": could not frame request: " + e.getMessage(), "");
        }
        return buf.toByteArray();
    }

    /**
     * Read the header stream off the front of an init response without decoding
     * it, returning its single batch reframed as a one-batch IPC stream.
     *
     * <p>The untyped counterpart of {@code ClientHandler.readHeaderStream}: a
     * caller relaying batches cannot name a Java type for the header any more
     * than it can for the data. Also the error path for an {@code /init} that
     * failed before the header was written — the worker then answers a single
     * error stream, so the EXCEPTION batch is raised here with its own type and
     * message rather than being mistaken for a header.</p>
     */
    private byte[] readRawHeaderStream(ByteArrayInputStream in, String method) throws IOException {
        try (IpcStreamReader r = new IpcStreamReader(in, Allocators.root())) {
            while (true) {
                Map<String, String> md = r.readNextBatch();
                if (md == null) {
                    throw new RpcError("ProtocolError",
                            method + ": stream header missing from init response", "");
                }
                VectorSchemaRoot root = r.root();
                if (dispatchLogOrError(md, root.getRowCount())) continue;
                byte[] framed;
                if (LocationResolver.isPointer(root.getRowCount(), md)) {
                    try (ResolvedBatch resolved = resolvePointer(md)) {
                        framed = Wire.writeOneBatch(resolved.root(), resolved.customMetadata(),
                                resolved.dictionaries());
                    }
                } else {
                    framed = Wire.writeOneBatch(root, md, r.dictionaryProvider());
                }
                // Consume the header stream's trailing EOS so the body stream
                // that follows starts at a clean boundary.
                r.drain();
                return framed;
            }
        }
    }

    // ------------------------------------------------------------------

    private final class ClientHandler implements InvocationHandler {

        private final Map<String, RpcMethodInfo> methods;
        private final String protocol;
        private final String protocolVersion;

        ClientHandler(Map<String, RpcMethodInfo> methods, String protocol, String protocolVersion) {
            this.methods = methods;
            this.protocol = protocol;
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
            byte[] response = post(urlFor(protocol, info.name(), ""), requestBody(info, m, args), info.name());
            try (IpcStreamReader r = new IpcStreamReader(
                    new ByteArrayInputStream(response), Allocators.root())) {
                while (true) {
                    Map<String, String> md = r.readNextBatch();
                    if (md == null) {
                        throw new RpcError("ProtocolError",
                                info.name() + ": response stream ended without a result batch", "");
                    }
                    VectorSchemaRoot root = r.root();
                    if (dispatchLogOrError(md, root.getRowCount())) continue;
                    if (LocationResolver.isPointer(root.getRowCount(), md)) {
                        try (ResolvedBatch resolved = resolvePointer(md)) {
                            return ClientMarshalling.decodeResult(info, resolved.root());
                        }
                    }
                    return ClientMarshalling.decodeResult(info, root);
                }
            }
        }

        private Object doStream(RpcMethodInfo info, Method m, Object[] args) throws IOException {
            byte[] response = post(urlFor(protocol, info.name(), "/init"),
                    requestBody(info, m, args), info.name() + "/init");
            // The init response is a *sequence* of IPC streams when the method
            // declares a header: the header stream, then the stream body. One
            // reader per stream, each picking up where the last one's
            // end-of-stream marker left off.
            ByteArrayInputStream in = new ByteArrayInputStream(response);
            ArrowSerializableRecord header = null;
            Class<?> headerType = ClientMarshalling.resolveHeaderType(info);
            if (headerType != null) header = readHeaderStream(in, headerType);
            return new HttpRpcStream<>(HttpRpcConnection.this, protocol, info.name(), in, header);
        }

        private byte[] requestBody(RpcMethodInfo info, Method m, Object[] args) throws IOException {
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            ClientMarshalling.writeRequest(buf, info, m, args, protocol, protocolVersion);
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
                    if (dispatchLogOrError(md, r.root().getRowCount())) continue;
                    if (LocationResolver.isPointer(r.root().getRowCount(), md)) {
                        // A header can be externalized like any other batch; a
                        // zero-row pointer read as the header itself would
                        // decode to a record of nulls.
                        try (ResolvedBatch resolved = resolvePointer(md)) {
                            ArrowSerializableRecord header = RecordCodec.fromRowMap(
                                    (Class<? extends ArrowSerializableRecord>) headerType,
                                    Marshalling.decodeRow(resolved.root(), resolved.dictionaries(),
                                            resolved.root().getSchema()));
                            r.drain();
                            return header;
                        }
                    }
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
        private ProxyEndpoint socksProxy;
        private String protocolVersion;
        private Long acceptedMaxResponseBytes = DEFAULT_ACCEPTED_MAX_RESPONSE_BYTES;
        private ExternalLocationConfig externalConfig;
        private Integer compressionLevel;
        private IrohEndpoint irohEndpoint;
        private IrohTransportOptions irohOptions;
        private IrohTransportProvider irohProvider;

        private Builder(String endpoint) {
            if (endpoint == null || endpoint.isBlank()) {
                throw new IllegalArgumentException("endpoint must not be blank");
            }
            String trimmed = endpoint.trim();
            while (trimmed.endsWith("/")) trimmed = trimmed.substring(0, trimmed.length() - 1);
            this.endpoint = trimmed;
        }

        private Builder(String endpoint, IrohEndpoint irohEndpoint,
                        IrohTransportOptions irohOptions, IrohTransportProvider irohProvider) {
            this(endpoint);
            this.irohEndpoint = irohEndpoint;
            this.irohOptions = irohOptions;
            this.irohProvider = irohProvider;
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
            if (HttpServer.ACCEPT_MAX_RESPONSE_BYTES_HEADER.equalsIgnoreCase(name)) {
                throw new IllegalArgumentException("use acceptedMaxResponseBytes() for " + name);
            }
            headers.put(name, value);
            return this;
        }

        /** Largest decoded Arrow IPC response this native client will accept. */
        public Builder acceptedMaxResponseBytes(long bytes) {
            if (bytes < (64L << 10) || bytes > 9_007_199_254_740_991L) {
                throw new IllegalArgumentException(
                        "acceptedMaxResponseBytes must be between 65536 and 9007199254740991");
            }
            this.acceptedMaxResponseBytes = bytes;
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
            if (timeout == null || timeout.isZero() || timeout.isNegative()) {
                throw new IllegalArgumentException("connect timeout must be positive");
            }
            this.connectTimeout = timeout;
            return this;
        }

        /**
         * Route every HTTP and HTTPS connection through an explicit credential-free SOCKS5h
         * proxy. The origin hostname is sent to the proxy and is never resolved locally.
         * Proxy failure is terminal and never falls back to a direct connection.
         *
         * @param proxyUri a {@code socks5h://host:port} URI without user info
         * @return this builder
         */
        public Builder socks5hProxy(String proxyUri) {
            if (irohEndpoint != null) {
                throw new IllegalArgumentException("socks5hProxy does not apply to httpi://");
            }
            this.socksProxy = parseSocks5hProxy(proxyUri);
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
        public Builder httpClient(HttpClient client) {
            if (irohEndpoint != null) {
                throw new IllegalArgumentException("httpClient does not apply to httpi://");
            }
            this.httpClient = client;
            return this;
        }

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

        /**
         * Resolve externalized ({@code vgi_rpc.location}) batches by fetching
         * them from the storage the pointer names.
         *
         * <p>Without this, a pointer batch is a hard {@link RpcError}: the
         * alternative — delivering the pointer's zero-row body — is silent row
         * loss on exactly the responses too large to send inline, which is the
         * one case where losing rows is most likely to go unnoticed. Supplying
         * a config does not weaken that; an unresolvable pointer still fails.
         *
         * @param config storage configuration for the fetch, or {@code null} to
         *     keep refusing pointer batches
         * @return this builder
         */
        public Builder externalLocation(ExternalLocationConfig config) {
            this.externalConfig = config;
            return this;
        }

        /**
         * Compress request bodies with zstd at {@code level}.
         *
         * <p>Three states, and they are distinct:</p>
         *
         * <ul>
         *   <li><em>never called</em> — this client's default, which is no
         *       request compression;</li>
         *   <li>{@code null} — request compression explicitly disabled, which
         *       is how a caller states the default rather than inheriting
         *       it;</li>
         *   <li>an integer — that zstd level.</li>
         * </ul>
         *
         * <p>Enabling it costs one {@code OPTIONS /health} probe on the first
         * call, because a body is compressed only when the worker advertises
         * the codec. Compressing regardless would turn a worker that speaks no
         * compression into a 415 on every request, and the advertisement is the
         * only thing that distinguishes such a worker from one built before the
         * header existed.</p>
         *
         * <p>Responses are compressed only if the worker chooses to; enabling
         * this also stops the connection demanding {@code identity} back, so a
         * compressed answer becomes possible (and is decoded transparently).</p>
         *
         * @param level the zstd compression level, or {@code null} to disable
         *     request compression
         * @return this builder
         */
        public Builder compressionLevel(Integer level) {
            this.compressionLevel = level;
            return this;
        }

        /**
         * Build the connection.
         *
         * @return a ready connection
         */
        public HttpRpcConnection build() {
            if (irohEndpoint != null) {
                throw new IllegalStateException("call buildIroh() for an httpi:// connection");
            }
            return new HttpRpcConnection(this);
        }

        /** Open the configured {@code iroh-http/2} provider and build the connection. */
        public HttpRpcConnection buildIroh() throws IOException {
            if (irohEndpoint == null) {
                throw new IllegalStateException("buildIroh() requires HttpRpcConnection.irohBuilder()");
            }
            IrohHttpTransport transport = irohProvider.openHttp(irohEndpoint, irohOptions);
            if (transport == null) {
                throw new IrohTransportException("Iroh provider returned a null HTTP transport",
                        IrohErrorStage.OPEN_STREAM, IrohErrorCategory.UNAVAILABLE,
                        IrohDispatchCertainty.NOT_SENT);
            }
            return new HttpRpcConnection(this, transport);
        }
    }

    private static ProxyEndpoint parseSocks5hProxy(String value) {
        URI uri;
        try {
            uri = URI.create(value);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("proxy must be credential-free socks5h://host:port", e);
        }
        if (!"socks5h".equals(uri.getScheme()) || uri.getRawUserInfo() != null
                || uri.getHost() == null || uri.getPort() < 1 || uri.getPort() > 65535
                || uri.getRawQuery() != null || uri.getRawFragment() != null
                || !(uri.getRawPath() == null || uri.getRawPath().isEmpty())) {
            throw new IllegalArgumentException("proxy must be credential-free socks5h://host:port");
        }
        return new ProxyEndpoint(uri.getHost(), uri.getPort());
    }

    private record ProxyEndpoint(String host, int port) {}
}
