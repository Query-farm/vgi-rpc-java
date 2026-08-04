// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.luben.zstd.Zstd;
import farm.query.vgirpc.AccessLogScope;
import farm.query.vgirpc.AuthContext;
import farm.query.vgirpc.AuthScope;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.RpcServer;
import farm.query.vgirpc.RpcStream;
import farm.query.vgirpc.SessionLostError;
import farm.query.vgirpc.external.UploadUrlProvider;
import farm.query.vgirpc.http.auth.ProxyProof;
import farm.query.vgirpc.transport.RpcTransport;
import farm.query.vgirpc.wire.Allocators;
import farm.query.vgirpc.wire.IpcStreamReader;
import farm.query.vgirpc.wire.IpcStreamWriter;
import farm.query.vgirpc.wire.Metadata;
import farm.query.vgirpc.wire.Wire;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * HTTP transport server for {@link RpcServer}. Each request is handled by a
 * lightweight servlet that constructs an in-memory {@link RpcTransport} to
 * reuse the existing unary dispatch code.
 *
 * <p>Endpoints:
 * <ul>
 *   <li>{@code GET /vgi/health} — JSON status probe.</li>
 *   <li>{@code POST /vgi/{method}} — unary call. Request body is one Arrow IPC
 *   stream (params). Response body is one Arrow IPC stream (result or error).</li>
 *   <li>{@code POST /vgi/{method}/init} and {@code /exchange} — streaming
 *   endpoints.</li>
 *   <li>{@code POST /vgi/__introspect_token__} — opaque credential to principal;
 *   refuses definitively unless {@link TokenIntrospection} is configured.</li>
 * </ul>
 */
public final class HttpServer {

    /** MIME type of every request and response body: a single Arrow IPC stream. */
    public static final String ARROW_CONTENT_TYPE = "application/vnd.apache.arrow.stream";

    /** Capability response header (mirrors {@code vgi_rpc/http/_common.py}): the request-body size cap in bytes. */
    public static final String MAX_REQUEST_BYTES_HEADER = "VGI-Max-Request-Bytes";
    /** Capability response header: {@code "true"} when the {@code __upload_url__} endpoint is wired up. */
    public static final String UPLOAD_URL_HEADER = "VGI-Upload-URL-Support";
    /** Capability response header: advisory per-object upload size cap in bytes. */
    public static final String MAX_UPLOAD_BYTES_HEADER = "VGI-Max-Upload-Bytes";
    /** Capability response header: operator-configured cap on inline response bodies, in bytes. */
    public static final String MAX_RESPONSE_BYTES_HEADER = "VGI-Max-Response-Bytes";
    /** Capability response header: operator-configured cap on externalized response payloads, in bytes. */
    public static final String MAX_EXTERNALIZED_RESPONSE_BYTES_HEADER = "VGI-Max-Externalized-Response-Bytes";
    /** Capability response header: {@code "true"}/{@code "false"} — whether the {@link RpcServer} has an external-location config and can externalise oversized payloads. */
    public static final String EXTERNALIZATION_ENABLED_HEADER = "VGI-Externalization-Enabled";
    /**
     * Capability response header: the ordered intersection of the codecs this
     * server can decode on requests and produce on responses — right now,
     * runtime-available and enabled by configuration — e.g. {@code "zstd, gzip"}.
     * Order is the server's own preference and is informational; the client's
     * stated order decides which codec a given response actually uses.
     * {@code identity} is omitted: always available, no information.
     *
     * <p>Present-but-empty means "I speak no compression", which is distinct
     * from an older server that never emits the header at all (assume zstd).
     */
    public static final String SUPPORTED_ENCODINGS_HEADER = "VGI-Supported-Encodings";
    /** Response header set to {@code "true"} when a 200 response body carries an Arrow error batch. */
    public static final String RPC_ERROR_HEADER = "X-VGI-RPC-Error";

    /**
     * The synthetic method name used by the {@code __upload_url__} endpoint.
     * Public so an intermediary that terminates or serves the upload-URL flow
     * need not copy the constant.
     */
    public static final String UPLOAD_URL_METHOD = "__upload_url__";
    /** Cap on the {@code count} parameter to one {@code __upload_url__/init} call. */
    public static final int MAX_UPLOAD_URL_COUNT = 100;

    private static final ObjectMapper JSON = new ObjectMapper();

    /** Schema for the {@code __upload_url__} request batch. */
    public static final Schema UPLOAD_URL_PARAMS_SCHEMA = new Schema(List.of(
            new Field("count", FieldType.nullable(new ArrowType.Int(64, true)), null)));

    /** Schema for the upload-URL response batch. */
    public static final Schema UPLOAD_URL_RESPONSE_SCHEMA = new Schema(List.of(
            new Field("upload_url", FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("download_url", FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("expires_at",
                    FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC")),
                    null)));

    private static final Schema UPLOAD_URL_SCHEMA = UPLOAD_URL_RESPONSE_SCHEMA;

    /** Shared static landing page, loaded once from the classpath (may be {@code null} if absent). */
    private static final byte[] LANDING_HTML = loadLandingHtml();

    private static byte[] loadLandingHtml() {
        try (InputStream in = HttpServer.class.getResourceAsStream("landing.html")) {
            return in == null ? null : in.readAllBytes();
        } catch (IOException e) {
            return null;
        }
    }

    private final RpcServer rpc;
    private final HttpStreamHandler streamHandler;
    private final Authenticator authenticator;
    private final DescribeProvider describeProvider;
    private final List<HttpPreHandler> preHandlers;
    private final Server jetty;
    private final String prefix;
    private final long maxRequestBytes;
    private final long maxResponseBytes;
    private final boolean advertiseMaxRequestBytes;
    private final int zstdLevel;
    private final UploadUrlProvider uploadUrlProvider;
    private final Long maxUploadBytes;
    /** Operator-facing response caps (advertised via VGI-Max-* headers and
     *  enforced post-flush as Arrow EXCEPTION + 200 + X-VGI-RPC-Error).
     *  0 = unbounded.  Distinct from {@link #maxResponseBytes}, which is the
     *  in-process memory bound. */
    private final long advertisedMaxResponseBytes;
    private final long advertisedMaxExternalizedResponseBytes;
    /** Operator-declared: this worker's proxy-proof gate is in REQUIRE mode.
     *  Advertisement only — the gate is an opaque {@link Authenticator}, so the
     *  server has no way to read the posture back off it. */
    private final boolean proxyProofRequired;
    /** The §5 proxy note, or {@code ""} when this service's auth does not
     *  depend on a proxy. Computed once from configuration — never from what
     *  failed on a request — so every 401 this server emits says the same
     *  thing and none of them is an oracle. */
    private final String proxyHint;
    private final boolean stickyEnabled;
    private final long stickyDefaultTtlSeconds;
    private final Map<String, String> stickyEchoHeaders;
    private final boolean exposeTestDrainAdmin;
    private final byte[] sessionTokenKey;
    private final SessionRegistry sessionRegistry;
    /** Codecs this server may use, in server-preference order — from
     *  {@link Config#supportedEncodings()}. Drives the negotiation walk, the
     *  request-body decode gate and the {@link #SUPPORTED_ENCODINGS_HEADER}
     *  advertisement, so none of the three can disagree. Empty = never
     *  compress, and accept no compressed request bodies. */
    private final List<String> supportedEncodings;
    /** The token-introspection endpoint, or {@code null} when no resolver was
     *  configured. Null is the load-bearing state: no worker grows a
     *  credential-to-identity oracle by upgrading a dependency. */
    private final TokenIntrospection introspection;
    /** Browser access policy, or {@code null} when no origin was configured —
     *  CORS is opt-in, and off means not one {@code Access-Control-*} header. */
    private final CorsPolicy cors;
    private int port;

    /**
     * Defaults: loopback bind, ephemeral port, no prefix, anonymous auth, 1-hour TTL, 16 MiB request/response cap.
     *
     * @param rpc the dispatcher serving the service
     */
    public HttpServer(RpcServer rpc) {
        this(rpc, Config.defaults());
    }

    /**
     * Create a server for {@code rpc} with the given configuration. Call
     * {@link #start()} to bind and begin accepting requests.
     *
     * @param rpc the dispatcher serving the service
     * @param config server configuration (see {@link Config})
     */
    public HttpServer(RpcServer rpc, Config config) {
        this.rpc = rpc;
        this.streamHandler = new HttpStreamHandler(rpc, config.tokenKey(),
                config.tokenTtlSeconds(), config.maxResponseBytes(), config.callStateCacheMaxEntries());
        this.authenticator = config.authenticator() != null ? config.authenticator() : Authenticator.ANONYMOUS;
        this.describeProvider = config.describeProvider();
        this.preHandlers = config.preHandlers();
        this.prefix = config.prefix();
        this.maxRequestBytes = config.maxRequestBytes();
        this.maxResponseBytes = config.maxResponseBytes();
        this.advertiseMaxRequestBytes = config.advertiseMaxRequestBytes();
        this.zstdLevel = config.zstdLevel();
        this.uploadUrlProvider = config.uploadUrlProvider();
        this.maxUploadBytes = config.maxUploadBytes();
        this.advertisedMaxResponseBytes = config.advertisedMaxResponseBytes();
        this.advertisedMaxExternalizedResponseBytes = config.advertisedMaxExternalizedResponseBytes();
        this.proxyProofRequired = config.proxyProofRequired();
        // The proof gate contributes its header only in require mode: in allow
        // mode an absent proof never denies, so the note would misdirect.
        List<String> proxyAuthHeaders = new ArrayList<>();
        if (config.proxyProofRequired()) proxyAuthHeaders.add(ProxyProof.PROOF_HEADER);
        for (String h : config.proxyAuthHeaders()) {
            if (!proxyAuthHeaders.contains(h)) proxyAuthHeaders.add(h);
        }
        this.proxyHint = Unauthorized.proxyHint(proxyAuthHeaders);
        this.stickyEnabled = config.stickyEnabled();
        this.stickyDefaultTtlSeconds = config.stickyDefaultTtlSeconds();
        this.stickyEchoHeaders = config.stickyEchoHeaders();
        this.exposeTestDrainAdmin = config.exposeTestDrainAdmin();
        this.supportedEncodings = config.supportedEncodings();
        // Sticky tokens reuse the per-process state-token key when one is
        // configured; otherwise a random 32-byte key is generated on the fly
        // (tokens won't survive worker restarts or load-balance, but the
        // conformance worker is a single process so that's fine).
        if (this.stickyEnabled) {
            if (config.tokenKey() != null) {
                this.sessionTokenKey = config.tokenKey().clone();
            } else {
                this.sessionTokenKey = new byte[32];
                new java.security.SecureRandom().nextBytes(this.sessionTokenKey);
            }
            this.sessionRegistry = new SessionRegistry(this.stickyDefaultTtlSeconds);
        } else {
            this.sessionTokenKey = null;
            this.sessionRegistry = null;
        }
        // Built before the CORS policy: corsExposeHeaders() reads this field, and
        // the advertise/expose pair has to agree.
        this.introspection = config.introspectResolver() == null ? null
                : new TokenIntrospection(config.introspectResolver(), config.introspectPrincipals(),
                        config.introspectTtlSeconds(), config.introspectRateLimitPerSecond());
        this.cors = config.corsOrigins().isEmpty()
                ? null
                : new CorsPolicy(config.corsOrigins(), config.corsMaxAgeSeconds(), corsExposeHeaders());
        this.jetty = new Server();
        // Graceful-shutdown window: Jetty.stop() waits up to this many ms for
        // in-flight requests to finish before forcing closes. 15s is enough
        // for a worker tick to complete (NS API timeout is 10s) without
        // dragging out PaaS-side rolling restarts.
        jetty.setStopTimeout(15_000L);
        jetty.addConnector(buildConnector(jetty, config));

        ServletContextHandler ctx = new ServletContextHandler();
        ctx.setContextPath("/");
        String pattern = prefix.isEmpty() ? "/*" : prefix + "/*";
        ctx.addServlet(new ServletHolder(new RouterServlet()), pattern);
        jetty.setHandler(ctx);
    }

    private static ServerConnector buildConnector(Server server, Config config) {
        TlsConfig tls = config.tls();
        ServerConnector connector;
        if (tls == null) {
            connector = new ServerConnector(server);
        } else {
            SslContextFactory.Server ssl = new SslContextFactory.Server();
            ssl.setKeyStorePath(tls.keystorePath().toAbsolutePath().toString());
            ssl.setKeyStorePassword(tls.keystorePassword());
            if (tls.keyManagerPassword() != null) ssl.setKeyManagerPassword(tls.keyManagerPassword());
            HttpConfiguration https = new HttpConfiguration();
            https.setSecureScheme("https");
            https.addCustomizer(new SecureRequestCustomizer());
            connector = new ServerConnector(server,
                    new SslConnectionFactory(ssl, "http/1.1"),
                    new HttpConnectionFactory(https));
        }
        connector.setHost(config.host());
        connector.setPort(config.port());
        connector.setIdleTimeout(config.idleTimeoutMs());
        return connector;
    }

    /**
     * Immutable configuration for {@link HttpServer}. Use {@link #defaults()} or
     * {@link #builder()} to construct; prefer the builder for any non-default field.
     *
     * @param host             listen address. Defaults to {@code "127.0.0.1"};
     *                         set to {@code "0.0.0.0"} (or a specific interface)
     *                         only when fronted by TLS or a TLS-terminating proxy.
     * @param port             listen port; {@code 0} for an OS-assigned ephemeral port.
     * @param prefix           URL prefix (e.g. {@code "/vgi"}); empty for no prefix.
     * @param tokenKey         AEAD master key (32 bytes) used to seal stream
     *                         state tokens; {@code null} generates a random
     *                         per-process key (tokens won't survive restarts
     *                         or load-balance across workers).
     * @param tokenTtlSeconds  maximum state-token age before rejection; defaults to
     *                         {@value #DEFAULT_TOKEN_TTL_SECONDS}s. {@code 0} disables
     *                         enforcement (not recommended for multi-user deployments).
     * @param authenticator    per-request authenticator; {@code null} = anonymous.
     * @param preHandlers      pre-route handlers run in order before dispatch.
     * @param maxRequestBytes  request body cap; defaults to
     *                         {@value #DEFAULT_MAX_BYTES} bytes. Oversized requests
     *                         get HTTP 413 — large batches must use the
     *                         external-location protocol instead.
     * @param maxResponseBytes response body cap; same rationale as request cap.
     * @param idleTimeoutMs    Jetty connector idle timeout in milliseconds.
     * @param zstdLevel        compression level for the {@code zstd}
     *                         Content-Encoding (1=fastest, 22=max). Default
     *                         {@value #DEFAULT_ZSTD_LEVEL} — measured 4.7x faster
     *                         than level 3 on an 8.41 MB Arrow payload <em>and</em>
     *                         smaller on the wire, so it is not a size/speed
     *                         trade-off.
     * @param supportedEncodings codecs this server may produce on responses and
     *                         accept on request bodies, in server-preference
     *                         order; the value advertised via
     *                         {@code VGI-Supported-Encodings}. {@code null} takes
     *                         the default ({@code zstd, gzip}, narrowed by
     *                         {@code VGI_HTTP_DISABLE_ZSTD}); an <em>empty</em>
     *                         list means "never compress" — identity only —
     *                         which is advertised as a present-but-empty header
     *                         and makes compressed request bodies a 415.
     *                         {@code identity} is not a member: it is always
     *                         available and never advertised.
     * @param tls              TLS settings; {@code null} = plaintext (only safe
     *                         on loopback or behind a TLS-terminating proxy).
     * @param advertiseMaxRequestBytes when {@code true}, every response carries
     *                         {@code VGI-Max-Request-Bytes} so capability-aware
     *                         clients can externalize oversized requests up front.
     *                         Default {@code false}.
     * @param uploadUrlProvider when non-null, wires the {@code __upload_url__/init}
     *                         endpoint (presigned upload/download URL minting) and
     *                         advertises {@code VGI-Upload-URL-Support: true}.
     *                         {@code null} disables the endpoint.
     * @param maxUploadBytes   advisory per-object upload cap advertised via
     *                         {@code VGI-Max-Upload-Bytes} when an upload-URL
     *                         provider is set; {@code null} omits the header.
     *                         Not enforced server-side.
     * @param advertisedMaxResponseBytes operator-facing cap on inline response
     *                         bodies, advertised via {@code VGI-Max-Response-Bytes}
     *                         and enforced post-flush for unary and stream-exchange
     *                         responses (Arrow EXCEPTION batch, HTTP 200 +
     *                         {@code X-VGI-RPC-Error}). {@code 0} = unbounded.
     *                         Distinct from {@code maxResponseBytes}, the in-process
     *                         memory bound.
     * @param advertisedMaxExternalizedResponseBytes cap advertised via
     *                         {@code VGI-Max-Externalized-Response-Bytes};
     *                         advertisement-only today (the Java HTTP transport
     *                         does not yet externalise stream output).
     *                         {@code 0} = unbounded/omitted.
     * @param proxyProofRequired advertise
     *                         {@code VGI-Proxy-Proof-Required: true} on every
     *                         response. Set it only in
     *                         {@link farm.query.vgirpc.http.auth.ProxyProof.Mode#REQUIRE}
     *                         — {@code off} and {@code allow} never deny, so they
     *                         must not claim they do. Advertisement only: the gate
     *                         is an opaque {@link Authenticator}, so the server
     *                         cannot introspect the posture and the operator states
     *                         it. Default {@code false}.
     * @param proxyAuthHeaders proxy-injected headers this service's
     *                         authentication depends on, for a custom
     *                         {@link Authenticator} the framework cannot
     *                         introspect. Their presence is what turns on the
     *                         §5 proxy note ({@code VGI-Auth-Proxy-Required} +
     *                         {@code proxy_hint}) on every 401. The built-in
     *                         proxy-proof gate contributes its own header via
     *                         {@code proxyProofRequired}, so this is only
     *                         needed on top of that. Default empty.
     * @param callStateCacheMaxEntries entry ceiling for the per-process
     *                         call-state cache — a pure accelerator, since a
     *                         miss reopens the call token the client echoed.
     *                         {@code 0} disables it, which is how a client that
     *                         forgets to echo that token fails deterministically
     *                         rather than only on a cold node. Default
     *                         {@value CallStateCache#DEFAULT_MAX_ENTRIES}.
     * @param stickyEnabled    enable opt-in HTTP sticky sessions: clients sending
     *                         {@code VGI-Session-Accept: true} get an HMAC-signed
     *                         session token bound to their principal, and calls
     *                         on the same session serialize on a per-session lock.
     *                         Default {@code false}.
     * @param stickyDefaultTtlSeconds idle TTL for sticky-session registry entries,
     *                         in seconds; must be {@code > 0} when sticky sessions
     *                         are enabled. Default 300.
     * @param stickyEchoHeaders header-name → value map set verbatim on responses
     *                         that mint a sticky-session token (the names are also
     *                         advertised via the sticky-echo capability header).
     * @param exposeTestDrainAdmin conformance-only: expose the unauthenticated
     *                         {@code POST/DELETE /__test_drain__} admin endpoint
     *                         that toggles drain mode. Never enable in production.
     * @param describeProvider producer for the standardized landing surface's JSON
     *                         ({@code describe.json} + lazy column endpoints);
     *                         {@code null} disables those routes (the shared
     *                         {@code landing.html} and JSON health status are
     *                         still served).
     * @param corsOrigins      origins allowed to call this server from a browser;
     *                         empty (the default) leaves CORS off entirely — no
     *                         {@code Access-Control-*} header on any response. A
     *                         single {@code "*"} allows all. See
     *                         {@link Builder#corsOrigins(List)}.
     * @param corsMaxAgeSeconds preflight cache lifetime advertised via
     *                         {@code Access-Control-Max-Age}; {@code 0} omits the
     *                         header. Default
     *                         {@value #DEFAULT_CORS_MAX_AGE_SECONDS}s. Ignored
     *                         when {@code corsOrigins} is empty.
     * @param introspectResolver enables {@code POST {prefix}/__introspect_token__}.
     *                         This is the on/off switch: {@code null} (the
     *                         default) leaves the endpoint refusing definitively
     *                         and holding no resolver, so no worker grows a
     *                         credential-to-identity oracle by upgrading a
     *                         dependency. See {@link TokenIntrospection}.
     * @param introspectPrincipals principals permitted to introspect. Required
     *                         whenever {@code introspectResolver} is set, with
     *                         <em>no permissive default</em>: authentication and
     *                         introspection are different capabilities, and a
     *                         deployment where any valid credential may introspect
     *                         lets any user resolve any other user's credential to
     *                         its owner.
     * @param introspectTtlSeconds cache lifetime reported to the asker when a
     *                         {@link TokenIdentity} names none. Default
     *                         {@value TokenIntrospection#DEFAULT_TTL_SECONDS}s.
     * @param introspectRateLimitPerSecond introspection requests allowed per
     *                         caller per second (default
     *                         {@value TokenIntrospection#DEFAULT_RATE_LIMIT_PER_SECOND}).
     *                         Bounds, rather than closes, the oracle an
     *                         allowlisted-but-compromised caller still has.
     */
    public record Config(
            String host,
            int port,
            String prefix,
            byte[] tokenKey,
            long tokenTtlSeconds,
            Authenticator authenticator,
            List<HttpPreHandler> preHandlers,
            long maxRequestBytes,
            long maxResponseBytes,
            long idleTimeoutMs,
            int zstdLevel,
            List<String> supportedEncodings,
            TlsConfig tls,
            boolean advertiseMaxRequestBytes,
            UploadUrlProvider uploadUrlProvider,
            Long maxUploadBytes,
            long advertisedMaxResponseBytes,
            long advertisedMaxExternalizedResponseBytes,
            boolean proxyProofRequired,
            List<String> proxyAuthHeaders,
            int callStateCacheMaxEntries,
            boolean stickyEnabled,
            long stickyDefaultTtlSeconds,
            Map<String, String> stickyEchoHeaders,
            boolean exposeTestDrainAdmin,
            DescribeProvider describeProvider,
            List<String> corsOrigins,
            long corsMaxAgeSeconds,
            TokenResolver introspectResolver,
            List<String> introspectPrincipals,
            long introspectTtlSeconds,
            int introspectRateLimitPerSecond) {

        /** 1 hour. */
        public static final long DEFAULT_TOKEN_TTL_SECONDS = 3600;
        /** 2 hours — the ceiling Chromium honours for a preflight cache entry. */
        public static final long DEFAULT_CORS_MAX_AGE_SECONDS = 7200;
        /** 16 MiB applies to both request body and serialized response. */
        public static final long DEFAULT_MAX_BYTES = 16L << 20;
        /** 30 seconds. */
        public static final long DEFAULT_IDLE_TIMEOUT_MS = 30_000;
        /**
         * Fastest zstd level. Not a "cheap but bulkier" setting: on an 8.41 MB
         * Arrow payload level 1 measured 4.7x faster than level 3 <em>and</em>
         * produced a smaller body, so there is nothing to trade away. Arrow IPC
         * buffers are already dictionary/bit-packed, which is where the higher
         * levels' extra search normally pays off.
         */
        public static final int DEFAULT_ZSTD_LEVEL = 1;

        /**
         * The default producible codec set: both codecs, server-preference
         * order (zstd first — it dominates gzip on large Arrow bodies).
         * See {@link #defaultSupportedEncodings()} for the environment
         * override applied when the config leaves the set unset.
         */
        public static final List<String> DEFAULT_SUPPORTED_ENCODINGS =
                List.of(MediaTypes.ZSTD, MediaTypes.GZIP);

        /**
         * The codec set used when none is configured: {@link #DEFAULT_SUPPORTED_ENCODINGS},
         * or {@code [gzip]} when the {@code VGI_HTTP_DISABLE_ZSTD} environment
         * variable is set to anything but {@code "0"}.
         *
         * <p>That variable is the historical knob for exercising the gzip path
         * without uninstalling zstd-jni (it mirrors vgi-python's factory). It is
         * now just a preset over the general mechanism — one narrowing of the
         * configurable set, not a parallel code path — and an explicit
         * {@link Builder#supportedEncodings(List)} overrides it.
         *
         * @return the effective default producible set
         */
        public static List<String> defaultSupportedEncodings() {
            String disableZstd = System.getenv("VGI_HTTP_DISABLE_ZSTD");
            boolean disabled = disableZstd != null && !disableZstd.isEmpty() && !disableZstd.equals("0");
            return disabled ? List.of(MediaTypes.GZIP) : DEFAULT_SUPPORTED_ENCODINGS;
        }

        /**
         * Normalizes nullable fields (host, prefix, key copy, immutable
         * collection copies) and validates numeric bounds and codec names.
         */
        public Config {
            host = host != null ? host : "127.0.0.1";
            prefix = prefix != null ? prefix : "";
            tokenKey = tokenKey != null ? tokenKey.clone() : null;
            preHandlers = preHandlers != null ? List.copyOf(preHandlers) : List.of();
            proxyAuthHeaders = proxyAuthHeaders != null ? List.copyOf(proxyAuthHeaders) : List.of();
            stickyEchoHeaders = stickyEchoHeaders != null ? Map.copyOf(stickyEchoHeaders) : Map.of();
            corsOrigins = corsOrigins != null ? List.copyOf(corsOrigins) : List.of();
            introspectPrincipals = introspectPrincipals != null ? List.copyOf(introspectPrincipals) : List.of();
            supportedEncodings = supportedEncodings != null
                    ? normalizeEncodings(supportedEncodings)
                    : defaultSupportedEncodings();
            if (maxRequestBytes <= 0) throw new IllegalArgumentException("maxRequestBytes must be > 0");
            if (maxResponseBytes <= 0) throw new IllegalArgumentException("maxResponseBytes must be > 0");
            if (idleTimeoutMs < 0) throw new IllegalArgumentException("idleTimeoutMs must be >= 0");
            if (zstdLevel < 1 || zstdLevel > 22) throw new IllegalArgumentException("zstdLevel must be in [1, 22]");
            if (callStateCacheMaxEntries < 0) {
                throw new IllegalArgumentException("callStateCacheMaxEntries must be >= 0");
            }
            if (stickyEnabled && stickyDefaultTtlSeconds <= 0) {
                throw new IllegalArgumentException("stickyDefaultTtlSeconds must be > 0 when sticky is enabled");
            }
            if (corsMaxAgeSeconds < 0) throw new IllegalArgumentException("corsMaxAgeSeconds must be >= 0");
            // Validated at construction rather than at the first proxy preflight:
            // a credential-to-identity oracle is not something to discover is
            // misconfigured in production.
            if (introspectResolver != null) {
                TokenIntrospection.normalizeIntrospectors(introspectPrincipals);
            } else if (!introspectPrincipals.isEmpty()) {
                throw new IllegalArgumentException(
                        "introspectPrincipals was given without introspectResolver; the endpoint stays "
                                + "disabled, so the allowlist would have no effect. Pass both or neither.");
            }
            if (introspectTtlSeconds < 0) throw new IllegalArgumentException("introspectTtlSeconds must be >= 0");
            if (introspectRateLimitPerSecond < 0) {
                throw new IllegalArgumentException("introspectRateLimitPerSecond must be >= 0");
            }
        }

        /**
         * Default configuration.
         *
         * @return a config with all defaults (loopback, ephemeral port, anonymous auth)
         */
        public static Config defaults() { return builder().build(); }
        /**
         * Start building a configuration.
         *
         * @return a new {@link Builder} initialized with the defaults
         */
        public static Builder builder() { return new Builder(); }

        /**
         * Fluent builder for {@link Config}. Defaults bind to {@code 127.0.0.1}
         * on an automatically chosen port with no path prefix; override as needed
         * and call {@link #build()}.
         */
        public static final class Builder {
            private String host = "127.0.0.1";
            private int port = 0;
            private String prefix = "";
            private byte[] tokenKey;
            private long tokenTtlSeconds = DEFAULT_TOKEN_TTL_SECONDS;
            private Authenticator authenticator;
            private List<HttpPreHandler> preHandlers = List.of();
            private long maxRequestBytes = DEFAULT_MAX_BYTES;
            private long maxResponseBytes = DEFAULT_MAX_BYTES;
            private long idleTimeoutMs = DEFAULT_IDLE_TIMEOUT_MS;
            private int zstdLevel = DEFAULT_ZSTD_LEVEL;
            /** {@code null} = "unset", resolved to {@link Config#defaultSupportedEncodings()}
             *  at build time; an empty list is a real value ("never compress"). */
            private List<String> supportedEncodings;
            private TlsConfig tls;
            private boolean advertiseMaxRequestBytes;
            private UploadUrlProvider uploadUrlProvider;
            private Long maxUploadBytes;
            private long advertisedMaxResponseBytes;
            private long advertisedMaxExternalizedResponseBytes;
            private boolean proxyProofRequired;
            private List<String> proxyAuthHeaders = List.of();
            private int callStateCacheMaxEntries = CallStateCache.DEFAULT_MAX_ENTRIES;
            private boolean stickyEnabled;
            private long stickyDefaultTtlSeconds = 300;
            private Map<String, String> stickyEchoHeaders = Map.of();
            private boolean exposeTestDrainAdmin;
            private DescribeProvider describeProvider;
            private List<String> corsOrigins = List.of();
            private long corsMaxAgeSeconds = DEFAULT_CORS_MAX_AGE_SECONDS;
            private TokenResolver introspectResolver;
            private List<String> introspectPrincipals = List.of();
            private long introspectTtlSeconds = TokenIntrospection.DEFAULT_TTL_SECONDS;
            private int introspectRateLimitPerSecond = TokenIntrospection.DEFAULT_RATE_LIMIT_PER_SECOND;

            /**
             * Listen address (default {@code "127.0.0.1"}). See {@link Config#host()}.
             *
             * @param host the bind address; non-loopback values should be fronted by TLS
             * @return this builder
             */
            public Builder host(String host) { this.host = host; return this; }
            /**
             * Listen port; {@code 0} for an OS-assigned ephemeral port.
             *
             * @param port the port to bind (default {@code 0})
             * @return this builder
             */
            public Builder port(int port) { this.port = port; return this; }
            /**
             * URL prefix such as {@code "/vgi"}; empty for none.
             *
             * @param prefix the path prefix all endpoints are mounted under (default empty)
             * @return this builder
             */
            public Builder prefix(String prefix) { this.prefix = prefix; return this; }
            /**
             * 32-byte AEAD key sealing stream-state tokens; {@code null} generates a random per-process key.
             *
             * @param tokenKey the master key (defensively copied); set a fixed key
             *        when tokens must survive restarts or load-balance across workers
             * @return this builder
             */
            public Builder tokenKey(byte[] tokenKey) { this.tokenKey = tokenKey; return this; }
            /**
             * Maximum state-token age in seconds before rejection; {@code 0} disables enforcement.
             *
             * @param tokenTtlSeconds the TTL (default {@value Config#DEFAULT_TOKEN_TTL_SECONDS})
             * @return this builder
             */
            public Builder tokenTtlSeconds(long tokenTtlSeconds) { this.tokenTtlSeconds = tokenTtlSeconds; return this; }
            /**
             * Per-request authenticator; {@code null} = anonymous.
             *
             * @param authenticator credential check applied to every request (default {@link Authenticator#ANONYMOUS})
             * @return this builder
             */
            public Builder authenticator(Authenticator authenticator) { this.authenticator = authenticator; return this; }
            /**
             * Pre-route handlers run in order before dispatch.
             *
             * @param preHandlers the handlers; the first to return {@code true} short-circuits dispatch (default none)
             * @return this builder
             */
            public Builder preHandlers(List<HttpPreHandler> preHandlers) { this.preHandlers = preHandlers; return this; }
            /**
             * Request-body size cap in bytes; oversized requests get HTTP 413.
             *
             * @param maxRequestBytes the cap (default {@value Config#DEFAULT_MAX_BYTES} bytes)
             * @return this builder
             */
            public Builder maxRequestBytes(long maxRequestBytes) { this.maxRequestBytes = maxRequestBytes; return this; }
            /**
             * Response-body size cap in bytes (in-process memory bound).
             *
             * @param maxResponseBytes the cap (default {@value Config#DEFAULT_MAX_BYTES} bytes)
             * @return this builder
             */
            public Builder maxResponseBytes(long maxResponseBytes) { this.maxResponseBytes = maxResponseBytes; return this; }
            /**
             * Jetty connector idle timeout in milliseconds.
             *
             * @param idleTimeoutMs the timeout (default {@value Config#DEFAULT_IDLE_TIMEOUT_MS} ms)
             * @return this builder
             */
            public Builder idleTimeoutMs(long idleTimeoutMs) { this.idleTimeoutMs = idleTimeoutMs; return this; }
            /**
             * {@code zstd} Content-Encoding level, 1 (fastest) to 22 (max);
             * default {@value Config#DEFAULT_ZSTD_LEVEL}.
             *
             * @param zstdLevel the compression level used for {@code zstd}-encoded responses
             * @return this builder
             */
            public Builder zstdLevel(int zstdLevel) { this.zstdLevel = zstdLevel; return this; }
            /**
             * The codecs this server may produce on responses and accept on
             * request bodies, in server-preference order — the whole of its
             * compression configuration, and exactly what it advertises via
             * {@code VGI-Supported-Encodings}.
             *
             * <p>Pass {@link java.util.List#of()} to turn compression off: the
             * server then negotiates nothing (every response is identity),
             * answers a compressed request body with 415, and advertises a
             * present-but-empty header — which clients read as a positive "this
             * server speaks no compression", distinct from an absent header
             * (a legacy server, assume zstd).
             *
             * <p>There is deliberately no separate on/off flag: "off" is the
             * empty set, and narrower sets such as {@code List.of("gzip")} — what
             * {@code VGI_HTTP_DISABLE_ZSTD} presets — are the same mechanism.
             *
             * @param encodings the producible codecs, from {@link MediaTypes#ZSTD}
             *        and {@link MediaTypes#GZIP}; case-insensitive, duplicates
             *        collapse, and {@code identity} is rejected (always available,
             *        never advertised); an unknown token fails in {@link #build()}.
             *        {@code null} restores the default set.
             * @return this builder
             */
            public Builder supportedEncodings(List<String> encodings) {
                this.supportedEncodings = encodings; return this;
            }
            /**
             * TLS settings; {@code null} = plaintext (only safe on loopback or behind a TLS proxy).
             *
             * @param tls keystore settings for Jetty's HTTPS connector (default {@code null})
             * @return this builder
             */
            public Builder tls(TlsConfig tls) { this.tls = tls; return this; }
            /**
             * Advertise {@code VGI-Max-Request-Bytes} on every response.
             *
             * @param v {@code true} to emit the header so clients can externalize
             *        oversized requests up front (default {@code false})
             * @return this builder
             */
            public Builder advertiseMaxRequestBytes(boolean v) { this.advertiseMaxRequestBytes = v; return this; }
            /**
             * Wire the {@code __upload_url__/init} endpoint and advertise {@code VGI-Upload-URL-Support: true}.
             *
             * @param p mints presigned upload/download URL pairs; {@code null} disables the endpoint (default)
             * @return this builder
             */
            public Builder uploadUrlProvider(UploadUrlProvider p) { this.uploadUrlProvider = p; return this; }
            /**
             * Advertised via {@code VGI-Max-Upload-Bytes}; informational only.
             *
             * @param v the advisory per-object upload cap in bytes; {@code null} omits the header (default)
             * @return this builder
             */
            public Builder maxUploadBytes(Long v) { this.maxUploadBytes = v; return this; }
            /**
             * Advertised via {@code VGI-Max-Response-Bytes} and enforced
             * post-flush as a hard cap for unary and stream-exchange.
             * Pass {@code 0} to disable.
             *
             * @param v the cap in bytes; {@code 0} = unbounded (default)
             * @return this builder
             */
            public Builder advertisedMaxResponseBytes(long v) { this.advertisedMaxResponseBytes = v; return this; }
            /**
             * Advertised via {@code VGI-Max-Externalized-Response-Bytes}.  Java's
             * HTTP transport does not yet externalise stream output, so the
             * cap is advertisement-only today.
             *
             * @param v the cap in bytes; {@code 0} omits the header (default)
             * @return this builder
             */
            public Builder advertisedMaxExternalizedResponseBytes(long v) {
                this.advertisedMaxExternalizedResponseBytes = v; return this;
            }
            /**
             * Advertise {@code VGI-Proxy-Proof-Required: true} on every response.
             *
             * <p>Set it only when the installed proxy-proof gate is in
             * {@link farm.query.vgirpc.http.auth.ProxyProof.Mode#REQUIRE}: the header
             * is how an operator or proxy confirms a worker actually rejects
             * unproofed requests, so a worker in {@code allow} (which never denies)
             * advertising it would be the misconfiguration it is meant to expose.
             * It enables nothing on its own — the gate arrives as an opaque
             * {@link Authenticator} the server cannot inspect.
             *
             * @param v {@code true} to advertise the header (default {@code false})
             * @return this builder
             */
            public Builder proxyProofRequired(boolean v) { this.proxyProofRequired = v; return this; }
            /**
             * Declare the proxy-injected headers this service's authentication
             * depends on, for a custom {@link Authenticator} the framework
             * cannot introspect.
             *
             * <p>Declaring any header makes every 401 carry the §5 proxy note.
             * That is deliberate: the note describes a <em>static</em> property
             * of the deployment, not what failed on a given request, so it
             * discloses nothing about which stage rejected an attempt — and it
             * is still right in the case it exists for, where the proxy is not
             * forwarding the header and every request 401s.
             *
             * <p>The built-in proxy-proof gate contributes {@code VGI-Proxy-Proof}
             * on its own via {@link #proxyProofRequired(boolean)} (require mode
             * only — in allow mode an absent proof never denies, so the note
             * would misdirect), so this is only needed on top of that.
             *
             * @param headers header names a trusted proxy must set (default none)
             * @return this builder
             */
            public Builder proxyAuthHeaders(List<String> headers) { this.proxyAuthHeaders = headers; return this; }
            /**
             * Entry ceiling for the per-process call-state cache; {@code 0} disables it.
             *
             * <p>The cache is an accelerator, never a contract — a miss reopens
             * the call token the client echoed. Disabling it is how the
             * stateless-relay path gets exercised on every turn instead of only
             * on a cold or load-balanced node.
             *
             * @param v the ceiling (default {@value CallStateCache#DEFAULT_MAX_ENTRIES}); {@code 0} disables
             * @return this builder
             */
            public Builder callStateCacheMaxEntries(int v) { this.callStateCacheMaxEntries = v; return this; }
            /**
             * Enable opt-in HTTP sticky sessions.
             *
             * @param v {@code true} to honor {@code VGI-Session-Accept} opt-ins and
             *        mint HMAC-signed session tokens (default {@code false})
             * @return this builder
             */
            public Builder stickyEnabled(boolean v) { this.stickyEnabled = v; return this; }
            /**
             * Default TTL for new sticky sessions, in seconds.
             *
             * @param v idle seconds before a session registry entry expires
             *        (default 300; must be {@code > 0} when sticky is enabled)
             * @return this builder
             */
            public Builder stickyDefaultTtlSeconds(long v) { this.stickyDefaultTtlSeconds = v; return this; }
            /**
             * Header-name → value map echoed back to clients on session opens.
             *
             * @param m headers set verbatim on responses that mint a session token (default empty)
             * @return this builder
             */
            public Builder stickyEchoHeaders(Map<String, String> m) { this.stickyEchoHeaders = m; return this; }
            /**
             * Conformance-only: expose POST/DELETE {@code /__test_drain__} for tests.
             *
             * @param v {@code true} to expose the unauthenticated drain-toggle
             *        endpoint; never enable in production (default {@code false})
             * @return this builder
             */
            public Builder exposeTestDrainAdmin(boolean v) { this.exposeTestDrainAdmin = v; return this; }

            /**
             * Producer for the standardized landing surface's JSON contract
             * ({@code describe.json} + lazy column endpoints).
             *
             * @param p the describe provider; {@code null} disables the describe
             *          routes (default)
             * @return this builder
             */
            public Builder describeProvider(DescribeProvider p) { this.describeProvider = p; return this; }

            /**
             * Origins allowed to call this server from a browser; empty (the
             * default) leaves CORS off entirely.
             *
             * <p>Off means <em>no</em> {@code Access-Control-*} header on any
             * response, not a permissive default: a server that answers every
             * origin regardless of configuration is a different — and worse —
             * bug than one that answers none.
             *
             * <p>A single {@code "*"} entry allows all. That is safe here only
             * because vgi-rpc credentials are header-borne and this server never
             * sets {@code Access-Control-Allow-Credentials}, so a wildcard grant
             * carries no ambient authority. Anything else is matched
             * case-insensitively against the request's {@code Origin}, which is
             * then echoed back.
             *
             * @param origins allowed origins (e.g. {@code ["https://app.example"]})
             * @return this builder
             */
            public Builder corsOrigins(List<String> origins) { this.corsOrigins = origins; return this; }
            /**
             * Convenience for the single-origin case. See {@link #corsOrigins(List)}.
             *
             * @param origin the one allowed origin, or {@code "*"}
             * @return this builder
             */
            public Builder corsOrigin(String origin) { return corsOrigins(List.of(origin)); }
            /**
             * How long a browser may cache a preflight, in seconds; {@code 0}
             * omits {@code Access-Control-Max-Age} so the browser uses its own
             * default. Ignored when no origin is configured.
             *
             * @param v cache lifetime (default {@value #DEFAULT_CORS_MAX_AGE_SECONDS}s)
             * @return this builder
             */
            public Builder corsMaxAgeSeconds(long v) { this.corsMaxAgeSeconds = v; return this; }

            /**
             * Enable {@code POST {prefix}/__introspect_token__}, which resolves an
             * opaque bearer credential to a principal for a reverse proxy that
             * must know the caller's identity before it can authorize.
             *
             * <p>Off unless called. A disabled worker still answers the path
             * definitively ({@code 404 not_enabled}) while holding no resolver and
             * looking nothing up — a caller that reads anything else as transient
             * would otherwise retry forever against a worker that will never
             * support the feature.
             *
             * <p>The resolver takes the credential and nothing else, deliberately:
             * see {@link TokenResolver} for the four ways replaying it through this
             * server's own {@link Authenticator} breaks. It never returns claims;
             * see {@link TokenIdentity}.
             *
             * @param resolver resolves the subject credential; {@code null} disables
             *        the endpoint (the default)
             * @param principals principals permitted to introspect. Must name at
             *        least one — there is no permissive default, because
             *        "any authenticated caller" is exactly the configuration that
             *        turns this endpoint into an open oracle
             * @return this builder
             */
            public Builder tokenIntrospection(TokenResolver resolver, List<String> principals) {
                this.introspectResolver = resolver;
                this.introspectPrincipals = principals != null ? principals : List.of();
                return this;
            }
            /**
             * Cache lifetime reported to the asker when a {@link TokenIdentity}
             * names none.
             *
             * <p>Treat it as an authorization window: for any path the asker serves
             * without re-presenting the credential, that is exactly what it is.
             *
             * @param v the TTL in seconds (default
             *        {@value TokenIntrospection#DEFAULT_TTL_SECONDS})
             * @return this builder
             */
            public Builder introspectTtlSeconds(long v) { this.introspectTtlSeconds = v; return this; }
            /**
             * Introspection requests allowed per caller per second.
             *
             * <p>Bounds, rather than closes, the oracle an allowlisted caller whose
             * own credential leaked still has — a ceiling on how fast an attacker
             * converts guesses into answers.
             *
             * @param v the per-second ceiling (default
             *        {@value TokenIntrospection#DEFAULT_RATE_LIMIT_PER_SECOND})
             * @return this builder
             */
            public Builder introspectRateLimitPerSecond(int v) {
                this.introspectRateLimitPerSecond = v; return this;
            }

            /**
             * Build the immutable config.
             *
             * @return the validated {@link Config}
             * @throws IllegalArgumentException if a numeric bound is out of range,
             *         or {@link #supportedEncodings(List)} named an unknown codec
             *         (see {@link Config})
             */
            public Config build() {
                return new Config(host, port, prefix, tokenKey, tokenTtlSeconds, authenticator,
                        preHandlers, maxRequestBytes, maxResponseBytes, idleTimeoutMs, zstdLevel,
                        supportedEncodings, tls,
                        advertiseMaxRequestBytes, uploadUrlProvider, maxUploadBytes,
                        advertisedMaxResponseBytes, advertisedMaxExternalizedResponseBytes,
                        proxyProofRequired, proxyAuthHeaders, callStateCacheMaxEntries,
                        stickyEnabled, stickyDefaultTtlSeconds, stickyEchoHeaders, exposeTestDrainAdmin,
                        describeProvider, corsOrigins, corsMaxAgeSeconds,
                        introspectResolver, introspectPrincipals,
                        introspectTtlSeconds, introspectRateLimitPerSecond);
            }
        }

        /**
         * Return a copy of this config with {@code describeProvider} set. Used by
         * worker libraries that receive a fully-built config and layer the
         * landing surface on top.
         *
         * @param p the describe provider to attach
         * @return a copy of this config with the provider set
         */
        public Config withDescribeProvider(DescribeProvider p) {
            return new Config(host, port, prefix, tokenKey, tokenTtlSeconds, authenticator,
                    preHandlers, maxRequestBytes, maxResponseBytes, idleTimeoutMs, zstdLevel,
                    supportedEncodings, tls,
                    advertiseMaxRequestBytes, uploadUrlProvider, maxUploadBytes,
                    advertisedMaxResponseBytes, advertisedMaxExternalizedResponseBytes,
                    proxyProofRequired, proxyAuthHeaders, callStateCacheMaxEntries,
                    stickyEnabled, stickyDefaultTtlSeconds, stickyEchoHeaders, exposeTestDrainAdmin,
                    p, corsOrigins, corsMaxAgeSeconds,
                    introspectResolver, introspectPrincipals,
                    introspectTtlSeconds, introspectRateLimitPerSecond);
        }
    }

    /**
     * Bind the connector and start accepting requests. After this returns,
     * {@link #port()} reflects the actual bound port.
     *
     * @throws Exception if Jetty fails to start (e.g. the port is in use)
     */
    public void start() throws Exception {
        jetty.start();
        this.port = ((ServerConnector) jetty.getConnectors()[0]).getLocalPort();
    }

    /**
     * The actual listen port, useful when the config requested port {@code 0}.
     *
     * @return the bound listen port (resolved after {@link #start()}; {@code 0} before)
     */
    public int port() { return port; }

    /**
     * Gracefully stop the server, waiting up to the configured stop timeout for
     * in-flight requests.
     *
     * @throws Exception if Jetty fails to stop cleanly
     */
    public void stop() throws Exception { jetty.stop(); }

    /**
     * Block until the server thread terminates.
     *
     * @throws InterruptedException if the calling thread is interrupted while waiting
     */
    public void join() throws InterruptedException { jetty.join(); }

    // --- Servlet ---------------------------------------------------------

    /** Single servlet that dispatches health / unary / stream sub-paths. */
    /** Mint a 16-char hex correlation id, matching the reference's shape. */
    private static String newRequestId() {
        return java.util.UUID.randomUUID().toString().replace("-", "").substring(0, 16);
    }

    private final class RouterServlet extends HttpServlet {

        @Override
        protected void service(HttpServletRequest req, HttpServletResponse resp) throws IOException {
            // Set capability headers on every response (parity with the Python
            // _CapabilitiesMiddleware: announce externalisation contract upfront).
            applyCapabilityHeaders(req, resp);
            // Echo the caller's correlation id, or mint one. Set before
            // dispatch so it survives every exit path — the error responses
            // are precisely the ones someone later grep's the log for.
            String requestId = req.getHeader(HttpHeaders.REQUEST_ID);
            if (requestId == null || requestId.isBlank() || requestId.length() > 128) {
                requestId = newRequestId();
            }
            resp.setHeader(HttpHeaders.REQUEST_ID, requestId);
            // Before dispatch so the grant rides every answer — a 401 or a 413
            // a browser cannot read is a network error with no explanation.
            if (cors != null) cors.apply(req, resp);
            // Access-log records produced during dispatch are parked here and
            // emitted on close, once the encoded body has been measured.
            // response_bytes cannot be read where the record is written:
            // compression runs after the handler, so a record emitted there
            // could only ever report the uncompressed size.
            try (AccessLogScope access = AccessLogScope.open(requestId)) {
                try {
                    super.service(req, resp);
                } catch (AuthUnavailableException e) {
                    // Caught here rather than at each authenticate() call site so
                    // every route answers an outage the same way. A 401 would tell
                    // every caller to re-authenticate against a service that is
                    // simply down, and invite them to negative-cache the outage.
                    writeServiceUnavailable(resp, e);
                } catch (jakarta.servlet.ServletException se) {
                    throw new IOException(se);
                } finally {
                    access.httpStatus(resp.getStatus());
                }
            }
        }

        @Override
        protected void doOptions(HttpServletRequest req, HttpServletResponse resp) {
            // Used by clients as the canonical capability-discovery target.
            resp.setHeader("Cache-Control", "public, max-age=300");
            resp.setStatus(HttpServletResponse.SC_OK);
        }

        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
            if (runPreHandlers(req, resp)) return;
            String p = pathInfo(req);
            if ("".equals(p) || "/".equals(p)) {
                // Root: content-negotiate. Browsers (Accept: text/html) get the
                // shared static landing page; health checks / ?format=json get
                // the JSON status.
                if (wantsHtml(req) && LANDING_HTML != null) {
                    resp.setStatus(HttpServletResponse.SC_OK);
                    resp.setContentType("text/html; charset=utf-8");
                    resp.getOutputStream().write(LANDING_HTML);
                    return;
                }
                writeStatusJson(resp);
                return;
            }
            if ("health".equals(p)) {
                writeStatusJson(resp);
                return;
            }
            if (describeProvider != null && serveDescribe(p, resp)) {
                return;
            }
            resp.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED);
        }

        /** Serve {@code describe.json} and {@code describe/{c}/{s}/{t}.json}; returns
         *  {@code true} when the path matched (response already written). */
        private boolean serveDescribe(String p, HttpServletResponse resp) throws IOException {
            if ("describe.json".equals(p)) {
                writeRawJson(resp, HttpServletResponse.SC_OK,
                        describeProvider.describeJson(rpc.serverId(), oauthActive()));
                return true;
            }
            if (p.startsWith("describe/") && p.endsWith(".json")) {
                String rest = p.substring("describe/".length(), p.length() - ".json".length());
                String[] parts = rest.split("/");
                if (parts.length == 3) {
                    String cols = describeProvider.columnsJson(
                            urlDecode(parts[0]), urlDecode(parts[1]), urlDecode(parts[2]));
                    if (cols == null) {
                        writeJson(resp, HttpServletResponse.SC_NOT_FOUND,
                                Map.of("error", "object not found"));
                    } else {
                        writeRawJson(resp, HttpServletResponse.SC_OK, cols);
                    }
                    return true;
                }
            }
            return false;
        }

        private void writeStatusJson(HttpServletResponse resp) throws IOException {
            writeJson(resp, HttpServletResponse.SC_OK, Map.of(
                    "status", "ok",
                    "server_id", rpc.serverId(),
                    "protocol", rpc.protocolName()));
        }

        @Override
        protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
            if (runPreHandlers(req, resp)) return;
            String rest = pathInfo(req);
            if (rest.isEmpty() || "health".equals(rest)) {
                resp.sendError(HttpServletResponse.SC_NOT_FOUND);
                return;
            }
            if (exposeTestDrainAdmin && StickyHeaders.TEST_DRAIN_PATH.equals(rest)) {
                handleTestDrain(req, resp, true);
                return;
            }
            if (TokenIntrospection.ENDPOINT.equals(rest)) {
                handleIntrospect(req, resp);
                return;
            }
            if (UPLOAD_URL_METHOD.equals(rest) || (UPLOAD_URL_METHOD + "/init").equals(rest)) {
                handleUploadUrl(req, resp);
                return;
            }
            boolean stream = rest.endsWith("/init") || rest.endsWith("/exchange");
            boolean init = rest.endsWith("/init");
            String methodName = !stream ? rest
                    : rest.substring(0, rest.length() - (init ? "/init".length() : "/exchange".length()));
            // An RPC method name never contains a slash, so a path still holding
            // one after the /init and /exchange suffixes names no route at all.
            // Answering 404 rather than dispatching it keeps a mistyped or
            // wrong-prefix POST a definitive client error — dispatched, a
            // non-Arrow body dies in the IPC reader and surfaces as a 500, which
            // a caller reads as "retry later".
            if (methodName.isEmpty() || methodName.indexOf('/') >= 0) {
                resp.sendError(HttpServletResponse.SC_NOT_FOUND);
                return;
            }
            if (stream) {
                handleStream(req, resp, methodName, init);
                return;
            }
            handleUnary(req, resp, methodName);
        }

        @Override
        protected void doDelete(HttpServletRequest req, HttpServletResponse resp) throws IOException {
            if (runPreHandlers(req, resp)) return;
            String rest = pathInfo(req);
            if (StickyHeaders.SESSION_PATH.equals(rest)) {
                handleSessionDelete(req, resp);
                return;
            }
            if (exposeTestDrainAdmin && StickyHeaders.TEST_DRAIN_PATH.equals(rest)) {
                handleTestDrain(req, resp, false);
                return;
            }
            resp.sendError(HttpServletResponse.SC_NOT_FOUND);
        }

        private boolean runPreHandlers(HttpServletRequest req, HttpServletResponse resp) throws IOException {
            for (HttpPreHandler h : preHandlers) {
                if (h.handle(req, resp)) return true;
            }
            return false;
        }

        private String pathInfo(HttpServletRequest req) {
            String pi = req.getPathInfo();
            if (pi == null) return "";
            if (pi.startsWith("/")) pi = pi.substring(1);
            return pi;
        }
    }

    /** Set capability-advertisement headers on every response. */
    private void applyCapabilityHeaders(HttpServletRequest req, HttpServletResponse resp) {
        if (advertiseMaxRequestBytes) {
            resp.setHeader(MAX_REQUEST_BYTES_HEADER, Long.toString(maxRequestBytes));
        }
        if (advertisedMaxResponseBytes > 0) {
            resp.setHeader(MAX_RESPONSE_BYTES_HEADER, Long.toString(advertisedMaxResponseBytes));
        }
        if (advertisedMaxExternalizedResponseBytes > 0) {
            resp.setHeader(MAX_EXTERNALIZED_RESPONSE_BYTES_HEADER,
                    Long.toString(advertisedMaxExternalizedResponseBytes));
        }
        // Always present so capability-aware clients can decide whether to
        // expect externalised payloads.
        resp.setHeader(EXTERNALIZATION_ENABLED_HEADER,
                rpc.externalConfig() != null ? "true" : "false");
        // Advertise the codec set so a client that defaulted to zstd can switch
        // to gzip when zstd is disabled (mirrors vgi-python's factory).
        resp.setHeader(SUPPORTED_ENCODINGS_HEADER, enabledEncodings());
        if (uploadUrlProvider != null) {
            resp.setHeader(UPLOAD_URL_HEADER, "true");
            if (maxUploadBytes != null) {
                resp.setHeader(MAX_UPLOAD_BYTES_HEADER, Long.toString(maxUploadBytes));
            }
        }
        // Advertised only in REQUIRE mode: a proxy has no other way to tell an
        // enforcing worker from one silently ignoring the proof header, which is
        // the misconfiguration that turns the whole feature into a no-op.
        if (proxyProofRequired) {
            resp.setHeader(ProxyProof.PROOF_REQUIRED_HEADER, "true");
        }
        // Absent, never "false", when disabled: a proxy preflights on presence.
        if (introspection != null) {
            resp.setHeader(TokenIntrospection.ENABLED_HEADER, "true");
        }
        if (stickyEnabled) {
            resp.setHeader(StickyHeaders.STICKY_ENABLED, "true");
            resp.setHeader(StickyHeaders.STICKY_TTL, Long.toString(stickyDefaultTtlSeconds));
            if (!stickyEchoHeaders.isEmpty()) {
                resp.setHeader(StickyHeaders.STICKY_ECHO,
                        String.join(",", stickyEchoHeaders.keySet()));
            }
        }
    }

    /**
     * The response headers a browser client may read, built from the same
     * conditions as {@link #applyCapabilityHeaders}.
     *
     * <p>The two must stay in lockstep: an advertised-but-unexposed capability
     * is invisible to JavaScript and to nothing else, so it survives every test
     * driven by an HTTP client that ignores CORS. Adding a header there without
     * adding it here ships a server a browser can read no capability from.
     */
    private List<String> corsExposeHeaders() {
        List<String> expose = new ArrayList<>(List.of(
                HttpHeaders.WWW_AUTHENTICATE,
                HttpHeaders.X_VGI_CONTENT_ENCODING,
                // How a client tells a 200 carrying an error batch from a 200
                // carrying a result — unreadable, the two are indistinguishable.
                RPC_ERROR_HEADER,
                EXTERNALIZATION_ENABLED_HEADER,
                SUPPORTED_ENCODINGS_HEADER,
                // Describes a rejection rather than a capability, so it is never
                // advertised on /health — but a browser that cannot read it is
                // back to guessing the 401 reason out of the body.
                HttpHeaders.VGI_AUTH_REASON,
                // Also never advertised on /health: it rides every response
                // including the failures, and it is what lets a browser client
                // quote an id the server's own log can be searched for.
                HttpHeaders.REQUEST_ID));
        if (advertiseMaxRequestBytes) expose.add(MAX_REQUEST_BYTES_HEADER);
        if (advertisedMaxResponseBytes > 0) expose.add(MAX_RESPONSE_BYTES_HEADER);
        if (advertisedMaxExternalizedResponseBytes > 0) expose.add(MAX_EXTERNALIZED_RESPONSE_BYTES_HEADER);
        if (uploadUrlProvider != null) {
            expose.add(UPLOAD_URL_HEADER);
            if (maxUploadBytes != null) expose.add(MAX_UPLOAD_BYTES_HEADER);
        }
        if (proxyProofRequired) expose.add(ProxyProof.PROOF_REQUIRED_HEADER);
        if (introspection != null) expose.add(TokenIntrospection.ENABLED_HEADER);
        if (!proxyHint.isEmpty()) expose.add(HttpHeaders.VGI_AUTH_PROXY_REQUIRED);
        if (stickyEnabled) {
            expose.add(StickyHeaders.STICKY_ENABLED);
            expose.add(StickyHeaders.STICKY_TTL);
            // Not advertisements but per-response state: a browser client inside
            // a session helper reads the minted token and the close signal here.
            expose.add(StickyHeaders.SESSION);
            expose.add(StickyHeaders.SESSION_CLOSE);
            if (!stickyEchoHeaders.isEmpty()) {
                expose.add(StickyHeaders.STICKY_ECHO);
                for (String name : stickyEchoHeaders.keySet()) {
                    expose.add(StickyHeaders.ECHO_PREFIX + name);
                }
            }
        }
        return expose;
    }

    /**
     * True when the request path should bypass the {@code maxRequestBytes} cap.
     * Mirrors the Python {@code _MaxRequestBytesMiddleware.exempt_prefixes}:
     * {@code __upload_url__} and {@code health} payloads are intrinsically tiny.
     */
    private boolean isMaxBytesExempt(String pathRest) {
        return pathRest.equals("health")
                || pathRest.equals(UPLOAD_URL_METHOD)
                || pathRest.startsWith(UPLOAD_URL_METHOD + "/");
    }

    private void handleUploadUrl(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        if (uploadUrlProvider == null) {
            resp.sendError(HttpServletResponse.SC_NOT_FOUND);
            return;
        }
        // Read & validate the request batch (carries vgi_rpc.method=__upload_url__ and a count column)
        byte[] body;
        try {
            // Exempt from maxRequestBytes — _MaxRequestBytesMiddleware skips this prefix.
            body = readBodyUnbounded(req);
        } catch (IOException ioe) {
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "could not read request body");
            return;
        }
        int count = 1;
        try (IpcStreamReader r = new IpcStreamReader(new ByteArrayInputStream(body), Allocators.root())) {
            Map<String, String> meta = r.readNextBatch();
            if (meta == null) {
                resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "empty request body");
                return;
            }
            String mname = meta.get(Metadata.RPC_METHOD);
            if (!UPLOAD_URL_METHOD.equals(mname)) {
                resp.sendError(HttpServletResponse.SC_BAD_REQUEST,
                        "method mismatch: expected " + UPLOAD_URL_METHOD);
                return;
            }
            VectorSchemaRoot root = r.root();
            if (root.getRowCount() > 0) {
                org.apache.arrow.vector.FieldVector v = root.getVector("count");
                if (v instanceof BigIntVector bi && !bi.isNull(0)) {
                    long c = bi.get(0);
                    if (c > 0) count = (int) Math.min(c, MAX_UPLOAD_URL_COUNT);
                }
            }
        } catch (IOException ioe) {
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "invalid Arrow IPC body");
            return;
        }
        count = Math.max(1, Math.min(count, MAX_UPLOAD_URL_COUNT));

        // Generate the URLs and write the response IPC stream.
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (VectorSchemaRoot root = VectorSchemaRoot.create(UPLOAD_URL_SCHEMA, Allocators.root());
             IpcStreamWriter w = new IpcStreamWriter(out)) {
            VarCharVector uploadVec = (VarCharVector) root.getVector("upload_url");
            VarCharVector downloadVec = (VarCharVector) root.getVector("download_url");
            TimeStampMicroTZVector expiresVec = (TimeStampMicroTZVector) root.getVector("expires_at");
            uploadVec.allocateNew();
            downloadVec.allocateNew();
            expiresVec.allocateNew(count);
            try {
                for (int i = 0; i < count; i++) {
                    UploadUrlProvider.UploadUrl url;
                    try {
                        url = uploadUrlProvider.generateUploadUrl();
                    } catch (Exception e) {
                        throw new IOException("upload URL generation failed: " + e.getMessage(), e);
                    }
                    uploadVec.setSafe(i, url.uploadUrl().getBytes(StandardCharsets.UTF_8));
                    downloadVec.setSafe(i, url.downloadUrl().getBytes(StandardCharsets.UTF_8));
                    Instant exp = url.expiresAt() != null ? url.expiresAt() : Instant.now().plusSeconds(3600);
                    long micros = exp.getEpochSecond() * 1_000_000L + exp.getNano() / 1_000L;
                    expiresVec.setSafe(i, micros);
                }
                root.setRowCount(count);
                w.writeBatch(root, null);
            } catch (IOException ioe) {
                // Fall through: an empty (or partial) IPC body is a server error
                resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                Wire.writeErrorStream(out, UPLOAD_URL_SCHEMA, ioe, rpc.serverId());
                writeArrowResponse(req, resp, out.toByteArray());
                return;
            }
        }
        writeArrowResponse(req, resp, out.toByteArray());
    }

    private byte[] readBodyUnbounded(HttpServletRequest req) throws IOException {
        try (InputStream in = req.getInputStream();
             ByteArrayOutputStream buf = new ByteArrayOutputStream()) {
            byte[] chunk = new byte[8192];
            int n;
            while ((n = in.read(chunk)) > 0) buf.write(chunk, 0, n);
            byte[] body = buf.toByteArray();
            AccessLogScope.recordRequestBytes(body.length);
            return maybeDecodeRequestBody(req, body);
        }
    }

    private void handleUnary(HttpServletRequest req, HttpServletResponse resp, String method) throws IOException {
        byte[] body;
        try {
            body = readBody(req);
        } catch (PayloadTooLargeException e) {
            writePayloadTooLarge(resp, e);
            return;
        } catch (UnsupportedContentEncodingException e) {
            writeUnsupportedEncoding(resp, e);
            return;
        }

        AuthContext auth;
        try {
            auth = authenticator.authenticate(req);
        } catch (AuthException e) {
            writeUnauthorized(resp, e);
            return;
        }

        // Build sticky scope (after auth so we have the principal for AAD).
        SessionScope scope;
        try {
            scope = buildSessionScope(req, auth);
        } catch (SessionLostError e) {
            writeSessionLostResponse(req, resp, e);
            return;
        }

        BoundedByteArrayOutputStream out = new BoundedByteArrayOutputStream(maxResponseBytes);
        Map<String, Object> md = buildTransportMetadata(req);
        try {
            try (AutoCloseable authPop = AuthScope.push(auth, md);
                 AutoCloseable sessPop = SessionScope.push(scope);
                 InMemoryTransport t = new InMemoryTransport(body, out)) {
                rpc.serveOne(t);
            } catch (PayloadTooLargeException e) {
                writePayloadTooLarge(resp, e);
                return;
            } catch (Exception e) {
                throw new IOException(e);
            }
        } finally {
            // Release the per-session lock acquired in buildSessionScope or
            // CallContext.openSession; idempotent — closeSession may have
            // released it already on the close path.
            releaseSessionLock(scope);
        }
        emitResponseCookies(resp, md);
        emitSessionResponseHeaders(resp, scope);
        // Operator-facing response cap: post-flush enforcement.  Mirrors the
        // Python reference's strict-fail — overshoot replaces the body with
        // an Arrow EXCEPTION batch carrying the literal "max_response_bytes"
        // token, surfaced via 200 + X-VGI-RPC-Error: true so RPC clients
        // observe RpcError, not a transport failure.
        if (advertisedMaxResponseBytes > 0 && out.size() > advertisedMaxResponseBytes) {
            writeResponseCapError(req, resp, method, out.size(), advertisedMaxResponseBytes);
            return;
        }
        writeArrowResponse(req, resp, out.toByteArray());
    }

    /** Build the per-request sticky scope. Throws {@link SessionLostError}
     *  with a uniform message when a {@code VGI-Session} header was
     *  presented but doesn't match a live registry entry (no probing). */
    private SessionScope buildSessionScope(HttpServletRequest req, AuthContext auth) {
        String principal = auth != null && auth.principal() != null ? auth.principal() : "";
        String principalKey = computePrincipalKey(auth);
        boolean optIn = "true".equalsIgnoreCase(req.getHeader(StickyHeaders.SESSION_ACCEPT));
        SessionScope scope = new SessionScope(optIn, stickyEnabled, principalKey, principal,
                rpc.serverId(), sessionTokenKey, sessionRegistry);
        if (!stickyEnabled) return scope;
        sessionRegistry.ensureReaperStarted();
        String tokenStr = req.getHeader(StickyHeaders.SESSION);
        if (tokenStr == null) return scope;
        SessionToken parsed;
        try {
            parsed = SessionToken.unpack(tokenStr.getBytes(StandardCharsets.US_ASCII),
                    sessionTokenKey, principal);
        } catch (IllegalArgumentException e) {
            // Uniform failure message regardless of why the token didn't open
            // (closes the timing / log side-channel between tag-fail, AAD-fail,
            // wrong-server, expired-entry, and miss).
            throw new SessionLostError("session token rejected");
        }
        if (!parsed.serverId().equals(rpc.serverId())) {
            throw new SessionLostError("session token rejected");
        }
        SessionRegistry.Entry entry = sessionRegistry.get(parsed.sessionId(), principalKey);
        if (entry == null) {
            throw new SessionLostError("session token rejected");
        }
        // Serialize concurrent calls on the same session: acquire the
        // per-entry lock before dispatch. Released by releaseSessionLock()
        // in the response-cleanup path.
        entry.lock().lock();
        scope.bindEntry(entry, SessionScope.ACTION_RESUME);
        return scope;
    }

    /** Release the per-session lock held while dispatch ran. Safe to call
     *  on any path — drops silently when the current thread isn't the
     *  holder (e.g. {@code CallContext.closeSession} already released it). */
    private static void releaseSessionLock(SessionScope scope) {
        if (scope == null) return;
        SessionRegistry.Entry entry = scope.entry();
        if (entry == null) return;
        java.util.concurrent.locks.ReentrantLock lock = entry.lock();
        if (lock != null && lock.isHeldByCurrentThread()) {
            try { lock.unlock(); } catch (IllegalMonitorStateException ignore) { }
        }
    }

    private static String computePrincipalKey(AuthContext auth) {
        if (auth == null || !auth.authenticated()) return "\0anonymous";
        String domain = auth.domain() != null ? auth.domain() : "";
        String principal = auth.principal() != null ? auth.principal() : "";
        return "\1" + domain + "\0" + principal;
    }

    /** Mint response headers for sticky-session opens / closes. */
    private void emitSessionResponseHeaders(HttpServletResponse resp, SessionScope scope) {
        if (scope == null) return;
        if (scope.mintTokenB64() != null) {
            resp.setHeader(StickyHeaders.SESSION, scope.mintTokenB64());
            for (Map.Entry<String, String> e : stickyEchoHeaders.entrySet()) {
                resp.setHeader(StickyHeaders.ECHO_PREFIX + e.getKey(), e.getValue());
            }
        }
        if (scope.closeSignal()) {
            resp.setHeader(StickyHeaders.SESSION_CLOSE, "true");
        }
    }

    /** Replace the response with an Arrow EXCEPTION-batch stream carrying
     *  {@link SessionLostError} so the client receives the typed error
     *  with {@code error_kind = "session_lost"} just like a dispatch-time raise. */
    private void writeSessionLostResponse(HttpServletRequest req, HttpServletResponse resp,
                                           SessionLostError e) throws IOException {
        ByteArrayOutputStream errOut = new ByteArrayOutputStream();
        Wire.writeErrorStream(errOut, RpcStream.EMPTY_SCHEMA, e, rpc.serverId());
        resp.setStatus(HttpServletResponse.SC_OK);
        resp.setHeader(RPC_ERROR_HEADER, "true");
        writeArrowResponse(req, resp, errOut.toByteArray());
    }

    /** {@code DELETE /__session__}: best-effort eviction. Always 200, no
     *  information leak (clients can't probe whether a session existed). */
    private void handleSessionDelete(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        if (!stickyEnabled) {
            resp.setStatus(HttpServletResponse.SC_OK);
            return;
        }
        String tokenStr = req.getHeader(StickyHeaders.SESSION);
        if (tokenStr == null || tokenStr.isEmpty()) {
            resp.setStatus(HttpServletResponse.SC_OK);
            return;
        }
        AuthContext auth;
        try { auth = authenticator.authenticate(req); }
        catch (AuthException e) { resp.setStatus(HttpServletResponse.SC_OK); return; }
        String principal = auth != null && auth.principal() != null ? auth.principal() : "";
        String principalKey = computePrincipalKey(auth);
        try {
            SessionToken parsed = SessionToken.unpack(
                    tokenStr.getBytes(StandardCharsets.US_ASCII), sessionTokenKey, principal);
            if (parsed.serverId().equals(rpc.serverId())) {
                // Pass the principalKey so close refuses cross-principal eviction
                // (defense-in-depth on top of the AAD binding in the token).
                sessionRegistry.close(parsed.sessionId(), principalKey);
            }
        } catch (RuntimeException ignore) {
            // Wrong key / tampered / wrong server — silently no-op (no probing).
        }
        resp.setStatus(HttpServletResponse.SC_OK);
        resp.setHeader(StickyHeaders.SESSION_CLOSE, "true");
    }

    /** {@code POST/DELETE /__test_drain__}: test-only admin endpoint flipping
     *  the drain flag. Exposed only when {@code exposeTestDrainAdmin=true},
     *  and additionally restricted to loopback callers so an operator who
     *  accidentally ships the flag enabled can't be DoS'd by an external
     *  drain trigger. */
    private void handleTestDrain(HttpServletRequest req, HttpServletResponse resp, boolean drain) throws IOException {
        if (!stickyEnabled) { resp.sendError(HttpServletResponse.SC_NOT_FOUND); return; }
        if (!isLoopbackRequest(req)) {
            resp.sendError(HttpServletResponse.SC_FORBIDDEN);
            return;
        }
        sessionRegistry.setDraining(drain);
        resp.setStatus(HttpServletResponse.SC_NO_CONTENT);
    }

    /**
     * {@code POST {prefix}/__introspect_token__}: resolve an opaque credential
     * to a principal, or say definitively that this worker will not.
     *
     * <p>An {@link AuthException} from the caller's own authenticator collapses
     * onto the same 403 a non-allowlisted caller gets. Distinguishing "your
     * credential is bad" from "your credential is fine but you may not
     * introspect" would tell an unauthorized caller which of the two it is, and
     * both are equally final. An {@link AuthUnavailableException} is not caught
     * here — it is not a rejection, and the servlet boundary renders it as 503.
     */
    private void handleIntrospect(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        if (introspection == null) {
            TokenIntrospection.writeNotEnabled(resp);
            return;
        }
        AuthContext auth;
        try {
            auth = authenticator.authenticate(req);
        } catch (AuthException e) {
            auth = AuthContext.ANONYMOUS;
        }
        introspection.handle(req, resp, auth);
    }

    /**
     * Render the transient-failure answer: {@code 503} with {@code Retry-After}.
     *
     * <p>The counterpart to {@link #writeUnauthorized}. A 401 says "your
     * credential is bad" and invites a caller to negative-cache; this says
     * "I could not find out", which a caller must retry instead.
     */
    private static void writeServiceUnavailable(HttpServletResponse resp, AuthUnavailableException e)
            throws IOException {
        resp.setHeader("Retry-After", Integer.toString(e.retryAfterSeconds()));
        resp.setHeader("Cache-Control", "no-store");
        writeJson(resp, HttpServletResponse.SC_SERVICE_UNAVAILABLE, Map.of(
                "error", "service_unavailable",
                "detail", e.getMessage() != null ? e.getMessage() : ""));
    }

    private static boolean isLoopbackRequest(HttpServletRequest req) {
        String remote = req.getRemoteAddr();
        if (remote == null) return false;
        // Cover IPv4 + IPv6 loopback forms and the IPv4-mapped IPv6 variant.
        return remote.equals("127.0.0.1") || remote.equals("0:0:0:0:0:0:0:1")
                || remote.equals("::1") || remote.equals("::ffff:127.0.0.1");
    }

    /** Replace the response with an Arrow EXCEPTION-batch IPC stream when the
     *  body overshoots the operator-facing response cap. */
    private void writeResponseCapError(HttpServletRequest req, HttpServletResponse resp,
                                       String method, long actual, long limit) throws IOException {
        RuntimeException overshoot = new RuntimeException(
                "HTTP body exceeds max_response_bytes (" + actual + " > " + limit
                        + ") for method '" + method + "'");
        ByteArrayOutputStream errOut = new ByteArrayOutputStream();
        Wire.writeErrorStream(errOut, RpcStream.EMPTY_SCHEMA, overshoot, rpc.serverId());
        // 200 + X-VGI-RPC-Error so clients that discard 5xx bodies still parse
        // the IPC error batch.  Matches Python's _set_http_status.
        resp.setStatus(HttpServletResponse.SC_OK);
        resp.setHeader(RPC_ERROR_HEADER, "true");
        writeArrowResponse(req, resp, errOut.toByteArray());
    }

    private static Map<String, Object> buildTransportMetadata(HttpServletRequest req) {
        Map<String, Object> md = new LinkedHashMap<>();
        String remote = req.getRemoteAddr();
        if (remote != null) md.put("remote_addr", remote);
        String ua = req.getHeader(HttpHeaders.USER_AGENT);
        if (ua != null) md.put("user_agent", ua);
        // Request cookies (CallContext.cookies()) + a mutable sink that
        // CallContext.setCookie() writes into; emitResponseCookies() drains it
        // into Set-Cookie headers after dispatch.
        Map<String, String> reqCookies = new LinkedHashMap<>();
        jakarta.servlet.http.Cookie[] cookies = req.getCookies();
        if (cookies != null) {
            for (jakarta.servlet.http.Cookie c : cookies) reqCookies.put(c.getName(), c.getValue());
        }
        md.put(CallContext.REQUEST_COOKIES_KEY, reqCookies);
        md.put(CallContext.RESPONSE_COOKIES_KEY, new LinkedHashMap<String, String>());
        return md;
    }

    /** Drain any cookies the handler set via {@link CallContext#setCookie} into
     *  {@code Set-Cookie} response headers. */
    @SuppressWarnings("unchecked")
    private static void emitResponseCookies(HttpServletResponse resp, Map<String, Object> md) {
        Object sink = md.get(CallContext.RESPONSE_COOKIES_KEY);
        if (!(sink instanceof Map)) return;
        for (Map.Entry<String, String> e : ((Map<String, String>) sink).entrySet()) {
            resp.addHeader("Set-Cookie",
                    e.getKey() + "=" + e.getValue() + "; Path=/; HttpOnly; SameSite=Strict");
        }
    }

    /**
     * Render the standardized 401 of {@code docs/unauthorized-spec.md} §4: the
     * reason header, a no-store cache directive, the proxy note when this
     * service's auth depends on a proxy, and the JSON envelope.
     *
     * <p>§4.2 lets a service always answer JSON — what it must never do is
     * answer a non-HTML request with HTML — and this port takes that option,
     * so {@code Accept} does not change the body. The reason header, the part
     * clients actually parse, is set either way.</p>
     *
     * <p>Both {@code VGI-} headers describe a rejection, so they are set here
     * and nowhere else: they are not capability advertisements and must not
     * appear on a successful response.</p>
     */
    private void writeUnauthorized(HttpServletResponse resp, AuthException e) throws IOException {
        if (e.wwwAuthenticate() != null) {
            resp.setHeader(HttpHeaders.WWW_AUTHENTICATE, e.wwwAuthenticate());
        }
        AuthReason reason = e.reason();
        resp.setHeader(HttpHeaders.VGI_AUTH_REASON, reason.code());
        if (!proxyHint.isEmpty()) {
            resp.setHeader(HttpHeaders.VGI_AUTH_PROXY_REQUIRED, "true");
        }
        // A 401 is per-request and flips to 200 on the next attempt with a
        // credential, so no shared cache may hold it.
        resp.setHeader("Cache-Control", "no-store");
        String detail = e.getMessage() != null ? e.getMessage() : "";
        writeJson(resp, HttpServletResponse.SC_UNAUTHORIZED,
                Unauthorized.envelope(reason, detail, proxyHint));
    }

    private static void writeJson(HttpServletResponse resp, int status, Map<String, ?> body) throws IOException {
        resp.setStatus(status);
        resp.setContentType(MediaTypes.APPLICATION_JSON);
        resp.getOutputStream().write(JSON.writeValueAsBytes(body));
    }

    /** Write a pre-serialized JSON string as the response body. */
    private static void writeRawJson(HttpServletResponse resp, int status, String json) throws IOException {
        resp.setStatus(status);
        resp.setContentType(MediaTypes.APPLICATION_JSON);
        resp.getOutputStream().write(json.getBytes(StandardCharsets.UTF_8));
    }

    /** True when the request prefers HTML: not {@code ?format=json}, and the
     *  {@code Accept} header advertises {@code text/html} (browsers). */
    private static boolean wantsHtml(HttpServletRequest req) {
        if ("json".equals(req.getParameter("format"))) return false;
        String accept = req.getHeader("Accept");
        return accept != null && accept.contains("text/html");
    }

    private static String urlDecode(String s) {
        return URLDecoder.decode(s, StandardCharsets.UTF_8);
    }

    /** Whether an interactive authenticator (e.g. OAuth/PKCE) is active. Surfaced
     *  as the {@code oauth} flag of {@code describe.json}. */
    private boolean oauthActive() {
        return authenticator != Authenticator.ANONYMOUS;
    }

    private byte[] readBody(HttpServletRequest req) throws IOException {
        long contentLength = req.getContentLengthLong();
        if (contentLength > maxRequestBytes) {
            throw new PayloadTooLargeException("request body Content-Length " + contentLength
                    + " exceeds maxRequestBytes=" + maxRequestBytes
                    + "; large batches must use the external-location protocol");
        }
        byte[] body;
        try (InputStream in = req.getInputStream();
             BoundedByteArrayOutputStream buf = new BoundedByteArrayOutputStream(maxRequestBytes)) {
            copyBounded(in, buf, maxRequestBytes);
            body = buf.toByteArray();
        }
        // Measured before decompression: this is what the peer actually sent,
        // and therefore what the link was billed for.
        AccessLogScope.recordRequestBytes(body.length);
        return maybeDecodeRequestBody(req, body);
    }

    private static void copyBounded(InputStream in, OutputStream out, long limit) throws IOException {
        byte[] chunk = new byte[8192];
        long total = 0;
        int n;
        while ((n = in.read(chunk)) > 0) {
            total += n;
            if (total > limit) {
                throw new PayloadTooLargeException("request body exceeds " + limit
                        + " bytes; large batches must use the external-location protocol");
            }
            out.write(chunk, 0, n);
        }
    }

    private void writeArrowResponse(HttpServletRequest req, HttpServletResponse resp, byte[] body) throws IOException {
        resp.setContentType(ARROW_CONTENT_TYPE);
        ResponseEncoding choice = chooseResponseEncoding(req, supportedEncodings);
        byte[] encoded = encodeArrowBody(resp, choice, body, zstdLevel);
        // Post-compression, so this is the egress figure. The logical Arrow size
        // the worker produced is a different number by up to three orders of
        // magnitude, and is reported separately as output_bytes.
        AccessLogScope.recordResponseBytes(encoded.length);
        resp.getOutputStream().write(encoded);
    }

    /** Compress {@code body} with the negotiated codec (if any) and stamp the
     *  codec on the response. Returns the bytes to write.
     *
     *  <p>A codec negotiated only via {@link HttpHeaders#X_VGI_ACCEPT_ENCODING}
     *  is announced on {@link HttpHeaders#X_VGI_CONTENT_ENCODING} rather than
     *  the standard {@code Content-Encoding}: such a client reached for the
     *  custom header because its fetch/proxy layer mangles or silently
     *  auto-decodes standard content-coding, so the response must not claim
     *  one. */
    static byte[] encodeArrowBody(HttpServletResponse resp, ResponseEncoding choice,
                                   byte[] body, int zstdLevel) throws IOException {
        String enc = choice.encoding();
        if (enc == null) return body;
        resp.setHeader(choice.usedCustomHeader()
                        ? HttpHeaders.X_VGI_CONTENT_ENCODING
                        : HttpHeaders.CONTENT_ENCODING,
                enc);
        return MediaTypes.ZSTD.equals(enc) ? Zstd.compress(body, zstdLevel) : gzipCompress(body);
    }

    private static void writePayloadTooLarge(HttpServletResponse resp, RuntimeException e) throws IOException {
        writeJson(resp, HttpServletResponse.SC_REQUEST_ENTITY_TOO_LARGE,
                Map.of("error", e.getMessage()));
    }

    private static void writeUnsupportedEncoding(HttpServletResponse resp,
                                                  UnsupportedContentEncodingException e) throws IOException {
        resp.setHeader(SUPPORTED_ENCODINGS_HEADER, e.supportedEncodings());
        writeJson(resp, HttpServletResponse.SC_UNSUPPORTED_MEDIA_TYPE, Map.of("error", e.getMessage()));
    }

    /**
     * Decode a compressed request body, if the {@code Content-Encoding} names a
     * codec this server is configured for.
     *
     * <p>Gated by the same {@link #supportedEncodings} list that the negotiation
     * walk and the advertisement use: a codec the server does not advertise is
     * one it refuses to decode (HTTP 415, carrying the advertised set so the
     * client can retry correctly). With an empty set that is every codec — a
     * server that states it speaks no compression must not quietly accept a
     * compressed body either.
     */
    private byte[] maybeDecodeRequestBody(HttpServletRequest req, byte[] body) throws IOException {
        String enc = req.getHeader(HttpHeaders.CONTENT_ENCODING);
        if (enc == null || enc.isEmpty()) return body;
        String token = enc.trim().toLowerCase(Locale.ROOT);
        if (token.isEmpty() || MediaTypes.IDENTITY.equals(token)) return body;
        if (!KNOWN_ENCODINGS.contains(token)) {
            throw new UnsupportedContentEncodingException(
                    "unsupported Content-Encoding: " + enc, enabledEncodings());
        }
        if (!supportedEncodings.contains(token)) {
            throw new UnsupportedContentEncodingException(
                    "Content-Encoding '" + token + "' is not enabled on this server", enabledEncodings());
        }
        if (MediaTypes.ZSTD.equals(token)) {
            long size = Zstd.getFrameContentSize(body);
            if (size <= 0) throw new IOException("zstd frame has unknown size");
            byte[] out = new byte[(int) size];
            long ret = Zstd.decompress(out, body);
            if (Zstd.isError(ret)) {
                throw new IOException("zstd decompress failed: " + Zstd.getErrorName(ret));
            }
            return out;
        }
        return gzipDecompress(body, maxRequestBytes);
    }

    /**
     * Value of {@link #SUPPORTED_ENCODINGS_HEADER}: the codecs this server both
     * accepts on requests and produces on responses, in server-preference order,
     * without {@code identity}. Derived from the same list the negotiation walk
     * consults, so advertisement and behaviour cannot drift. The empty string is
     * a legitimate value — "no compression" — and is emitted as a present,
     * empty header rather than omitted.
     */
    private String enabledEncodings() {
        return String.join(", ", supportedEncodings);
    }

    /** Compressed codecs this build can encode at all — the domain
     *  {@link Config#supportedEncodings()} is drawn from. Both encoders are
     *  unconditionally present (zstd-jni is a hard dependency,
     *  {@code java.util.zip} is stdlib), so configuration is the only gate and
     *  any subset — including none — is expressible. */
    private static final List<String> COMPRESSION_CODECS = List.of(MediaTypes.ZSTD, MediaTypes.GZIP);

    /** Tokens {@link #parseEncodingList} recognises: the compressed codecs plus
     *  {@code identity}, which every server can always produce. */
    private static final List<String> KNOWN_ENCODINGS =
            List.of(MediaTypes.ZSTD, MediaTypes.GZIP, MediaTypes.IDENTITY);

    /**
     * Validate and canonicalise a configured codec set: trimmed, lowercased,
     * de-duplicated, order preserved (it is the server's advertised preference).
     *
     * <p>The empty list is a legitimate configuration — "this server speaks no
     * compression" — and is what makes an empty {@link #SUPPORTED_ENCODINGS_HEADER}
     * reachable. {@code identity} is rejected rather than silently dropped: it is
     * always available and never advertised, so naming it means the caller
     * expected it to mean something.
     *
     * @param raw the configured codec names
     * @return the canonical list
     * @throws IllegalArgumentException if a name is null or not a known codec
     */
    static List<String> normalizeEncodings(List<String> raw) {
        List<String> out = new ArrayList<>(raw.size());
        for (String name : raw) {
            if (name == null) throw new IllegalArgumentException("supportedEncodings must not contain null");
            String token = name.trim().toLowerCase(Locale.ROOT);
            if (MediaTypes.IDENTITY.equals(token)) {
                throw new IllegalArgumentException("supportedEncodings must not contain '"
                        + MediaTypes.IDENTITY + "': it is always available and never advertised;"
                        + " use an empty list to mean \"never compress\"");
            }
            if (!COMPRESSION_CODECS.contains(token)) {
                throw new IllegalArgumentException("unsupported encoding '" + name
                        + "'; supported: " + COMPRESSION_CODECS);
            }
            if (!out.contains(token)) out.add(token);
        }
        return List.copyOf(out);
    }

    /**
     * Outcome of response-codec negotiation.
     *
     * @param encoding        the chosen codec, or {@code null} to send the body
     *                        uncompressed (nothing the client offered is producible here)
     * @param usedCustomHeader {@code true} when the choice came only from
     *                        {@link HttpHeaders#X_VGI_ACCEPT_ENCODING}, which
     *                        moves the announcement to
     *                        {@link HttpHeaders#X_VGI_CONTENT_ENCODING}
     */
    record ResponseEncoding(String encoding, boolean usedCustomHeader) {}

    /**
     * Parse an {@code Accept-Encoding}-style header into an ordered, de-duplicated
     * list of codecs this transport knows.
     *
     * <p>Split on {@code ,}; trim; lowercase; drop anything after a {@code ;} —
     * q-values are parsed off and <em>ignored</em>, not honoured; skip unknown
     * tokens (the caller intersects with what we can produce anyway); keep the
     * first occurrence of a repeat. The client's order is preserved, because it
     * is the client's preference that decides. A missing or empty header yields
     * an empty list.
     */
    static List<String> parseEncodingList(String headerValue) {
        if (headerValue == null || headerValue.isEmpty()) return List.of();
        List<String> out = new ArrayList<>(KNOWN_ENCODINGS.size());
        for (String raw : headerValue.split(",")) {
            String token = raw.trim().toLowerCase(Locale.ROOT);
            int semi = token.indexOf(';');
            if (semi >= 0) token = token.substring(0, semi).trim();
            if (token.isEmpty() || !KNOWN_ENCODINGS.contains(token) || out.contains(token)) continue;
            out.add(token);
        }
        return out;
    }

    /**
     * Pick the response codec: the first entry of
     * {@code X-VGI-Accept-Encoding ++ (Accept-Encoding minus it)} that this
     * server can actually produce.
     *
     * <p>VGI's own preference header wins over the generic {@code Accept-Encoding}.
     * HTTP clients — e.g. cpp-httplib, which the DuckDB extension uses — inject
     * their own {@code Accept-Encoding: deflate, gzip, br, zstd}, listing gzip
     * before zstd; walking that list first picks gzip and silently ignores the
     * zstd-first order VGI states in {@code X-VGI-Accept-Encoding}. gzip
     * compression dominates large Arrow responses (432ms vs ~40ms of zstd for
     * 200MB of bodies — a 4.2x slower round-trip end to end). Conversely a
     * browser/WASM client can send <em>only</em> the custom header, because
     * {@code fetch()} may not set {@code Accept-Encoding}.
     *
     * <p>Note the ordering this implies: the merged list is walked in the
     * <em>client's</em> stated order, not a hardcoded server preference.
     * {@code identity} is producible by definition, so a client that names it
     * ahead of the compressed codecs gets an uncompressed body — an explicit,
     * per-request "compression off" switch.
     *
     * @param req        the request whose encoding headers are read
     * @param producible the codecs this server can produce, from
     *                   {@link Config#supportedEncodings()}; empty means
     *                   nothing is ever compressed
     */
    static ResponseEncoding chooseResponseEncoding(HttpServletRequest req, List<String> producible) {
        List<String> custom = parseEncodingList(req.getHeader(HttpHeaders.X_VGI_ACCEPT_ENCODING));
        List<String> standard = parseEncodingList(req.getHeader(HttpHeaders.ACCEPT_ENCODING));
        List<String> merged = new ArrayList<>(custom);
        for (String enc : standard) {
            if (!custom.contains(enc)) merged.add(enc);
        }
        for (String enc : merged) {
            // An identity body is just a body: no codec, and no encoding header
            // on either name.
            if (MediaTypes.IDENTITY.equals(enc)) return new ResponseEncoding(null, false);
            if (!producible.contains(enc)) continue;
            return new ResponseEncoding(enc, custom.contains(enc) && !standard.contains(enc));
        }
        return new ResponseEncoding(null, !custom.isEmpty());
    }

    private static byte[] gzipCompress(byte[] data) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(Math.max(32, data.length / 2));
        try (java.util.zip.GZIPOutputStream gz = new java.util.zip.GZIPOutputStream(bos)) {
            gz.write(data);
        }
        return bos.toByteArray();
    }

    private static byte[] gzipDecompress(byte[] data, long maxOutput) throws IOException {
        try (java.util.zip.GZIPInputStream gz =
                     new java.util.zip.GZIPInputStream(new ByteArrayInputStream(data));
             ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            byte[] chunk = new byte[8192];
            long total = 0;
            int n;
            while ((n = gz.read(chunk)) > 0) {
                total += n;
                if (maxOutput > 0 && total > maxOutput) {
                    throw new IOException("gzip request body exceeds " + maxOutput + " bytes");
                }
                out.write(chunk, 0, n);
            }
            return out.toByteArray();
        }
    }

    private void handleStream(HttpServletRequest req, HttpServletResponse resp,
                               String method, boolean init) throws IOException {
        byte[] body;
        try {
            body = readBody(req);
        } catch (PayloadTooLargeException e) {
            writePayloadTooLarge(resp, e);
            return;
        } catch (UnsupportedContentEncodingException e) {
            writeUnsupportedEncoding(resp, e);
            return;
        }

        AuthContext auth;
        try {
            auth = authenticator.authenticate(req);
        } catch (AuthException e) {
            writeUnauthorized(resp, e);
            return;
        }

        SessionScope scope;
        try {
            scope = buildSessionScope(req, auth);
        } catch (SessionLostError e) {
            writeSessionLostResponse(req, resp, e);
            return;
        }

        byte[] out;
        Map<String, Object> md = buildTransportMetadata(req);
        try {
            try (AutoCloseable authPop = AuthScope.push(auth, md);
                 AutoCloseable sessPop = SessionScope.push(scope)) {
                out = init ? streamHandler.handleInit(method, body) : streamHandler.handleExchange(method, body);
            } catch (PayloadTooLargeException e) {
                writePayloadTooLarge(resp, e);
                return;
            } catch (Exception e) {
                // The error is reported to the client as an Arrow error stream
                // below; set VGI_STREAM_DEBUG to also log it server-side (the
                // stream-state serialization path is otherwise hard to diagnose).
                if (System.getenv("VGI_STREAM_DEBUG") != null) e.printStackTrace();
                // Serialise an error stream so the client can read it uniformly.
                ByteArrayOutputStream errOut = new ByteArrayOutputStream();
                Wire.writeErrorStream(errOut, RpcStream.EMPTY_SCHEMA, e, rpc.serverId());
                out = errOut.toByteArray();
                resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            }
        } finally {
            releaseSessionLock(scope);
        }
        emitResponseCookies(resp, md);
        emitSessionResponseHeaders(resp, scope);
        // Wire-cap enforcement: /exchange strict-fails on overshoot (mirrors
        // Python's TestHttpResponseCap.test_exchange_strict_fail), while /init
        // is soft-capped — a producer that emits one batch larger than the
        // cap is allowed through because HttpStreamHandler appends a
        // continuation token so the client can resume via /exchange
        // (TestHttpResponseCapSoftWire.test_producer_overshoot_uses_continuation).
        if (!init && advertisedMaxResponseBytes > 0 && out.length > advertisedMaxResponseBytes) {
            writeResponseCapError(req, resp, method, out.length, advertisedMaxResponseBytes);
            return;
        }
        writeArrowResponse(req, resp, out);
    }


    /** Simple in-memory transport: reads a fixed byte buffer, writes to another buffer. */
    private static final class InMemoryTransport implements RpcTransport {
        private final InputStream in;
        private final OutputStream out;
        InMemoryTransport(byte[] body, OutputStream out) {
            this.in = new ByteArrayInputStream(body);
            this.out = out;
        }
        @Override public InputStream reader() { return in; }
        @Override public OutputStream writer() { return out; }
        @Override public void close() { /* backed by ByteArray streams; nothing to release */ }
    }
}
