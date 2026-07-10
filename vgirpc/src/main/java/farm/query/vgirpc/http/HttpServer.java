// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.luben.zstd.Zstd;
import farm.query.vgirpc.AuthContext;
import farm.query.vgirpc.AuthScope;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.RpcServer;
import farm.query.vgirpc.RpcStream;
import farm.query.vgirpc.SessionLostError;
import farm.query.vgirpc.external.UploadUrlProvider;
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
    /** Capability response header: comma-separated content-encodings the server accepts/produces (e.g. {@code "zstd, gzip"}). */
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
    private final boolean stickyEnabled;
    private final long stickyDefaultTtlSeconds;
    private final Map<String, String> stickyEchoHeaders;
    private final boolean exposeTestDrainAdmin;
    private final byte[] sessionTokenKey;
    private final SessionRegistry sessionRegistry;
    /** When {@code true} (VGI_HTTP_DISABLE_ZSTD set), zstd is dropped from the
     *  advertised/accepted codec set and the server uses gzip only. Mirrors the
     *  vgi-python http fixture's {@code VGI_HTTP_DISABLE_ZSTD} knob. */
    private final boolean disableZstd;
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
                config.tokenTtlSeconds(), config.maxResponseBytes());
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
        this.stickyEnabled = config.stickyEnabled();
        this.stickyDefaultTtlSeconds = config.stickyDefaultTtlSeconds();
        this.stickyEchoHeaders = config.stickyEchoHeaders();
        this.exposeTestDrainAdmin = config.exposeTestDrainAdmin();
        String disZstd = System.getenv("VGI_HTTP_DISABLE_ZSTD");
        this.disableZstd = disZstd != null && !disZstd.isEmpty() && !disZstd.equals("0");
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
     *                         Content-Encoding (1=fastest, 22=max). Default 3.
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
            TlsConfig tls,
            boolean advertiseMaxRequestBytes,
            UploadUrlProvider uploadUrlProvider,
            Long maxUploadBytes,
            long advertisedMaxResponseBytes,
            long advertisedMaxExternalizedResponseBytes,
            boolean stickyEnabled,
            long stickyDefaultTtlSeconds,
            Map<String, String> stickyEchoHeaders,
            boolean exposeTestDrainAdmin,
            DescribeProvider describeProvider) {

        /** 1 hour. */
        public static final long DEFAULT_TOKEN_TTL_SECONDS = 3600;
        /** 16 MiB applies to both request body and serialized response. */
        public static final long DEFAULT_MAX_BYTES = 16L << 20;
        /** 30 seconds. */
        public static final long DEFAULT_IDLE_TIMEOUT_MS = 30_000;
        /** Mid-range zstd level: solid ratio, modest CPU. */
        public static final int DEFAULT_ZSTD_LEVEL = 3;

        /**
         * Normalizes nullable fields (host, prefix, key copy, immutable
         * collection copies) and validates numeric bounds.
         */
        public Config {
            host = host != null ? host : "127.0.0.1";
            prefix = prefix != null ? prefix : "";
            tokenKey = tokenKey != null ? tokenKey.clone() : null;
            preHandlers = preHandlers != null ? List.copyOf(preHandlers) : List.of();
            stickyEchoHeaders = stickyEchoHeaders != null ? Map.copyOf(stickyEchoHeaders) : Map.of();
            if (maxRequestBytes <= 0) throw new IllegalArgumentException("maxRequestBytes must be > 0");
            if (maxResponseBytes <= 0) throw new IllegalArgumentException("maxResponseBytes must be > 0");
            if (idleTimeoutMs < 0) throw new IllegalArgumentException("idleTimeoutMs must be >= 0");
            if (zstdLevel < 1 || zstdLevel > 22) throw new IllegalArgumentException("zstdLevel must be in [1, 22]");
            if (stickyEnabled && stickyDefaultTtlSeconds <= 0) {
                throw new IllegalArgumentException("stickyDefaultTtlSeconds must be > 0 when sticky is enabled");
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
            private TlsConfig tls;
            private boolean advertiseMaxRequestBytes;
            private UploadUrlProvider uploadUrlProvider;
            private Long maxUploadBytes;
            private long advertisedMaxResponseBytes;
            private long advertisedMaxExternalizedResponseBytes;
            private boolean stickyEnabled;
            private long stickyDefaultTtlSeconds = 300;
            private Map<String, String> stickyEchoHeaders = Map.of();
            private boolean exposeTestDrainAdmin;
            private DescribeProvider describeProvider;

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
             * {@code zstd} Content-Encoding level, 1 (fastest) to 22 (max); default 3.
             *
             * @param zstdLevel the compression level used for {@code zstd}-encoded responses
             * @return this builder
             */
            public Builder zstdLevel(int zstdLevel) { this.zstdLevel = zstdLevel; return this; }
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
             * Build the immutable config.
             *
             * @return the validated {@link Config}
             * @throws IllegalArgumentException if a numeric bound is out of range (see {@link Config})
             */
            public Config build() {
                return new Config(host, port, prefix, tokenKey, tokenTtlSeconds, authenticator,
                        preHandlers, maxRequestBytes, maxResponseBytes, idleTimeoutMs, zstdLevel, tls,
                        advertiseMaxRequestBytes, uploadUrlProvider, maxUploadBytes,
                        advertisedMaxResponseBytes, advertisedMaxExternalizedResponseBytes,
                        stickyEnabled, stickyDefaultTtlSeconds, stickyEchoHeaders, exposeTestDrainAdmin,
                        describeProvider);
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
                    preHandlers, maxRequestBytes, maxResponseBytes, idleTimeoutMs, zstdLevel, tls,
                    advertiseMaxRequestBytes, uploadUrlProvider, maxUploadBytes,
                    advertisedMaxResponseBytes, advertisedMaxExternalizedResponseBytes,
                    stickyEnabled, stickyDefaultTtlSeconds, stickyEchoHeaders, exposeTestDrainAdmin,
                    p);
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
    private final class RouterServlet extends HttpServlet {

        @Override
        protected void service(HttpServletRequest req, HttpServletResponse resp) throws IOException {
            // Set capability headers on every response (parity with the Python
            // _CapabilitiesMiddleware: announce externalisation contract upfront).
            applyCapabilityHeaders(req, resp);
            try {
                super.service(req, resp);
            } catch (jakarta.servlet.ServletException se) {
                throw new IOException(se);
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
            if (UPLOAD_URL_METHOD.equals(rest) || (UPLOAD_URL_METHOD + "/init").equals(rest)) {
                handleUploadUrl(req, resp);
                return;
            }
            if (rest.endsWith("/init") || rest.endsWith("/exchange")) {
                boolean init = rest.endsWith("/init");
                String methodName = rest.substring(0, rest.length() - (init ? "/init".length() : "/exchange".length()));
                handleStream(req, resp, methodName, init);
                return;
            }
            handleUnary(req, resp, rest);
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
            return maybeDecodeRequestBody(req, buf.toByteArray());
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
            writeAuthFailure(resp, e);
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

    private static void writeAuthFailure(HttpServletResponse resp, AuthException e) throws IOException {
        if (e.wwwAuthenticate() != null) {
            resp.setHeader(HttpHeaders.WWW_AUTHENTICATE, e.wwwAuthenticate());
        }
        String msg = e.getMessage() != null ? e.getMessage() : "Unauthorized";
        writeJson(resp, HttpServletResponse.SC_UNAUTHORIZED, Map.of("error", msg));
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
        String enc = chooseResponseEncoding(req);
        if (MediaTypes.ZSTD.equals(enc)) {
            resp.setHeader(HttpHeaders.CONTENT_ENCODING, MediaTypes.ZSTD);
            body = Zstd.compress(body, zstdLevel);
        } else if (MediaTypes.GZIP.equals(enc)) {
            resp.setHeader(HttpHeaders.CONTENT_ENCODING, MediaTypes.GZIP);
            body = gzipCompress(body);
        }
        resp.getOutputStream().write(body);
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

    private byte[] maybeDecodeRequestBody(HttpServletRequest req, byte[] body) throws IOException {
        String enc = req.getHeader(HttpHeaders.CONTENT_ENCODING);
        if (enc == null || enc.isEmpty()) return body;
        if (enc.equalsIgnoreCase(MediaTypes.ZSTD)) {
            if (disableZstd) {
                throw new UnsupportedContentEncodingException(
                        "Content-Encoding 'zstd' is not enabled on this server", enabledEncodings());
            }
            long size = Zstd.getFrameContentSize(body);
            if (size <= 0) throw new IOException("zstd frame has unknown size");
            byte[] out = new byte[(int) size];
            long ret = Zstd.decompress(out, body);
            if (Zstd.isError(ret)) {
                throw new IOException("zstd decompress failed: " + Zstd.getErrorName(ret));
            }
            return out;
        }
        if (enc.equalsIgnoreCase(MediaTypes.GZIP)) {
            return gzipDecompress(body, maxRequestBytes);
        }
        throw new UnsupportedContentEncodingException(
                "unsupported Content-Encoding: " + enc, enabledEncodings());
    }

    /** Comma-separated content-encodings this server accepts and produces. */
    private String enabledEncodings() {
        return disableZstd ? MediaTypes.GZIP : MediaTypes.ZSTD + ", " + MediaTypes.GZIP;
    }

    /** Pick the response {@code Content-Encoding} from the client's
     *  {@code Accept-Encoding}, honouring the enabled codec set (zstd preferred
     *  unless disabled). {@code null} = send the body uncompressed. */
    private String chooseResponseEncoding(HttpServletRequest req) {
        String accept = req.getHeader(HttpHeaders.ACCEPT_ENCODING);
        if (accept == null) return null;
        String lower = accept.toLowerCase(Locale.ROOT);
        if (!disableZstd && lower.contains(MediaTypes.ZSTD)) return MediaTypes.ZSTD;
        if (lower.contains(MediaTypes.GZIP)) return MediaTypes.GZIP;
        return null;
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
            writeAuthFailure(resp, e);
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
