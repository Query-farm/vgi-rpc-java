// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.luben.zstd.Zstd;
import farm.query.vgirpc.AuthContext;
import farm.query.vgirpc.AuthScope;
import farm.query.vgirpc.RpcServer;
import farm.query.vgirpc.RpcStream;
import farm.query.vgirpc.transport.RpcTransport;
import farm.query.vgirpc.wire.Wire;
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

    public static final String ARROW_CONTENT_TYPE = "application/vnd.apache.arrow.stream";

    private static final ObjectMapper JSON = new ObjectMapper();

    private final RpcServer rpc;
    private final HttpStreamHandler streamHandler;
    private final Authenticator authenticator;
    private final List<HttpPreHandler> preHandlers;
    private final Server jetty;
    private final String prefix;
    private final long maxRequestBytes;
    private final long maxResponseBytes;
    private final int zstdLevel;
    private int port;

    /** Defaults: loopback bind, ephemeral port, no prefix, anonymous auth, 1-hour TTL, 16 MiB request/response cap. */
    public HttpServer(RpcServer rpc) {
        this(rpc, Config.defaults());
    }

    public HttpServer(RpcServer rpc, Config config) {
        this.rpc = rpc;
        this.streamHandler = new HttpStreamHandler(rpc, config.signingKey(),
                config.tokenTtlSeconds(), config.maxResponseBytes());
        this.authenticator = config.authenticator() != null ? config.authenticator() : Authenticator.ANONYMOUS;
        this.preHandlers = config.preHandlers();
        this.prefix = config.prefix();
        this.maxRequestBytes = config.maxRequestBytes();
        this.maxResponseBytes = config.maxResponseBytes();
        this.zstdLevel = config.zstdLevel();
        this.jetty = new Server();
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
     * @param signingKey       HMAC signing key for stream state tokens; {@code null}
     *                         generates a random per-process key (tokens won't
     *                         survive restarts).
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
     */
    public record Config(
            String host,
            int port,
            String prefix,
            byte[] signingKey,
            long tokenTtlSeconds,
            Authenticator authenticator,
            List<HttpPreHandler> preHandlers,
            long maxRequestBytes,
            long maxResponseBytes,
            long idleTimeoutMs,
            int zstdLevel,
            TlsConfig tls) {

        /** 1 hour. */
        public static final long DEFAULT_TOKEN_TTL_SECONDS = 3600;
        /** 16 MiB applies to both request body and serialized response. */
        public static final long DEFAULT_MAX_BYTES = 16L << 20;
        /** 30 seconds. */
        public static final long DEFAULT_IDLE_TIMEOUT_MS = 30_000;
        /** Mid-range zstd level: solid ratio, modest CPU. */
        public static final int DEFAULT_ZSTD_LEVEL = 3;

        public Config {
            host = host != null ? host : "127.0.0.1";
            prefix = prefix != null ? prefix : "";
            signingKey = signingKey != null ? signingKey.clone() : null;
            preHandlers = preHandlers != null ? List.copyOf(preHandlers) : List.of();
            if (maxRequestBytes <= 0) throw new IllegalArgumentException("maxRequestBytes must be > 0");
            if (maxResponseBytes <= 0) throw new IllegalArgumentException("maxResponseBytes must be > 0");
            if (idleTimeoutMs < 0) throw new IllegalArgumentException("idleTimeoutMs must be >= 0");
            if (zstdLevel < 1 || zstdLevel > 22) throw new IllegalArgumentException("zstdLevel must be in [1, 22]");
        }

        public static Config defaults() { return builder().build(); }
        public static Builder builder() { return new Builder(); }

        public static final class Builder {
            private String host = "127.0.0.1";
            private int port = 0;
            private String prefix = "";
            private byte[] signingKey;
            private long tokenTtlSeconds = DEFAULT_TOKEN_TTL_SECONDS;
            private Authenticator authenticator;
            private List<HttpPreHandler> preHandlers = List.of();
            private long maxRequestBytes = DEFAULT_MAX_BYTES;
            private long maxResponseBytes = DEFAULT_MAX_BYTES;
            private long idleTimeoutMs = DEFAULT_IDLE_TIMEOUT_MS;
            private int zstdLevel = DEFAULT_ZSTD_LEVEL;
            private TlsConfig tls;

            public Builder host(String host) { this.host = host; return this; }
            public Builder port(int port) { this.port = port; return this; }
            public Builder prefix(String prefix) { this.prefix = prefix; return this; }
            public Builder signingKey(byte[] signingKey) { this.signingKey = signingKey; return this; }
            public Builder tokenTtlSeconds(long tokenTtlSeconds) { this.tokenTtlSeconds = tokenTtlSeconds; return this; }
            public Builder authenticator(Authenticator authenticator) { this.authenticator = authenticator; return this; }
            public Builder preHandlers(List<HttpPreHandler> preHandlers) { this.preHandlers = preHandlers; return this; }
            public Builder maxRequestBytes(long maxRequestBytes) { this.maxRequestBytes = maxRequestBytes; return this; }
            public Builder maxResponseBytes(long maxResponseBytes) { this.maxResponseBytes = maxResponseBytes; return this; }
            public Builder idleTimeoutMs(long idleTimeoutMs) { this.idleTimeoutMs = idleTimeoutMs; return this; }
            public Builder zstdLevel(int zstdLevel) { this.zstdLevel = zstdLevel; return this; }
            public Builder tls(TlsConfig tls) { this.tls = tls; return this; }

            public Config build() {
                return new Config(host, port, prefix, signingKey, tokenTtlSeconds, authenticator,
                        preHandlers, maxRequestBytes, maxResponseBytes, idleTimeoutMs, zstdLevel, tls);
            }
        }
    }

    public void start() throws Exception {
        jetty.start();
        this.port = ((ServerConnector) jetty.getConnectors()[0]).getLocalPort();
    }

    public int port() { return port; }

    public void stop() throws Exception { jetty.stop(); }

    public void join() throws InterruptedException { jetty.join(); }

    // --- Servlet ---------------------------------------------------------

    /** Single servlet that dispatches health / unary / stream sub-paths. */
    private final class RouterServlet extends HttpServlet {
        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
            if (runPreHandlers(req, resp)) return;
            String p = pathInfo(req);
            if ("health".equals(p) || "".equals(p) || "/".equals(p)) {
                writeJson(resp, HttpServletResponse.SC_OK, Map.of(
                        "status", "ok",
                        "server_id", rpc.serverId(),
                        "protocol", rpc.protocolName()));
                return;
            }
            resp.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED);
        }

        @Override
        protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
            if (runPreHandlers(req, resp)) return;
            String rest = pathInfo(req);
            if (rest.isEmpty() || "health".equals(rest)) {
                resp.sendError(HttpServletResponse.SC_NOT_FOUND);
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

    private void handleUnary(HttpServletRequest req, HttpServletResponse resp, String method) throws IOException {
        byte[] body;
        try {
            body = readBody(req);
        } catch (PayloadTooLargeException e) {
            writePayloadTooLarge(resp, e);
            return;
        }

        AuthContext auth;
        try {
            auth = authenticator.authenticate(req);
        } catch (AuthException e) {
            writeAuthFailure(resp, e);
            return;
        }

        BoundedByteArrayOutputStream out = new BoundedByteArrayOutputStream(maxResponseBytes);
        try (AutoCloseable ignored = AuthScope.push(auth, buildTransportMetadata(req));
             InMemoryTransport t = new InMemoryTransport(body, out)) {
            rpc.serveOne(t);
        } catch (PayloadTooLargeException e) {
            writePayloadTooLarge(resp, e);
            return;
        } catch (Exception e) {
            throw new IOException(e);
        }
        writeArrowResponse(req, resp, out.toByteArray());
    }

    private static Map<String, Object> buildTransportMetadata(HttpServletRequest req) {
        Map<String, Object> md = new LinkedHashMap<>();
        String remote = req.getRemoteAddr();
        if (remote != null) md.put("remote_addr", remote);
        String ua = req.getHeader(HttpHeaders.USER_AGENT);
        if (ua != null) md.put("user_agent", ua);
        return md;
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
        if (acceptsZstd(req)) {
            resp.setHeader(HttpHeaders.CONTENT_ENCODING, MediaTypes.ZSTD);
            body = Zstd.compress(body, zstdLevel);
        }
        resp.getOutputStream().write(body);
    }

    private static void writePayloadTooLarge(HttpServletResponse resp, RuntimeException e) throws IOException {
        writeJson(resp, HttpServletResponse.SC_REQUEST_ENTITY_TOO_LARGE,
                Map.of("error", e.getMessage()));
    }

    private static byte[] maybeDecodeRequestBody(HttpServletRequest req, byte[] body) throws IOException {
        String enc = req.getHeader(HttpHeaders.CONTENT_ENCODING);
        if (enc == null) return body;
        if (enc.equalsIgnoreCase(MediaTypes.ZSTD)) {
            long size = Zstd.getFrameContentSize(body);
            if (size <= 0) throw new IOException("zstd frame has unknown size");
            byte[] out = new byte[(int) size];
            long ret = Zstd.decompress(out, body);
            if (Zstd.isError(ret)) {
                throw new IOException("zstd decompress failed: " + Zstd.getErrorName(ret));
            }
            return out;
        }
        throw new IOException("unsupported Content-Encoding: " + enc);
    }

    private void handleStream(HttpServletRequest req, HttpServletResponse resp,
                               String method, boolean init) throws IOException {
        byte[] body;
        try {
            body = readBody(req);
        } catch (PayloadTooLargeException e) {
            writePayloadTooLarge(resp, e);
            return;
        }

        AuthContext auth;
        try {
            auth = authenticator.authenticate(req);
        } catch (AuthException e) {
            writeAuthFailure(resp, e);
            return;
        }

        byte[] out;
        try (AutoCloseable ignored = AuthScope.push(auth, buildTransportMetadata(req))) {
            out = init ? streamHandler.handleInit(method, body) : streamHandler.handleExchange(method, body);
        } catch (PayloadTooLargeException e) {
            writePayloadTooLarge(resp, e);
            return;
        } catch (Exception e) {
            // Serialise an error stream so the client can read it uniformly.
            ByteArrayOutputStream errOut = new ByteArrayOutputStream();
            Wire.writeErrorStream(errOut, RpcStream.EMPTY_SCHEMA, e, rpc.serverId());
            out = errOut.toByteArray();
            resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
        writeArrowResponse(req, resp, out);
    }

    private static boolean acceptsZstd(HttpServletRequest req) {
        String accept = req.getHeader(HttpHeaders.ACCEPT_ENCODING);
        return accept != null && accept.toLowerCase(Locale.ROOT).contains(MediaTypes.ZSTD);
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
