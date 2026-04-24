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
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;

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
    private int port;

    public HttpServer(RpcServer rpc) {
        this(rpc, "", 0, null, 0, null, null);
    }

    public HttpServer(RpcServer rpc, String prefix, int port) {
        this(rpc, prefix, port, null, 0, null, null);
    }

    public HttpServer(RpcServer rpc, String prefix, int port,
                      byte[] signingKey, long tokenTtlSeconds) {
        this(rpc, prefix, port, signingKey, tokenTtlSeconds, null, null);
    }

    public HttpServer(RpcServer rpc, String prefix, int port,
                      byte[] signingKey, long tokenTtlSeconds,
                      Authenticator authenticator) {
        this(rpc, prefix, port, signingKey, tokenTtlSeconds, authenticator, null);
    }

    /**
     * @param signingKey optional HMAC signing key for stream state tokens; when
     *     {@code null} a random per-process key is generated. Stable keys let
     *     clients resume streams across server restarts.
     * @param tokenTtlSeconds maximum state-token age before it's rejected;
     *     {@code 0} disables TTL enforcement.
     * @param authenticator per-request authenticator; {@code null} means
     *     anonymous (default).
     * @param preHandlers optional list of pre-route handlers, run in order
     *     before the default /health + /{method} + /{method}/init + /{method}/exchange
     *     dispatch. Used to mount add-on routes (e.g. OAuth PKCE callback).
     */
    public HttpServer(RpcServer rpc, String prefix, int port,
                      byte[] signingKey, long tokenTtlSeconds,
                      Authenticator authenticator,
                      List<HttpPreHandler> preHandlers) {
        this.rpc = rpc;
        this.streamHandler = new HttpStreamHandler(rpc, signingKey, tokenTtlSeconds);
        this.authenticator = authenticator != null ? authenticator : Authenticator.ANONYMOUS;
        this.preHandlers = preHandlers != null ? List.copyOf(preHandlers) : List.of();
        this.prefix = prefix;
        this.jetty = new Server();
        ServerConnector connector = new ServerConnector(jetty);
        connector.setHost("127.0.0.1");
        connector.setPort(port);
        jetty.addConnector(connector);

        ServletContextHandler ctx = new ServletContextHandler();
        ctx.setContextPath("/");
        String pattern = prefix.isEmpty() ? "/*" : prefix + "/*";
        ctx.addServlet(new ServletHolder(new RouterServlet()), pattern);
        jetty.setHandler(ctx);
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
        byte[] body = readBody(req);

        AuthContext auth;
        try {
            auth = authenticator.authenticate(req);
        } catch (AuthException e) {
            writeAuthFailure(resp, e);
            return;
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (AutoCloseable ignored = AuthScope.push(auth, buildTransportMetadata(req));
             InMemoryTransport t = new InMemoryTransport(body, out)) {
            rpc.serveOne(t);
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

    private static byte[] readBody(HttpServletRequest req) throws IOException {
        byte[] body;
        try (InputStream in = req.getInputStream(); ByteArrayOutputStream buf = new ByteArrayOutputStream()) {
            in.transferTo(buf);
            body = buf.toByteArray();
        }
        return maybeDecodeRequestBody(req, body);
    }

    private void writeArrowResponse(HttpServletRequest req, HttpServletResponse resp, byte[] body) throws IOException {
        resp.setContentType(ARROW_CONTENT_TYPE);
        if (acceptsZstd(req)) {
            resp.setHeader(HttpHeaders.CONTENT_ENCODING, MediaTypes.ZSTD);
            body = Zstd.compress(body, 3);
        }
        resp.getOutputStream().write(body);
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
        byte[] body = readBody(req);

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
