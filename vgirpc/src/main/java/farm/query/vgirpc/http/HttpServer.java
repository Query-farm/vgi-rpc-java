// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import farm.query.vgirpc.RpcServer;
import farm.query.vgirpc.Stream;
import farm.query.vgirpc.transport.RpcTransport;
import farm.query.vgirpc.wire.Wire;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.eclipse.jetty.ee10.servlet.ServletHandler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import jakarta.servlet.http.HttpServlet;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

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
 *   endpoints (not yet implemented; currently return 501).</li>
 * </ul>
 */
public final class HttpServer {

    public static final String ARROW_CONTENT_TYPE = "application/vnd.apache.arrow.stream";

    private final RpcServer rpc;
    private final HttpStreamHandler streamHandler;
    private final Authenticator authenticator;
    private final java.util.List<HttpPreHandler> preHandlers;
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
                      java.util.List<HttpPreHandler> preHandlers) {
        this.rpc = rpc;
        this.streamHandler = new HttpStreamHandler(rpc, signingKey, tokenTtlSeconds);
        this.authenticator = authenticator != null ? authenticator : Authenticator.ANONYMOUS;
        this.preHandlers = preHandlers != null ? java.util.List.copyOf(preHandlers) : java.util.List.of();
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
                resp.setContentType("application/json");
                byte[] body = ("{\"status\":\"ok\",\"server_id\":\"" + rpc.serverId()
                        + "\",\"protocol\":\"" + rpc.protocolName() + "\"}").getBytes();
                resp.getOutputStream().write(body);
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
                String methodName;
                boolean init;
                if (rest.endsWith("/init")) {
                    methodName = rest.substring(0, rest.length() - "/init".length());
                    init = true;
                } else {
                    methodName = rest.substring(0, rest.length() - "/exchange".length());
                    init = false;
                }
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
        try (InputStream in = req.getInputStream(); ByteArrayOutputStream buf = new ByteArrayOutputStream()) {
            in.transferTo(buf);
            body = buf.toByteArray();
        }
        body = maybeDecodeRequestBody(req, body);

        farm.query.vgirpc.AuthContext auth;
        try {
            auth = authenticator.authenticate(req);
        } catch (AuthException e) {
            writeAuthFailure(resp, e);
            return;
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (AutoCloseable ignored = farm.query.vgirpc.AuthScope.push(auth, buildTransportMetadata(req));
             InMemoryTransport t = new InMemoryTransport(body, out)) {
            rpc.serveOne(t);
        } catch (Exception e) {
            throw new IOException(e);
        }
        resp.setContentType(ARROW_CONTENT_TYPE);
        byte[] responseBytes = out.toByteArray();
        if (acceptsZstd(req)) {
            resp.setHeader("Content-Encoding", "zstd");
            responseBytes = com.github.luben.zstd.Zstd.compress(responseBytes, 3);
        }
        resp.getOutputStream().write(responseBytes);
    }

    private static java.util.Map<String, Object> buildTransportMetadata(HttpServletRequest req) {
        java.util.LinkedHashMap<String, Object> md = new java.util.LinkedHashMap<>();
        String remote = req.getRemoteAddr();
        if (remote != null) md.put("remote_addr", remote);
        String ua = req.getHeader("User-Agent");
        if (ua != null) md.put("user_agent", ua);
        return md;
    }

    private static void writeAuthFailure(HttpServletResponse resp, AuthException e) throws IOException {
        resp.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        resp.setContentType("application/json");
        if (e.wwwAuthenticate() != null) {
            resp.setHeader("WWW-Authenticate", e.wwwAuthenticate());
        }
        String msg = e.getMessage() != null ? e.getMessage() : "Unauthorized";
        resp.getOutputStream().write(("{\"error\":\"" + escape(msg) + "\"}").getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    private static String escape(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static byte[] maybeDecodeRequestBody(HttpServletRequest req, byte[] body) throws IOException {
        String enc = req.getHeader("Content-Encoding");
        if (enc == null) return body;
        if (enc.equalsIgnoreCase("zstd")) {
            long size = com.github.luben.zstd.Zstd.getFrameContentSize(body);
            if (size <= 0) throw new IOException("zstd frame has unknown size");
            byte[] out = new byte[(int) size];
            long ret = com.github.luben.zstd.Zstd.decompress(out, body);
            if (com.github.luben.zstd.Zstd.isError(ret)) {
                throw new IOException("zstd decompress failed: " + com.github.luben.zstd.Zstd.getErrorName(ret));
            }
            return out;
        }
        throw new IOException("unsupported Content-Encoding: " + enc);
    }

    private void handleStream(HttpServletRequest req, HttpServletResponse resp,
                               String method, boolean init) throws IOException {
        byte[] body;
        try (InputStream in = req.getInputStream(); ByteArrayOutputStream buf = new ByteArrayOutputStream()) {
            in.transferTo(buf);
            body = buf.toByteArray();
        }
        body = maybeDecodeRequestBody(req, body);

        farm.query.vgirpc.AuthContext auth;
        try {
            auth = authenticator.authenticate(req);
        } catch (AuthException e) {
            writeAuthFailure(resp, e);
            return;
        }

        byte[] out;
        try (AutoCloseable ignored = farm.query.vgirpc.AuthScope.push(auth, buildTransportMetadata(req))) {
            out = init ? streamHandler.handleInit(method, body) : streamHandler.handleExchange(method, body);
        } catch (Exception e) {
            // Serialise an error stream so the client can read it uniformly.
            ByteArrayOutputStream errOut = new ByteArrayOutputStream();
            Wire.writeErrorStream(errOut, Stream.EMPTY_SCHEMA, e, rpc.serverId());
            out = errOut.toByteArray();
            resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
        resp.setContentType(ARROW_CONTENT_TYPE);
        if (acceptsZstd(req)) {
            resp.setHeader("Content-Encoding", "zstd");
            out = com.github.luben.zstd.Zstd.compress(out, 3);
        }
        resp.getOutputStream().write(out);
    }

    private static boolean acceptsZstd(HttpServletRequest req) {
        String accept = req.getHeader("Accept-Encoding");
        return accept != null && accept.toLowerCase().contains("zstd");
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
        @Override public void close() {}
    }
}
