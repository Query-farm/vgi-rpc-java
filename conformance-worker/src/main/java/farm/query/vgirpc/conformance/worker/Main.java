// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.conformance.worker;

import farm.query.vgirpc.AccessLogHook;
import farm.query.vgirpc.AuthContext;
import farm.query.vgirpc.RpcServer;
import farm.query.vgirpc.conformance.ConformanceService;
import farm.query.vgirpc.conformance.ConformanceServiceImpl;
import farm.query.vgirpc.external.ExternalLocationConfig;
import farm.query.vgirpc.external.LocationResolver;
import farm.query.vgirpc.http.Authenticator;
import farm.query.vgirpc.http.HttpPreHandler;
import farm.query.vgirpc.http.HttpServer;
import farm.query.vgirpc.http.auth.BearerAuthenticator;
import farm.query.vgirpc.http.auth.JwtAuthenticator;
import farm.query.vgirpc.http.auth.MTlsAuthenticator;
import farm.query.vgirpc.http.auth.OAuthPkce;
import farm.query.vgirpc.http.auth.OidcMetadata;
import farm.query.vgirpc.transport.StdioTransport;
import farm.query.vgirpc.transport.TcpSocketTransport;
import farm.query.vgirpc.transport.UnixSocketTransport;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class Main {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(Main.class);

    private Main() {}

    public static void main(String[] args) throws Exception {
        ConformanceService impl = new ConformanceServiceImpl();
        RpcServer server = new RpcServer(ConformanceService.class, impl);
        // Match the Python reference's ConformanceService.protocol_version so the
        // describe conformance suite sees the same MAJOR.MINOR.PATCH label.
        server.setProtocolVersion("1.0.0");

        byte[] tokenKey = null;
        long tokenTtl = 0;
        String mode = null;
        String unixPath = null;
        // Raw-TCP target: host defaults to loopback; port 0 ⇒ OS auto-selects.
        String tcpHost = "127.0.0.1";
        int tcpPort = 0;
        Authenticator authenticator = null;
        List<HttpPreHandler> preHandlers = new ArrayList<>();
        String fakeStorageUrl = null;
        long externalizeThreshold = 4096;
        // -1 sentinel: "unset"; falls back to externalizeThreshold for backward compat.
        long maxRequestBytes = -1;
        String compression = "none";
        String accessLogPath = null;
        boolean strictMode = false;
        // 0 = unbounded; --strict bumps both to 1 MiB to mirror Python's
        // tests/serve_conformance_http_strict.py.
        long maxResponseBytes = 0;
        long maxExternalizedResponseBytes = 0;
        // Sticky sessions are ON by default to mirror the Python conformance
        // worker; --no-sticky disables them.
        boolean stickyEnabled = true;
        long stickyTtl = 300;
        // Response compression is on by default (zstd, gzip). --no-compression
        // boots the server with an EMPTY producible set, which is a server
        // configuration no client request can induce: it emits a
        // present-but-empty VGI-Supported-Encodings and never compresses.
        // Unrelated to --compression, which selects the codec for
        // external-location payload uploads.
        boolean responseCompression = true;
        ArgCursor c = new ArgCursor(args);
        while (c.hasNext()) {
            String a = c.next();
            switch (a) {
                case "--http" -> mode = "http";
                case "--unix" -> { mode = "unix"; unixPath = c.requireValue(a); }
                case "--tcp" -> {
                    mode = "tcp";
                    // Accept [HOST:]PORT; a bare PORT keeps the loopback default host.
                    String rawSpec = c.requireValue(a);
                    String portSpec = rawSpec;
                    int colon = rawSpec.lastIndexOf(':');
                    if (colon >= 0) {
                        tcpHost = rawSpec.substring(0, colon);
                        portSpec = rawSpec.substring(colon + 1);
                    }
                    try {
                        tcpPort = Integer.parseInt(portSpec);
                    } catch (NumberFormatException nfe) {
                        System.err.println("--tcp expects [HOST:]PORT, got: " + rawSpec);
                        System.exit(2);
                    }
                }
                case "--token-key" -> tokenKey = HexFormat.of().parseHex(c.requireValue(a));
                case "--token-ttl" -> tokenTtl = Long.parseLong(c.requireValue(a));
                case "--auth-bearer" -> authenticator = buildBearer(c.requireValue(a));
                case "--auth-mtls" -> {
                    String kind = c.requireValue(a);
                    if (!"xfcc".equals(kind)) { System.err.println("unsupported --auth-mtls kind: " + kind); System.exit(2); }
                    authenticator = MTlsAuthenticator.xfcc("mtls");
                }
                case "--auth-jwt" -> authenticator = buildJwt(c.requireValue(a));
                case "--auth-pkce" -> {
                    OAuthPkce pkce = buildPkce(c.requireValue(a));
                    authenticator = pkce.authenticator();
                    preHandlers.add(pkce.preHandler());
                }
                case "--fake-storage" -> fakeStorageUrl = c.requireValue(a);
                case "--externalize-threshold" -> externalizeThreshold = Long.parseLong(c.requireValue(a));
                case "--max-request-bytes" -> maxRequestBytes = Long.parseLong(c.requireValue(a));
                case "--compression" -> compression = c.requireValue(a);
                case "--access-log" -> accessLogPath = c.requireValue(a);
                case "--strict" -> strictMode = true;
                case "--max-response-bytes" -> maxResponseBytes = Long.parseLong(c.requireValue(a));
                case "--max-externalized-response-bytes" ->
                        maxExternalizedResponseBytes = Long.parseLong(c.requireValue(a));
                case "--no-compression" -> responseCompression = false;
                case "--no-sticky" -> stickyEnabled = false;
                case "--sticky-ttl" -> stickyTtl = Long.parseLong(c.requireValue(a));
                default -> { System.err.println("unknown arg: " + a); System.exit(2); }
            }
        }
        FakeStorage fakeStorage = null;
        if (fakeStorageUrl != null) {
            fakeStorage = new FakeStorage(fakeStorageUrl);
            ExternalLocationConfig.Builder cfgB = ExternalLocationConfig.builder()
                    .storage(fakeStorage)
                    .thresholdBytes(externalizeThreshold)
                    .urlValidator(ExternalLocationConfig.permissiveValidator());
            if ("zstd".equalsIgnoreCase(compression)) {
                cfgB.compression(ExternalLocationConfig.Compression.zstd());
            } else if (!"none".equalsIgnoreCase(compression)) {
                System.err.println("unknown --compression value: " + compression);
                System.exit(2);
            }
            ExternalLocationConfig cfg = cfgB.build();
            server.setExternalConfig(cfg);
            // Also wire the resolver so the server can transparently pull pointer
            // batches that clients upload via the __upload_url__ flow.
            server.setLocationResolver(new LocationResolver(cfg));
        }
        if (accessLogPath != null) {
            OutputStream accessLogOut = new FileOutputStream(accessLogPath, true);
            server.setDispatchHook(new AccessLogHook(accessLogOut, "vgi-rpc-java-conformance"));
        }
        if (mode == null) { servePipe(server); return; }
        if (strictMode) {
            if (maxResponseBytes <= 0) maxResponseBytes = 1024L * 1024L;
            if (maxExternalizedResponseBytes <= 0) maxExternalizedResponseBytes = 1024L * 1024L;
        }
        switch (mode) {
            case "http" -> serveHttp(server, tokenKey, tokenTtl, authenticator, preHandlers, fakeStorage,
                    maxRequestBytes >= 0 ? maxRequestBytes : externalizeThreshold,
                    maxResponseBytes, maxExternalizedResponseBytes,
                    stickyEnabled, stickyTtl, responseCompression);
            case "unix" -> serveUnix(server, Path.of(unixPath));
            case "tcp" -> serveTcp(server, tcpHost, tcpPort);
            default -> { System.err.println("unknown mode: " + mode); System.exit(2); }
        }
    }

    /** Mutable cursor over {@code args} that knows how to demand a value for a flag. */
    private static final class ArgCursor {
        private final String[] args;
        private int i;
        ArgCursor(String[] args) { this.args = args; }
        boolean hasNext() { return i < args.length; }
        String next() { return args[i++]; }
        String requireValue(String flag) {
            if (i >= args.length) { System.err.println(flag + " requires a value"); System.exit(2); }
            return args[i++];
        }
    }

    private static OAuthPkce buildPkce(String spec) {
        Map<String, String> cfg = splitKv(spec, "--auth-pkce");
        String clientId    = Objects.requireNonNull(cfg.get("client_id"),    "client_id required");
        String redirectUri = Objects.requireNonNull(cfg.get("redirect_uri"), "redirect_uri required");
        String issuer      = Objects.requireNonNull(cfg.get("issuer"),      "issuer required");
        String audience    = Objects.requireNonNull(cfg.get("audience"),    "audience required");

        OidcMetadata oidc;
        try { oidc = OidcMetadata.discover(issuer); }
        catch (Exception e) { throw new IllegalStateException("OIDC discovery failed: " + e.getMessage(), e); }

        JwtAuthenticator jwt = JwtAuthenticator.builder()
                .issuer(issuer).audience(audience)
                .jwksUri(oidc.jwksUri().toString())
                .build();

        byte[] sessionKey = cfg.containsKey("session_key_hex")
                ? HexFormat.of().parseHex(cfg.get("session_key_hex")) : randomKey(32);
        byte[] authKey = cfg.containsKey("auth_key_hex")
                ? HexFormat.of().parseHex(cfg.get("auth_key_hex")) : randomKey(32);

        return OAuthPkce.builder()
                .clientId(clientId)
                .redirectUri(redirectUri)
                .oidcMetadata(oidc)
                .idTokenValidator(jwt)
                .sessionKey(sessionKey)
                .authKey(authKey)
                .build();
    }

    private static Authenticator buildJwt(String spec) {
        JwtAuthenticator.Builder b = JwtAuthenticator.builder();
        for (Map.Entry<String, String> e : splitKv(spec, "--auth-jwt").entrySet()) {
            switch (e.getKey()) {
                case "issuer", "iss" -> b.issuer(e.getValue());
                case "audience", "aud" -> b.audience(e.getValue());
                case "jwks", "jwks_uri" -> b.jwksUri(e.getValue());
                case "principal_claim" -> b.principalClaim(e.getValue());
                default -> { System.err.println("unknown --auth-jwt key: " + e.getKey()); System.exit(2); }
            }
        }
        return b.build();
    }

    private static Authenticator buildBearer(String spec) {
        Map<String, AuthContext> tokens = new LinkedHashMap<>();
        for (Map.Entry<String, String> e : splitKv(spec, "--auth-bearer").entrySet()) {
            if (e.getValue().isEmpty()) {
                System.err.println("malformed --auth-bearer entry: " + e.getKey() + "=");
                System.exit(2);
            }
            tokens.put(e.getKey(), new AuthContext("bearer", true, e.getValue(), Collections.emptyMap()));
        }
        return BearerAuthenticator.fromMap(tokens);
    }

    /** Split a comma-separated {@code key=value} string; bail out with a usage message on malformed entries. */
    private static Map<String, String> splitKv(String spec, String flag) {
        Map<String, String> out = new LinkedHashMap<>();
        for (String pair : spec.split(",")) {
            int eq = pair.indexOf('=');
            if (eq <= 0) { System.err.println("malformed " + flag + " entry: " + pair); System.exit(2); }
            out.put(pair.substring(0, eq).trim(), pair.substring(eq + 1).trim());
        }
        return out;
    }

    private static byte[] randomKey(int length) {
        byte[] out = new byte[length];
        new SecureRandom().nextBytes(out);
        return out;
    }

    private static void servePipe(RpcServer server) {
        try (StdioTransport t = new StdioTransport()) { server.serve(t); }
    }

    private static void serveHttp(RpcServer server, byte[] tokenKey, long tokenTtl,
                                   Authenticator authenticator,
                                   List<HttpPreHandler> preHandlers,
                                   FakeStorage fakeStorage,
                                   long maxRequestBytes,
                                   long maxResponseBytes,
                                   long maxExternalizedResponseBytes,
                                   boolean stickyEnabled,
                                   long stickyTtl,
                                   boolean responseCompression) throws Exception {
        HttpServer.Config.Builder cb = HttpServer.Config.builder()
                .tokenKey(tokenKey)
                .tokenTtlSeconds(tokenTtl)
                .authenticator(authenticator)
                .preHandlers(preHandlers);
        // Empty producible set ⇒ present-but-empty VGI-Supported-Encodings and
        // no compression, whatever the client asks for. null would mean "unset"
        // and fall back to the default set, so the empty list is load-bearing.
        if (!responseCompression) cb.supportedEncodings(List.of());
        if (maxResponseBytes > 0) cb.advertisedMaxResponseBytes(maxResponseBytes);
        if (maxExternalizedResponseBytes > 0)
            cb.advertisedMaxExternalizedResponseBytes(maxExternalizedResponseBytes);
        if (stickyEnabled) {
            cb.stickyEnabled(true)
              .stickyDefaultTtlSeconds(stickyTtl)
              // Fixed echo header keeps the conformance ``TestSticky``
              // echo-roundtrip contract testable across ports.
              .stickyEchoHeaders(Map.of("x-vgi-conformance-echo", "conformance-fixed-marker"))
              .exposeTestDrainAdmin(true);
        }
        if (fakeStorage != null) {
            // Match the Python conformance worker: a tight inline-request cap
            // forces the client to discover capabilities and route oversized
            // payloads through __upload_url__ + a pointer batch.  In the
            // "externalize-always" variant the threshold is decoupled from
            // the request cap (threshold=1, request cap=1 MiB) so every
            // *response* batch externalizes while normal-sized inline
            // *requests* still flow through.
            cb.maxRequestBytes(maxRequestBytes)
              .advertiseMaxRequestBytes(true)
              .uploadUrlProvider(fakeStorage)
              .maxUploadBytes(64L << 20);  // 64 MiB advisory upload ceiling
        }
        HttpServer http = new HttpServer(server, cb.build());
        http.start();
        System.out.println("PORT:" + http.port());
        System.out.flush();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try { http.stop(); } catch (Exception e) { LOG.warn("http stop failed during shutdown", e); }
        }));
        http.join();
    }

    private static void serveUnix(RpcServer server, Path path) throws Exception {
        UnixSocketTransport.serveForever(path, server);
    }

    /**
     * Serve raw Arrow-IPC framing over a bare TCP socket (no auth/TLS — trusted
     * networks only). Mirrors {@link #serveUnix}: prints the discovery line —
     * {@code TCP:<host>:<port>} — on stdout once bound (after which no more
     * stdout), with the actual OS-selected port resolved when {@code port == 0}.
     */
    private static void serveTcp(RpcServer server, String host, int port) throws Exception {
        TcpSocketTransport.serveForever(host, port, server, 0L, (boundHost, boundPort) -> {
            System.out.println("TCP:" + boundHost + ":" + boundPort);
            System.out.flush();
        });
    }
}
