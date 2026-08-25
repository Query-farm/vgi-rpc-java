// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.conformance.worker;

import farm.query.vgirpc.AccessLogHook;
import farm.query.vgirpc.AuthContext;
import farm.query.vgirpc.RpcServer;
import farm.query.vgirpc.conformance.ConformanceService;
import farm.query.vgirpc.conformance.ConformanceServiceImpl;
import farm.query.vgirpc.conformance.TransportKindProbeService;
import farm.query.vgirpc.conformance.TransportKindProbeServiceImpl;
import farm.query.vgirpc.external.ExternalLocationConfig;
import farm.query.vgirpc.external.LocationResolver;
import farm.query.vgirpc.http.AuthFailure;
import farm.query.vgirpc.http.AuthReason;
import farm.query.vgirpc.http.AuthUnavailableException;
import farm.query.vgirpc.http.Authenticator;
import farm.query.vgirpc.http.HttpPreHandler;
import farm.query.vgirpc.http.HttpServer;
import farm.query.vgirpc.http.TokenIdentity;
import farm.query.vgirpc.http.auth.BearerAuthenticator;
import farm.query.vgirpc.http.auth.JwtAuthenticator;
import farm.query.vgirpc.http.auth.MTlsAuthenticator;
import farm.query.vgirpc.http.auth.OAuthPkce;
import farm.query.vgirpc.http.auth.OidcMetadata;
import farm.query.vgirpc.http.auth.ProxyProof;
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
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

public final class Main {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(Main.class);

    private Main() {}

    public static void main(String[] args) throws Exception {
        byte[] tokenKey = null;
        long tokenTtl = 0;
        String mode = null;
        String unixPath = null;
        // Self-shutdown idle timeout for --unix, seconds; 0 = no timeout. Lets this worker double
        // as a launch:-transport fixture (see docs/launcher-protocol.md's worker CLI surface —
        // every launcher-managed worker must accept --unix PATH --idle-timeout SEC).
        double unixIdleTimeoutSeconds = 0;
        // Raw-TCP target: host defaults to loopback; port 0 ⇒ OS auto-selects.
        String tcpHost = "127.0.0.1";
        int tcpPort = 0;
        Authenticator authenticator = null;
        List<HttpPreHandler> preHandlers = new ArrayList<>();
        String fakeStorageUrl = null;
        long externalizeThreshold = 4096;
        long maxFetchBytes = -1;
        long maxDecompressedFetchBytes = -1;
        boolean rejectLocalhostRedirects = false;
        // -1 sentinel: "unset"; falls back to externalizeThreshold for backward compat.
        long maxRequestBytes = -1;
        String compression = "none";
        String accessLogPath = null;
        // Optional access-log behaviours, mirroring the Python reference's flag
        // names so one driver can exercise every port the same way.
        double accessLogSample = 1.0;
        boolean accessLogAsync = false;
        int accessLogQueueSize = 10000;
        boolean accessLogPayloads = true;
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
        // Proxy proof: HMAC evidence that a request arrived through a trusted
        // proxy.  Mirrors the reference worker's CLI
        // (tests/serve_conformance_http_proof.py) so the shared TestProxyProof
        // group drives every language the same way.  The gate is an AND with
        // whatever authenticates the caller, so it is installed by wrapping —
        // never handed to Authenticator.chain, whose first-authenticated-wins
        // semantics would let a later credential bypass it.
        boolean httpProof = false;
        String proofMode = "require";
        String proofOriginId = "conformance-origin";
        String proofSecrets = "";
        int proofSkew = 30;
        boolean proofReplayCache = true;
        // The call-state cache is a pure accelerator; --no-call-state-cache
        // turns every stream continuation onto the miss path so the shared
        // TestColdCallStateCache group observes it deterministically.
        boolean callStateCache = true;
        // CORS is opt-in, so the default worker must stay header-free — that
        // "off by default" property is itself a conformance contract
        // (TestCorsOffMode), and only --cors-origin opts a worker out of it.
        List<String> corsOrigins = new ArrayList<>();
        // Token introspection is off unless asked for -- that "absent by default"
        // property is itself a conformance contract (TestTokenIntrospectionOffMode),
        // which runs against the plain worker.
        boolean introspect = false;
        boolean transportKindProbe = false;
        boolean failServeStartOnce = false;
        ArgCursor c = new ArgCursor(args);
        while (c.hasNext()) {
            String a = c.next();
            switch (a) {
                case "--http" -> mode = "http";
                case "--unix" -> { mode = "unix"; unixPath = c.requireValue(a); }
                case "--idle-timeout" -> unixIdleTimeoutSeconds = Double.parseDouble(c.requireValue(a));
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
                // Both imply HTTP, mirroring the Go and Rust workers' flags.
                case "--http-auth" -> { mode = "http"; authenticator = rejectAllAuthenticator(); }
                case "--http-proof" -> { mode = "http"; httpProof = true; }
                case "--proof-mode" -> proofMode = c.requireValue(a);
                case "--proof-origin-id" -> proofOriginId = c.requireValue(a);
                case "--proof-secrets" -> proofSecrets = c.requireValue(a);
                case "--proof-skew" -> proofSkew = Integer.parseInt(c.requireValue(a));
                case "--proof-no-replay-cache" -> proofReplayCache = false;
                case "--fake-storage" -> fakeStorageUrl = c.requireValue(a);
                case "--externalize-threshold" -> externalizeThreshold = Long.parseLong(c.requireValue(a));
                case "--max-fetch-bytes" -> maxFetchBytes = Long.parseLong(c.requireValue(a));
                case "--max-decompressed-fetch-bytes" ->
                        maxDecompressedFetchBytes = Long.parseLong(c.requireValue(a));
                case "--reject-localhost-redirects" -> rejectLocalhostRedirects = true;
                case "--max-request-bytes" -> maxRequestBytes = Long.parseLong(c.requireValue(a));
                case "--compression" -> compression = c.requireValue(a);
                case "--access-log" -> accessLogPath = c.requireValue(a);
                case "--access-log-sample" -> accessLogSample = Double.parseDouble(c.requireValue(a));
                case "--access-log-async" -> accessLogAsync = true;
                case "--access-log-queue-size" -> accessLogQueueSize = Integer.parseInt(c.requireValue(a));
                case "--access-log-no-payloads" -> accessLogPayloads = false;
                // Accepted and ignored. The other ports gate request_data behind a
                // DEBUG logger, so the porting guide's canonical verification command
                // passes --access-log-debug to turn it on; this worker writes the
                // record directly and logs payloads already, but rejecting an unknown
                // arg would make that one command line fail on Java alone. Deliberately
                // not the inverse of --access-log-no-payloads: payloads-by-default is
                // what let this port catch a request_data bug the others logged past.
                case "--access-log-debug" -> { }
                case "--strict" -> strictMode = true;
                case "--max-response-bytes" -> maxResponseBytes = Long.parseLong(c.requireValue(a));
                case "--max-externalized-response-bytes" ->
                        maxExternalizedResponseBytes = Long.parseLong(c.requireValue(a));
                case "--no-compression" -> responseCompression = false;
                case "--no-sticky" -> stickyEnabled = false;
                case "--sticky-ttl" -> stickyTtl = Long.parseLong(c.requireValue(a));
                case "--sticky-auth" -> authenticator = principalHeaderAuthenticator();
                case "--no-call-state-cache" -> callStateCache = false;
                // Implies HTTP, like --http-auth and --http-proof; repeatable.
                case "--cors-origin" -> { mode = "http"; corsOrigins.add(c.requireValue(a)); }
                // Implies HTTP, and implies principal-header auth below so the
                // introspector allowlist has something to check.
                case "--introspect" -> { mode = "http"; introspect = true; }
                case "--transport-kind-probe" -> transportKindProbe = true;
                case "--fail-serve-start-once" -> failServeStartOnce = true;
                default -> { System.err.println("unknown arg: " + a); System.exit(2); }
            }
        }
        RpcServer server;
        if (transportKindProbe) {
            server = new RpcServer(TransportKindProbeService.class, new TransportKindProbeServiceImpl());
        } else {
            server = new RpcServer(ConformanceService.class, new ConformanceServiceImpl());
            // Match the Python reference's ConformanceService.protocol_version so the
            // describe conformance suite sees the same MAJOR.MINOR.PATCH label.
            server.setProtocolVersion("2.0.0");
        }
        if (failServeStartOnce) {
            AtomicBoolean first = new AtomicBoolean(true);
            server.setServeStartHook(kind -> {
                if (first.compareAndSet(true, false)) {
                    throw new IllegalStateException("conformance injected on_serve_start failure");
                }
            });
        }
        // Applied after the loop so flag order does not matter, and only when no
        // stronger mode was selected.
        if (introspect && authenticator == null) authenticator = principalHeaderAuthenticator();
        if (httpProof) {
            authenticator = buildProofGate(
                    proofMode, proofOriginId, proofSecrets, proofSkew, proofReplayCache, authenticator);
        }
        FakeStorage fakeStorage = null;
        if (fakeStorageUrl != null) {
            fakeStorage = new FakeStorage(fakeStorageUrl);
            ExternalLocationConfig.Builder cfgB = ExternalLocationConfig.builder()
                    .storage(fakeStorage)
                    .thresholdBytes(externalizeThreshold);
            if (rejectLocalhostRedirects) {
                cfgB.urlValidator(uri -> {
                    if (!"http".equalsIgnoreCase(uri.getScheme())
                            || !"127.0.0.1".equals(uri.getHost())) {
                        throw new IllegalArgumentException("URL rejected by conformance policy");
                    }
                });
            } else {
                cfgB.urlValidator(ExternalLocationConfig.permissiveValidator());
            }
            if (maxFetchBytes >= 0) cfgB.maxFetchBytes(maxFetchBytes);
            if (maxDecompressedFetchBytes >= 0) {
                cfgB.maxDecompressedBytes(maxDecompressedFetchBytes);
            }
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
            AccessLogHook hook = AccessLogHook.builder(accessLogOut)
                    .serverVersion("vgi-rpc-java-conformance")
                    .sampleRate(accessLogSample)
                    .logPayloads(accessLogPayloads)
                    .asyncQueueSize(accessLogAsync ? accessLogQueueSize : 0)
                    .build();
            server.setDispatchHook(hook);
            // An async hook holds records in a queue; without this a normal
            // shutdown would discard whatever had not reached disk, and the
            // driver would read a truncated log as a conformance failure.
            Runtime.getRuntime().addShutdownHook(new Thread(hook::close));
        }
        if (mode == null) { servePipe(server); return; }
        if (strictMode) {
            if (maxResponseBytes <= 0) maxResponseBytes = 1024L * 1024L;
            if (maxExternalizedResponseBytes <= 0) maxExternalizedResponseBytes = 1024L * 1024L;
        }
        switch (mode) {
            case "http" -> serveHttp(server, tokenKey, tokenTtl, authenticator, preHandlers, fakeStorage,
                    maxRequestBytes >= 0 ? maxRequestBytes : externalizeThreshold,
                    maxRequestBytes >= 0,
                    maxResponseBytes, maxExternalizedResponseBytes,
                    stickyEnabled, stickyTtl, responseCompression,
                    // Only require mode denies, so only require mode advertises.
                    httpProof && "require".equals(proofMode),
                    callStateCache, corsOrigins, introspect);
            case "unix" -> serveUnix(server, Path.of(unixPath), (long) (unixIdleTimeoutSeconds * 1000));
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

    /**
     * Build the proxy-proof gate, wrapping {@code inner} when a credential mode was also selected.
     *
     * <p>{@code off} installs nothing at all: the feature is opt-in, and a worker that is not
     * configured for it must behave exactly as it did before the feature existed — which is what
     * {@code TestProxyProofOffMode} checks.
     */
    private static Authenticator buildProofGate(String mode, String originId, String secrets,
                                                 int skewSeconds, boolean replayCache,
                                                 Authenticator inner) {
        if ("off".equals(mode)) return inner;
        ProxyProof.Mode m = switch (mode) {
            case "allow" -> ProxyProof.Mode.ALLOW;
            case "require" -> ProxyProof.Mode.REQUIRE;
            default -> null;
        };
        if (m == null) {
            System.err.println("unknown --proof-mode: " + mode);
            System.exit(2);
        }
        ProxyProof.Config cfg = ProxyProof.Config.of(m, originId, ProxyProof.parseSecrets(secrets))
                .withSkewSeconds(skewSeconds);
        if (!replayCache) cfg = cfg.withoutReplayCache();
        return ProxyProof.require(cfg, inner);
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

    /**
     * Resolve the principal named in {@code X-Conformance-Principal}, or stay anonymous.
     *
     * <p>Backs {@code TestSticky::test_cross_principal_replay_rejected}, which needs one
     * worker reachable as two distinct identities so it can open a session as one and
     * replay the token as the other. Naming yourself in a header is obviously not
     * authentication — it is the cheapest thing every port can implement identically,
     * and the test only needs the two identities to be distinguishable.
     *
     * <p>Requests without the header stay anonymous rather than being rejected: the
     * conformance suite probes {@code GET /health} and the capability endpoint before
     * it authenticates anything.
     */
    private static Authenticator principalHeaderAuthenticator() {
        return request -> {
            String principal = request.getHeader("X-Conformance-Principal");
            if (principal == null || principal.isEmpty()) {
                return AuthContext.ANONYMOUS;
            }
            return new AuthContext("conformance", true, principal, Collections.emptyMap());
        };
    }

    // Fixed values the shared TestTokenIntrospection group is written against:
    // it posts the subject credential and asserts the principal, so a port
    // supplying the conformance_http_introspect_port fixture must configure
    // exactly these.
    private static final String CONFORMANCE_INTROSPECTOR = "conformance-introspector";
    private static final String CONFORMANCE_SUBJECT_TOKEN = "conformance-opaque-subject-token";
    private static final String CONFORMANCE_SUBJECT_PRINCIPAL = "subject@conformance.example";
    private static final String CONFORMANCE_SUBJECT_TOKEN_NAME = "conformance-subject";
    private static final long CONFORMANCE_SUBJECT_TTL = 300;
    /**
     * A JWS-shaped credential the resolver <em>would</em> resolve.
     *
     * <p>Deliberately resolvable: against an unknown JWS a port with no shape
     * guard rejects it as unknown and passes the test for the wrong reason. Made
     * resolvable, the guard is the only thing that can produce a rejection — a
     * port missing it answers 200 and fails.
     */
    private static final String CONFORMANCE_JWS_TRAP_TOKEN =
            "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhbGljZSJ9.c2lnbmF0dXJl";
    /**
     * The credential whose resolution is <em>unknowable</em> rather than unknown.
     *
     * <p>The shared suite posts it to check that a backing-store outage surfaces
     * as a transient 503 and not as this endpoint's own definitive 404 — which a
     * caller may negative-cache, so a briefly unreachable store would be
     * remembered as a bad credential for the cache's lifetime.
     */
    private static final String CONFORMANCE_UNAVAILABLE_TOKEN = "conformance-unavailable-token";

    /**
     * Resolve the fixed credentials the shared tests post.
     *
     * <p>Three answers, deliberately: an identity, {@code Optional.empty()} for
     * "does not resolve", and {@link AuthUnavailableException} for "I could not
     * find out". The third is not a flavour of the second — an empty result
     * becomes the definitive 404 a caller may negative-cache.
     */
    private static Optional<TokenIdentity> resolveConformanceToken(String token) {
        if (CONFORMANCE_UNAVAILABLE_TOKEN.equals(token)) {
            throw new AuthUnavailableException("conformance: mapping store unreachable");
        }
        if (CONFORMANCE_SUBJECT_TOKEN.equals(token) || CONFORMANCE_JWS_TRAP_TOKEN.equals(token)) {
            return Optional.of(new TokenIdentity(
                    CONFORMANCE_SUBJECT_PRINCIPAL, CONFORMANCE_SUBJECT_TOKEN_NAME, CONFORMANCE_SUBJECT_TTL));
        }
        return Optional.empty();
    }

    /** Conformance-fixture affordance, never part of the protocol. */
    private static final String CONFORMANCE_REASON_HEADER = "X-Conformance-Auth-Reason";

    /**
     * The reasons a <em>request</em> may ask to be refused with.
     *
     * <p>{@code proxy_required} is deliberately absent: the unauthorized spec derives it from
     * server configuration, never from the request, so a worker letting a caller summon it would
     * advertise a proxy dependency that does not exist. {@code unauthorized} is absent because it
     * is what the <em>absence</em> of a requested reason must produce — making it requestable
     * would hide whether the fallback path works at all. Anything not in this map, including a
     * typo, falls through to that fallback, so a test asking for a reason it cannot get fails
     * rather than quietly passing.
     */
    private static final Map<String, AuthReason> REQUESTABLE_REASONS = Map.of(
            "missing_credential", AuthReason.MISSING_CREDENTIAL,
            "invalid_credential", AuthReason.INVALID_CREDENTIAL,
            "expired_credential", AuthReason.EXPIRED_CREDENTIAL,
            "insufficient_scope", AuthReason.INSUFFICIENT_SCOPE);

    /**
     * Refuse every RPC call, with the reason the request named if it named one.
     *
     * <p>Backs the shared {@code TestHealth} exemption check and {@code TestUnauthorized}. The
     * latter needs one worker that <em>discriminates</em> between codes: membership in the closed
     * set is satisfied by a server stamping {@code unauthorized} on every 401, which is exactly
     * the failure that makes the code not worth branching on.
     */
    private static Authenticator rejectAllAuthenticator() {
        return request -> {
            String requested = request.getHeader(CONFORMANCE_REASON_HEADER);
            AuthReason reason = requested == null ? null : REQUESTABLE_REASONS.get(requested);
            if (reason != null) {
                // The detail is the code itself so the suite can assert header and body agree
                // without pinning prose.
                throw new AuthFailure(reason, reason.code());
            }
            throw new AuthFailure("authentication required");
        };
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
                                   boolean advertiseMaxRequestBytes,
                                   long maxResponseBytes,
                                   long maxExternalizedResponseBytes,
                                   boolean stickyEnabled,
                                   long stickyTtl,
                                   boolean responseCompression,
                                   boolean proxyProofRequired,
                                   boolean callStateCache,
                                   List<String> corsOrigins,
                                   boolean introspect) throws Exception {
        HttpServer.Config.Builder cb = HttpServer.Config.builder()
                .tokenKey(tokenKey)
                .tokenTtlSeconds(tokenTtl)
                .authenticator(authenticator)
                .preHandlers(preHandlers)
                .proxyProofRequired(proxyProofRequired);
        if (!callStateCache) cb.callStateCacheMaxEntries(0);
        if (!corsOrigins.isEmpty()) cb.corsOrigins(corsOrigins);
        if (introspect) {
            cb.tokenIntrospection(Main::resolveConformanceToken, List.of(CONFORMANCE_INTROSPECTOR))
              .introspectTtlSeconds(CONFORMANCE_SUBJECT_TTL);
        }
        // Empty producible set ⇒ present-but-empty VGI-Supported-Encodings and
        // no compression, whatever the client asks for. null would mean "unset"
        // and fall back to the default set, so the empty list is load-bearing.
        if (!responseCompression) cb.supportedEncodings(List.of());
        if (advertiseMaxRequestBytes) {
            cb.maxRequestBytes(maxRequestBytes)
              .advertiseMaxRequestBytes(true);
        }
        if (maxResponseBytes > 0) {
            // The advertised cap is an operator policy checked after the body
            // is serialized. Keep the internal safety ceiling above it so an
            // overshoot can be replaced by the protocol's structured error.
            long internalCeiling = maxResponseBytes > Long.MAX_VALUE / 2
                    ? Long.MAX_VALUE
                    : maxResponseBytes * 2;
            cb.maxResponseBytes(Math.max(HttpServer.Config.DEFAULT_MAX_RESPONSE_BYTES, internalCeiling))
              .advertisedMaxResponseBytes(maxResponseBytes);
        }
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

    private static void serveUnix(RpcServer server, Path path, long idleTimeoutMs) throws Exception {
        UnixSocketTransport.serveForever(path, server, idleTimeoutMs);
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
