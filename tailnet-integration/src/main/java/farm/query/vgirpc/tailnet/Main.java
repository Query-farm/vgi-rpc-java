// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.tailnet;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import farm.query.vgirpc.AuthContext;
import farm.query.vgirpc.AuthScope;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.RpcConnection;
import farm.query.vgirpc.RpcServer;
import farm.query.vgirpc.http.HttpRpcConnection;
import farm.query.vgirpc.http.HttpServer;
import farm.query.vgirpc.http.TailscalePeerIdentityProviders;
import farm.query.vgirpc.identity.IdentityAssurance;
import farm.query.vgirpc.identity.PeerAuthenticationPolicies;
import farm.query.vgirpc.identity.PeerAuthenticationPolicy;
import farm.query.vgirpc.identity.PeerEvidenceSet;
import farm.query.vgirpc.identity.PeerIdentity;
import farm.query.vgirpc.identity.PeerIdentityResult;
import farm.query.vgirpc.identity.PeerIdentityStatus;
import farm.query.vgirpc.identity.PeerResolutionContext;
import farm.query.vgirpc.identity.PeerSubjectKind;
import farm.query.vgirpc.identity.SubjectStability;
import farm.query.vgirpc.identity.TailscaleLocalApiProvider;
import farm.query.vgirpc.schema.ProtocolVersion;
import farm.query.vgirpc.transport.TcpSocketTransport;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;

/** Live-Tailnet qualification adapter; not a production proxy or deployment API. */
public final class Main {
    static final String PROVIDER = "tailscale";
    private static final int MAX_SNAPSHOT_BYTES = 65_536;
    private static final ObjectMapper JSON = new ObjectMapper(JsonFactory.builder()
            .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
            .streamReadConstraints(StreamReadConstraints.builder()
                    .maxDocumentLength(MAX_SNAPSHOT_BYTES)
                    .maxNestingDepth(16)
                    .maxStringLength(MAX_SNAPSHOT_BYTES)
                    .maxNumberLength(256)
                    .build())
            .build());

    private Main() {}

    /** Minimal shape of the Python Tailnet evidence service. */
    public interface SnapshotService {
        String snapshot();
    }

    /** Minimal cross-language worker surface used by the Python probe. */
    @ProtocolVersion("2.0.0")
    public interface ConformanceService {
        String echo_string(CallContext context, String value);
    }

    /** Context-injecting implementation; {@link CallContext} is off-wire. */
    public static final class Probe implements ConformanceService {
        private final Expectation expected;

        Probe(Expectation expected) {
            this.expected = expected;
        }

        @Override
        public String echo_string(CallContext context, String value) {
            validateContext(context, expected);
            return value;
        }
    }

    /** Expected safe evidence shape for one qualification mode. */
    record Expectation(
            String issuer,
            String evidenceSource,
            IdentityAssurance assurance,
            PeerSubjectKind subjectKind,
            SubjectStability subjectStability,
            String capability,
            String targetKind,
            String targetValue,
            String tag,
            boolean authenticated,
            boolean proxyPresent,
            String spoofedSubjectFingerprint) {}

    public static void main(String[] arguments) {
        try {
            run(arguments);
        } catch (Exception error) {
            System.err.println("vgi-rpc-tailnet-java: " + safeMessage(error));
            System.exit(1);
        }
    }

    static void run(String[] arguments) throws Exception {
        if (arguments.length == 0) {
            throw new IllegalArgumentException(
                    "usage: client-tcp|client-http|server-tcp|server-http [options]");
        }
        Args args = Args.parse(Arrays.copyOfRange(arguments, 1, arguments.length));
        switch (arguments[0]) {
            case "client-tcp" -> runTcpClient(args);
            case "client-http" -> runHttpClient(args);
            case "server-tcp" -> runTcpServer(args);
            case "server-http" -> runHttpServer(args);
            default -> throw new IllegalArgumentException("unknown mode");
        }
    }

    private static void runTcpClient(Args args) throws Exception {
        String host = args.required("--host");
        int port = parsePort(args.required("--port"));
        TcpSocketTransport transport = args.optional("--proxy") != null
                ? TcpSocketTransport.connect(host, port, args.optional("--proxy"), Duration.ofSeconds(20))
                : TcpSocketTransport.connect(host, port);
        try (RpcConnection connection = new RpcConnection(transport)) {
            SnapshotService service = connection.proxy(SnapshotService.class);
            assertStableSnapshots(service.snapshot(), service.snapshot(), clientExpectation(args));
        }
        System.out.println("Java TCP client Tailnet probe passed");
    }

    private static void runHttpClient(Args args) throws Exception {
        if (args.optional("--proxy") != null) {
            throw new IllegalArgumentException(
                    "Java HTTP SOCKS5h is unsupported because JDK HttpClient cannot guarantee remote DNS");
        }
        HttpRpcConnection.Builder builder = HttpRpcConnection.builder(args.required("--url"))
                .connectTimeout(Duration.ofSeconds(20))
                .requestTimeout(Duration.ofSeconds(20));
        if (args.optional("--spoof-login") != null) {
            builder.header("Tailscale-User-Login", args.optional("--spoof-login"));
        }
        try (HttpRpcConnection connection = builder.build()) {
            SnapshotService service = connection.proxy(SnapshotService.class);
            assertStableSnapshots(service.snapshot(), service.snapshot(), clientExpectation(args));
        }
        System.out.println("Java HTTP client Tailnet probe passed");
    }

    private static void runHttpServer(Args args) throws Exception {
        String issuer = args.required("--issuer");
        var provider = TailscalePeerIdentityProviders.serve(issuer, Set.of(
                args.value("--trusted-proxy-ipv4", "127.0.0.1"),
                args.value("--trusted-proxy-ipv6", "::1")));
        Expectation expected = new Expectation(
                issuer, "serve_proxy", IdentityAssurance.CONFIGURED_PROXY,
                PeerSubjectKind.UNKNOWN, SubjectStability.NONE,
                args.required("--expected-capability"), null, null, null,
                false, true, null);
        RpcServer rpc = new RpcServer(ConformanceService.class, new Probe(expected));
        rpc.setProtocolVersion("2.0.0");
        HttpServer server = new HttpServer(rpc, HttpServer.Config.builder()
                .host(args.value("--host", "127.0.0.1"))
                .port(parsePort(args.value("--port", "18080")))
                .peerIdentityProviders(List.of(provider))
                .peerAuthenticationPolicy(PeerAuthenticationPolicies.require(PROVIDER))
                .build());
        server.start();
        System.out.println("HTTP:" + args.value("--host", "127.0.0.1") + ":" + server.port());
        System.out.flush();
        Runtime.getRuntime().addShutdownHook(Thread.ofPlatform().unstarted(() -> stopQuietly(server)));
        server.join();
    }

    private static void runTcpServer(Args args) throws Exception {
        String issuer = args.required("--issuer");
        var provider = new TailscaleLocalApiProvider(issuer,
                new TailscaleLocalApiProvider.UnixLocalApiClient(
                        Path.of(args.value("--localapi-socket", "/var/run/tailscale/tailscaled.sock")), null));
        Expectation expected = new Expectation(
                issuer, "localapi", IdentityAssurance.LOCAL_DAEMON,
                PeerSubjectKind.TAGGED_NODE, SubjectStability.STABLE,
                args.required("--expected-capability"), "destination_ip", null,
                args.required("--expected-tag"), true, false, null);
        RpcServer rpc = new RpcServer(ConformanceService.class, new Probe(expected));
        rpc.setProtocolVersion("2.0.0");
        serveQualifiedTcp(args.value("--host", "0.0.0.0"),
                parsePort(args.value("--port", "19400")), rpc, provider);
    }

    /**
     * Adapter-owned TCP identity wrapper. The production Java raw-TCP listener does not yet
     * expose provider hooks, so this exists solely to qualify the existing provider/AuthScope
     * pieces without claiming a new production listener API.
     */
    static void serveQualifiedTcp(
            String host, int port, RpcServer rpc, TailscaleLocalApiProvider provider) throws Exception {
        Semaphore active = new Semaphore(64);
        try (ServerSocket listener = new ServerSocket()) {
            listener.setReuseAddress(true);
            listener.bind(new InetSocketAddress(host, port), 128);
            System.out.println("TCP:" + host + ":" + listener.getLocalPort());
            System.out.flush();
            while (true) {
                Socket socket = listener.accept();
                if (!active.tryAcquire()) {
                    socket.close();
                    continue;
                }
                Thread.startVirtualThread(() -> {
                    try {
                        serveQualifiedConnection(socket, rpc, provider);
                    } catch (Exception ignored) {
                        // Provider and peer-controlled failures are intentionally not logged.
                    } finally {
                        active.release();
                    }
                });
            }
        }
    }

    private static void serveQualifiedConnection(
            Socket socket, RpcServer rpc, TailscaleLocalApiProvider provider) throws Exception {
        try (socket; TcpSocketTransport transport = new TcpSocketTransport(socket)) {
            InetAddress remote = socket.getInetAddress();
            InetAddress local = socket.getLocalAddress();
            String source = endpoint(remote.getHostAddress(), socket.getPort());
            PeerResolutionContext resolution = new PeerResolutionContext(
                    "tcp", remote.getHostAddress(), source, null, local.getHostAddress(),
                    null, null, Map.of(), Map.of("remote_addr", source),
                    Instant.now().plusSeconds(5));
            PeerIdentityResult result = provider.resolve(resolution);
            PeerEvidenceSet evidence = new PeerEvidenceSet(List.of(result));
            AuthContext auth = PeerAuthenticationPolicies.primary(PROVIDER)
                    .evaluate(evidence, AuthContext.ANONYMOUS);
            AutoCloseable authScope = AuthScope.push(auth, Map.of("remote_addr", source), evidence);
            try {
                rpc.serve(transport);
            } finally {
                authScope.close();
            }
        }
    }

    static void validateContext(CallContext context, Expectation expected) {
        if (context == null) throw new SecurityException("missing call context");
        PeerEvidenceSet evidence = context.peerEvidence();
        if (evidence.status(PROVIDER) != PeerIdentityStatus.AVAILABLE) {
            throw new SecurityException("Tailscale evidence unavailable");
        }
        List<PeerIdentity> identities = evidence.forProvider(PROVIDER);
        if (identities.size() != 1) throw new SecurityException("ambiguous Tailscale evidence");
        PeerIdentity identity = identities.getFirst();
        boolean tagMatches = expected.tag() == null || stringList(identity.attributes().get("tags"))
                .contains(expected.tag());
        boolean matches = identity.issuer().equals(expected.issuer())
                && identity.evidenceSource().equals(expected.evidenceSource())
                && identity.assurance() == expected.assurance()
                && identity.subjectKind() == expected.subjectKind()
                && identity.subjectStability() == expected.subjectStability()
                && identity.subjectVerified() == (expected.subjectStability() != SubjectStability.NONE)
                && identity.capabilitiesVerified()
                && identity.capabilities().containsKey(expected.capability())
                && (identity.proxyAddress() != null) == expected.proxyPresent()
                && targetMatches(identity.attributes().get("capability_target"), expected, false)
                && tagMatches;
        PeerAuthenticationPolicy policy = expected.authenticated()
                ? PeerAuthenticationPolicies.primary(PROVIDER)
                : PeerAuthenticationPolicies.require(PROVIDER);
        AuthContext derived = policy.evaluate(evidence, AuthContext.ANONYMOUS);
        if (!matches || !derived.equals(context.auth())
                || !isSha256Hex(stringClaim(context.auth(), "peer_evidence_binding"))) {
            throw new SecurityException("unexpected Tailscale identity or authentication context");
        }
    }

    static void assertStableSnapshots(String first, String second, Expectation expected) throws Exception {
        JsonNode firstValue = validateSnapshot(first, expected);
        JsonNode secondValue = validateSnapshot(second, expected);
        if (!firstValue.equals(secondValue)) {
            throw new SecurityException("Tailnet evidence changed between qualification calls");
        }
    }

    static JsonNode validateSnapshot(String raw, Expectation expected) throws Exception {
        if (raw == null || raw.getBytes(StandardCharsets.UTF_8).length > MAX_SNAPSHOT_BYTES) {
            throw new SecurityException("invalid Tailnet evidence snapshot");
        }
        JsonNode root = JSON.readTree(raw);
        if (root == null || !root.isObject()) {
            throw new SecurityException("invalid Tailnet evidence snapshot");
        }
        JsonNode statuses = root.path("provider_status");
        JsonNode identities = root.path("identities");
        if (!statuses.isObject() || statuses.size() != 1
                || !"available".equals(statuses.path(PROVIDER).textValue())
                || !identities.isArray() || identities.size() != 1) {
            throw new SecurityException("unexpected Tailnet evidence snapshot");
        }
        JsonNode identity = identities.get(0);
        boolean subjectExpected = expected.subjectStability() != SubjectStability.NONE;
        JsonNode subjectFingerprint = identity.path("subject_fingerprint");
        JsonNode principalFingerprint = root.path("auth").path("principal_fingerprint");
        boolean fingerprintsMatch = optionalSha256(subjectFingerprint, subjectExpected)
                && optionalSha256(principalFingerprint, expected.authenticated());
        boolean spoofResistant = expected.spoofedSubjectFingerprint() == null
                || !expected.spoofedSubjectFingerprint().equals(subjectFingerprint.textValue());
        boolean matches = PROVIDER.equals(identity.path("provider").textValue())
                && expected.issuer().equals(identity.path("issuer").textValue())
                && expected.evidenceSource().equals(identity.path("evidence_source").textValue())
                && expected.assurance().wireValue().equals(identity.path("assurance").textValue())
                && expected.subjectKind().wireValue().equals(identity.path("subject_kind").textValue())
                && expected.subjectStability().wireValue().equals(identity.path("subject_stability").textValue())
                && identity.path("subject_verified").asBoolean(!subjectExpected) == subjectExpected
                && identity.path("capabilities_verified").asBoolean(false)
                && containsText(identity.path("capability_names"), expected.capability())
                && identity.path("proxy_present").asBoolean(!expected.proxyPresent()) == expected.proxyPresent()
                && containsOptionalText(identity.path("tags"), expected.tag())
                && targetMatches(identity.get("capability_target"), expected, true)
                && root.path("auth").path("authenticated").asBoolean(!expected.authenticated())
                        == expected.authenticated()
                && authDomainMatches(root.path("auth").get("domain"), expected.authenticated())
                && root.path("auth").path("principal_matches_identity").asBoolean(!expected.authenticated())
                        == expected.authenticated()
                && root.path("auth").path("peer_evidence_binding_present").asBoolean(false)
                && fingerprintsMatch
                && spoofResistant;
        if (!matches) throw new SecurityException("unexpected Tailnet evidence snapshot");
        return root;
    }

    private static Expectation clientExpectation(Args args) {
        boolean authenticated = args.flag("--expect-authenticated");
        String spoofLogin = args.optional("--spoof-login");
        return new Expectation(
                args.required("--expected-issuer"),
                args.required("--expected-evidence-source"),
                parseAssurance(args.required("--expected-assurance")),
                parseSubjectKind(args.required("--expected-subject-kind")),
                parseStability(args.required("--expected-subject-stability")),
                args.required("--expected-capability"),
                args.optional("--expected-target-kind"),
                args.optional("--expected-target-value"),
                args.optional("--expected-tag"),
                authenticated,
                args.flag("--expect-proxy"),
                spoofLogin != null ? sha256Hex("login:" + spoofLogin) : null);
    }

    @SuppressWarnings("unchecked")
    private static List<String> stringList(Object value) {
        if (!(value instanceof List<?> values)) return List.of();
        return values.stream().filter(String.class::isInstance).map(String.class::cast).toList();
    }

    private static boolean targetMatches(Object raw, Expectation expected, boolean redacted) {
        if (expected.targetKind() == null) return raw == null || raw instanceof JsonNode node && node.isNull();
        if (redacted) {
            if (!(raw instanceof JsonNode node) || !node.isObject()
                    || !expected.targetKind().equals(node.path("kind").textValue())) return false;
            if (expected.targetValue() != null) {
                return expected.targetValue().equals(node.path("value").textValue());
            }
            return !"destination_ip".equals(expected.targetKind()) || !node.has("value");
        }
        if (!(raw instanceof Map<?, ?> target)
                || !expected.targetKind().equals(target.get("kind"))) return false;
        Object value = target.get("value");
        if (expected.targetValue() != null) return expected.targetValue().equals(value);
        return !"destination_ip".equals(expected.targetKind()) || isIpLiteral(value);
    }

    private static boolean isIpLiteral(Object value) {
        if (!(value instanceof String text) || text.isBlank()) return false;
        if (text.indexOf(':') < 0) {
            String[] parts = text.split("\\.", -1);
            if (parts.length != 4) return false;
            for (String part : parts) {
                if (part.isEmpty() || !part.chars().allMatch(Character::isDigit)) return false;
                try {
                    if (Integer.parseInt(part) > 255) return false;
                } catch (NumberFormatException ignored) {
                    return false;
                }
            }
            return true;
        }
        if (!text.matches("[0-9A-Fa-f:.]+")) return false;
        try {
            return InetAddress.getByName(text).getAddress().length == 16;
        } catch (Exception ignored) {
            return false;
        }
    }

    private static boolean authDomainMatches(JsonNode domain, boolean authenticated) {
        return authenticated ? domain != null && PROVIDER.equals(domain.textValue())
                : domain == null || domain.isNull();
    }

    private static boolean containsText(JsonNode array, String expected) {
        if (!array.isArray()) return false;
        for (JsonNode item : array) if (expected.equals(item.textValue())) return true;
        return false;
    }

    private static boolean containsOptionalText(JsonNode array, String expected) {
        return expected == null || containsText(array, expected);
    }

    private static boolean optionalSha256(JsonNode node, boolean expected) {
        return expected ? node != null && isSha256Hex(node.textValue())
                : node == null || node.isNull();
    }

    private static boolean isSha256Hex(String value) {
        return value != null && value.matches("[0-9a-f]{64}");
    }

    private static String stringClaim(AuthContext auth, String name) {
        Object value = auth.claims().get(name);
        return value instanceof String text ? text : null;
    }

    private static String sha256Hex(String value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception error) {
            throw new IllegalStateException("SHA-256 unavailable", error);
        }
    }

    private static String endpoint(String address, int port) {
        return address.indexOf(':') >= 0 ? "[" + address + "]:" + port : address + ":" + port;
    }

    private static int parsePort(String raw) {
        int port = Integer.parseInt(raw);
        if (port < 0 || port > 65_535) throw new IllegalArgumentException("invalid port");
        return port;
    }

    private static IdentityAssurance parseAssurance(String raw) {
        for (IdentityAssurance value : IdentityAssurance.values()) {
            if (value.wireValue().equals(raw)) return value;
        }
        throw new IllegalArgumentException("unknown assurance");
    }

    private static PeerSubjectKind parseSubjectKind(String raw) {
        for (PeerSubjectKind value : PeerSubjectKind.values()) {
            if (value.wireValue().equals(raw)) return value;
        }
        throw new IllegalArgumentException("unknown subject kind");
    }

    private static SubjectStability parseStability(String raw) {
        for (SubjectStability value : SubjectStability.values()) {
            if (value.wireValue().equals(raw)) return value;
        }
        throw new IllegalArgumentException("unknown subject stability");
    }

    private static String safeMessage(Exception error) {
        return error instanceof IllegalArgumentException ? error.getMessage() : "qualification failed";
    }

    private static void stopQuietly(HttpServer server) {
        try {
            server.stop();
        } catch (Exception ignored) {
            // Best-effort process shutdown.
        }
    }

    /** Minimal strict option parser: one value per option and no duplicate keys. */
    record Args(Map<String, String> values, Set<String> flags) {
        static Args parse(String[] raw) {
            Map<String, String> values = new LinkedHashMap<>();
            Set<String> flags = new java.util.LinkedHashSet<>();
            Set<String> booleanFlags = Set.of("--expect-authenticated", "--expect-proxy");
            for (int index = 0; index < raw.length; index++) {
                String name = raw[index];
                if (!name.startsWith("--") || values.containsKey(name) || flags.contains(name)) {
                    throw new IllegalArgumentException("invalid or duplicate option");
                }
                if (booleanFlags.contains(name)) {
                    flags.add(name);
                    continue;
                }
                if (++index >= raw.length || raw[index].startsWith("--")) {
                    throw new IllegalArgumentException("option value is missing");
                }
                values.put(name, raw[index]);
            }
            return new Args(Map.copyOf(values), Set.copyOf(flags));
        }

        String required(String name) {
            String value = values.get(name);
            if (value == null || value.isBlank()) throw new IllegalArgumentException("required option is missing: " + name);
            return value;
        }

        String optional(String name) { return values.get(name); }
        String value(String name, String fallback) { return values.getOrDefault(name, fallback); }
        boolean flag(String name) { return flags.contains(name); }
    }
}
