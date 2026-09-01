// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0
package farm.query.vgirpc.identity;

import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.net.StandardProtocolFamily;
import java.net.SocketAddress;
import java.net.URI;
import java.net.URLEncoder;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Base64;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

/** No-cache Tailscale LocalAPI WhoIs peer evidence. */
public final class TailscaleLocalApiProvider implements PeerIdentityProvider {
    private static final String PROVIDER = "tailscale";
    private static final int MAX_RESPONSE_BYTES = 65_536;
    private static final int MAX_HEADER_BYTES = 16_384;
    private static final int MAX_JSON_VALUES = 4_096;
    private static final Pattern HEADER_NAME = Pattern.compile("[!#$%&'*+.^_`|~0-9A-Za-z-]+");
    private static final Pattern TAILSCALE_TAG = Pattern.compile("tag:[A-Za-z][A-Za-z0-9-]*");
    private static final ObjectMapper JSON = new ObjectMapper(
            com.fasterxml.jackson.core.JsonFactory.builder()
                    .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
                    .streamReadConstraints(StreamReadConstraints.builder()
                            .maxDocumentLength(MAX_RESPONSE_BYTES).maxNestingDepth(16)
                            .maxStringLength(MAX_RESPONSE_BYTES).maxNumberLength(256).build())
                    .build());

    private final String issuer;
    private final LocalApiClient client;

    public TailscaleLocalApiProvider(String issuer, LocalApiClient client) {
        if (issuer == null || issuer.isBlank() || containsControl(issuer)) throw new IllegalArgumentException("issuer is required");
        JsonValues.requireWellFormed(issuer, "issuer");
        this.issuer = issuer;
        this.client = java.util.Objects.requireNonNull(client, "client");
    }

    @Override public String provider() { return PROVIDER; }

    @Override public PeerIdentityResult resolve(PeerResolutionContext context) {
        if (context.immediatePeer() == null && context.sourceEndpoint() == null
                && context.assertedPeer() == null) {
            return result(PeerIdentityStatus.NOT_APPLICABLE);
        }
        try {
            LocalApiResponse response = client.whois(context);
            if (response.status() == 404) return result(PeerIdentityStatus.NO_MATCH);
            if (response.status() == 401 || response.status() == 403) return result(PeerIdentityStatus.PERMISSION_DENIED);
            if (response.status() >= 500 && response.status() <= 599) return result(PeerIdentityStatus.UNAVAILABLE);
            if (response.status() != 200) return result(PeerIdentityStatus.INVALID);
            if (!"application/json".equalsIgnoreCase(response.contentType())) {
                return result(PeerIdentityStatus.INVALID);
            }
            if (response.body().length > MAX_RESPONSE_BYTES) return result(PeerIdentityStatus.INVALID);
            JsonNode root = JSON.readTree(response.body());
            if (root == null || !root.isObject()) return result(PeerIdentityStatus.INVALID);
            requireJsonValueLimit(root);
            JsonNode node = root.path("Node");
            JsonNode profile = root.path("UserProfile");
            List<String> tags = strings(node.path("Tags"));
            boolean tagged = !tags.isEmpty();
            String subject;
            PeerSubjectKind kind;
            Map<String, Object> attributes = new LinkedHashMap<>();
            if (tagged) {
                String stableId = text(node, "StableID");
                if (stableId == null) return result(PeerIdentityStatus.INVALID);
                subject = "node:" + stableId;
                kind = PeerSubjectKind.TAGGED_NODE;
            } else {
                String userId = positiveNumericId(profile.get("ID"));
                if (userId == null) return result(PeerIdentityStatus.INVALID);
                subject = "user:" + userId;
                kind = PeerSubjectKind.USER;
            }
            put(attributes, "user_id", positiveNumericId(profile.get("ID")));
            put(attributes, "user_login", text(profile, "LoginName"));
            put(attributes, "user_display_name", text(profile, "DisplayName"));
            put(attributes, "node_id", text(node, "StableID"));
            put(attributes, "node_name", text(node, "Name"));
            attributes.put("tags", tags);
            if (context.serviceName() != null) attributes.put("capability_target", Map.of("kind", "service", "value", context.serviceName()));
            else if (context.destinationAddress() != null) {
                attributes.put("capability_target", Map.of("kind", "destination_ip", "value",
                        normalizeDestinationIp(context.destinationAddress())));
            } else attributes.put("capability_target", Map.of("kind", "node"));
            Map<String, Object> capabilities = objectOfArrays(root.path("CapMap"));
            PeerIdentity identity = new PeerIdentity(PROVIDER, "localapi", IdentityAssurance.LOCAL_DAEMON,
                    issuer, context.transport(), kind, subject, SubjectStability.STABLE, true,
                    attributes, capabilities, true,
                    normalizedSourceIp(context), normalizedProxyIp(context));
            return PeerIdentityResult.available(identity);
        } catch (LocalApiPermissionException e) {
            return result(PeerIdentityStatus.PERMISSION_DENIED);
        } catch (com.fasterxml.jackson.core.JsonProcessingException | MalformedLocalApiResponseException e) {
            return result(PeerIdentityStatus.INVALID);
        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) Thread.currentThread().interrupt();
            return result(PeerIdentityStatus.UNAVAILABLE);
        } catch (RuntimeException e) {
            return result(PeerIdentityStatus.INVALID);
        }
    }

    /** One uncached WhoIs lookup. Implementations must not invoke the tailscale CLI. */
    @FunctionalInterface
    public interface LocalApiClient {
        LocalApiResponse whois(PeerResolutionContext context) throws IOException, InterruptedException;
    }

    public record LocalApiResponse(int status, byte[] body, String contentType) {
        /** Source-compatible constructor for explicit/custom clients returning LocalAPI JSON. */
        public LocalApiResponse(int status, byte[] body) { this(status, body, "application/json"); }
        public LocalApiResponse { body = body == null ? new byte[0] : body.clone(); }
        @Override public byte[] body() { return body.clone(); }
    }

    /** LocalAPI over an explicitly configured HTTP endpoint and optional token. */
    public static final class HttpLocalApiClient implements LocalApiClient, AutoCloseable {
        private final URI endpoint;
        private final String token;
        private final Duration defaultTimeout;
        private final InetAddress[] addresses;

        public HttpLocalApiClient(URI endpoint, String token) {
            this(endpoint, token, Duration.ofSeconds(5));
        }

        public HttpLocalApiClient(URI endpoint, String token, Duration defaultTimeout) {
            if (endpoint == null || !"http".equals(endpoint.getScheme()) || endpoint.getHost() == null) {
                throw new IllegalArgumentException("LocalAPI endpoint must be an absolute http URI");
            }
            if (endpoint.getUserInfo() != null || endpoint.getQuery() != null || endpoint.getFragment() != null) {
                throw new IllegalArgumentException("LocalAPI endpoint cannot contain credentials, a query, or a fragment");
            }
            if (token != null && token.codePoints().anyMatch(code -> code <= 0x1f || code == 0x7f)) {
                throw new IllegalArgumentException("LocalAPI token contains control characters");
            }
            if (defaultTimeout == null || defaultTimeout.isZero() || defaultTimeout.isNegative()) {
                throw new IllegalArgumentException("defaultTimeout must be positive");
            }
            this.endpoint = endpoint;
            this.token = token;
            this.defaultTimeout = defaultTimeout;
            try {
                // LocalAPI overrides are deployment configuration. Resolve once at
                // construction so blocking JVM DNS can never consume a request's
                // monotonic identity-resolution budget or change underneath it.
                this.addresses = InetAddress.getAllByName(endpoint.getHost());
            } catch (java.net.UnknownHostException e) {
                throw new IllegalArgumentException("LocalAPI endpoint host could not be resolved", e);
            }
        }

        @Override public LocalApiResponse whois(PeerResolutionContext context) throws IOException, InterruptedException {
            IoDeadline deadline = deadline(context, defaultTimeout);
            int port = endpoint.getPort() >= 0 ? endpoint.getPort() : 80;
            IOException last = null;
            for (InetAddress address : addresses) {
                try (SocketChannel channel = SocketChannel.open()) {
                    channel.configureBlocking(false);
                    try (Selector selector = Selector.open()) {
                        SelectionKey key = channel.register(selector, 0);
                        connect(channel, new InetSocketAddress(address, port), selector, key, deadline);
                        writeAll(channel, selector, key, request(context, token), deadline);
                        return readHttp(channel, selector, key, deadline);
                    }
                } catch (IOException e) {
                    last = e;
                }
            }
            throw last != null ? last : new IOException("LocalAPI endpoint resolved without usable addresses");
        }

        @Override public void close() {}
    }

    /** LocalAPI over a Unix domain socket (Linux/BSD and compatible macOS installations). */
    public static final class UnixLocalApiClient implements LocalApiClient {
        private final Path socketPath;
        private final String token;
        private final Duration defaultTimeout;

        public UnixLocalApiClient(Path socketPath, String token) {
            this(socketPath, token, Duration.ofSeconds(5));
        }

        public UnixLocalApiClient(Path socketPath, String token, Duration defaultTimeout) {
            this.socketPath = java.util.Objects.requireNonNull(socketPath, "socketPath");
            validateToken(token);
            if (defaultTimeout == null || defaultTimeout.isZero() || defaultTimeout.isNegative()) {
                throw new IllegalArgumentException("defaultTimeout must be positive");
            }
            this.token = token;
            this.defaultTimeout = defaultTimeout;
        }

        @Override public LocalApiResponse whois(PeerResolutionContext context) throws IOException, InterruptedException {
            IoDeadline deadline = deadline(context, defaultTimeout);
            try (SocketChannel channel = SocketChannel.open(StandardProtocolFamily.UNIX)) {
                channel.configureBlocking(false);
                try (Selector selector = Selector.open()) {
                    SelectionKey key = channel.register(selector, 0);
                    connect(channel, UnixDomainSocketAddress.of(socketPath), selector, key, deadline);
                    writeAll(channel, selector, key, request(context, token), deadline);
                    return readHttp(channel, selector, key, deadline);
                }
            }
        }
    }

    /** The baseline JDK has no native Windows named-pipe HTTP transport. */
    public static LocalApiClient windowsNamedPipeUnsupported() {
        throw new UnsupportedOperationException(
                "JDK 21 has no native Windows named-pipe HTTP client; configure an explicit LocalAPI HTTP/token endpoint");
    }

    private static String query(PeerResolutionContext context) {
        StringBuilder query = new StringBuilder("addr=").append(url(whoisSourceAddress(context)))
                .append("&proto=tcp");
        if (context.serviceName() != null) query.append("&svc_name=").append(url(context.serviceName()));
        else if (context.destinationAddress() != null) {
            query.append("&dst_ip=").append(url(normalizeDestinationIp(context.destinationAddress())));
        }
        return query.toString();
    }

    private static byte[] request(PeerResolutionContext context, String token) {
        StringBuilder request = new StringBuilder("GET /localapi/v0/whois?").append(query(context))
                .append(" HTTP/1.1\r\nHost: local-tailscaled.sock\r\n")
                .append("Accept: application/json\r\nConnection: close\r\n");
        if (token != null && !token.isEmpty()) {
            String encoded = Base64.getEncoder().encodeToString(
                    (":" + token).getBytes(StandardCharsets.UTF_8));
            request.append("Authorization: Basic ").append(encoded).append("\r\n");
        }
        request.append("\r\n");
        return request.toString().getBytes(StandardCharsets.US_ASCII);
    }

    private static IoDeadline deadline(PeerResolutionContext context, Duration fallback)
            throws SocketTimeoutException {
        Duration budget = context.budgetNanos() == 0 ? fallback : context.remainingBudget();
        if (budget.isZero() || budget.isNegative()) throw new SocketTimeoutException("LocalAPI lookup timed out");
        return new IoDeadline(budget);
    }

    private static void connect(SocketChannel channel, SocketAddress address, Selector selector,
                                SelectionKey key, IoDeadline deadline)
            throws IOException, InterruptedException {
        if (channel.connect(address)) return;
        while (!channel.finishConnect()) await(selector, key, SelectionKey.OP_CONNECT, deadline);
    }

    private static void writeAll(SocketChannel channel, Selector selector, SelectionKey key,
                                 byte[] value, IoDeadline deadline)
            throws IOException, InterruptedException {
        ByteBuffer buffer = ByteBuffer.wrap(value);
        while (buffer.hasRemaining()) {
            if (channel.write(buffer) == 0) await(selector, key, SelectionKey.OP_WRITE, deadline);
        }
    }

    private static LocalApiResponse readHttp(SocketChannel channel, Selector selector,
                                             SelectionKey key, IoDeadline deadline)
            throws IOException, InterruptedException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        ByteBuffer buffer = ByteBuffer.allocate(4096);
        int boundary = -1;
        boolean eof = false;
        while (boundary < 0) {
            eof = !readSome(channel, selector, key, deadline, buffer, bytes);
            byte[] snapshot = bytes.toByteArray();
            boundary = headerBoundary(snapshot);
            if (boundary > MAX_HEADER_BYTES) {
                throw malformed("LocalAPI response headers are oversized");
            }
            if (boundary < 0 && snapshot.length > MAX_HEADER_BYTES) {
                throw malformed("LocalAPI response headers are oversized");
            }
            if (eof && boundary < 0) throw malformed("truncated LocalAPI HTTP headers");
        }

        byte[] response = bytes.toByteArray();
        ParsedHeaders parsed = parseHeaders(Arrays.copyOfRange(response, 0, boundary));
        int bodyStart = boundary + 4;
        int bufferedBody = response.length - bodyStart;

        String transferEncoding = parsed.headers().get("transfer-encoding");
        String rawLength = parsed.headers().get("content-length");
        boolean chunked = transferEncoding != null;
        if (chunked && !"chunked".equalsIgnoreCase(transferEncoding.trim())) {
            throw malformed("LocalAPI transfer-encoding is unsupported");
        }
        if (chunked && rawLength != null) throw malformed("conflicting LocalAPI response framing");
        Integer contentLength = null;
        if (rawLength != null) {
            if (!rawLength.matches("[0-9]+")) throw malformed("invalid LocalAPI content-length");
            try {
                long length = Long.parseLong(rawLength);
                if (length > MAX_RESPONSE_BYTES) throw malformed("LocalAPI response body is oversized");
                contentLength = (int) length;
            } catch (NumberFormatException e) {
                throw malformed("invalid LocalAPI content-length");
            }
            if (bufferedBody > contentLength) throw malformed("excess bytes after LocalAPI response body");
        }

        int wireLimit = chunked ? MAX_RESPONSE_BYTES * 4 : MAX_RESPONSE_BYTES;
        if (bufferedBody > wireLimit) throw malformed("LocalAPI response body is oversized");
        while (!eof && (chunked || contentLength == null || bytes.size() - bodyStart < contentLength)) {
            eof = !readSome(channel, selector, key, deadline, buffer, bytes);
            if (bytes.size() - bodyStart > wireLimit) {
                throw malformed("LocalAPI response body is oversized");
            }
        }
        int bodyLength = bytes.size() - bodyStart;
        if (contentLength != null && bodyLength != contentLength) {
            throw malformed("truncated LocalAPI response body");
        }
        byte[] complete = bytes.toByteArray();
        byte[] body = Arrays.copyOfRange(complete, bodyStart, complete.length);
        if (chunked) body = decodeChunked(body);
        return new LocalApiResponse(parsed.status(), body,
                parsed.headers().get("content-type"));
    }

    private static byte[] decodeChunked(byte[] wire) throws MalformedLocalApiResponseException {
        ByteArrayOutputStream decoded = new ByteArrayOutputStream();
        int offset = 0;
        while (true) {
            int lineEnd = crlf(wire, offset);
            if (lineEnd < 0) throw malformed("truncated LocalAPI chunk header");
            String line = new String(wire, offset, lineEnd - offset, StandardCharsets.US_ASCII);
            int extension = line.indexOf(';');
            String sizeText = (extension >= 0 ? line.substring(0, extension) : line).trim();
            if (!sizeText.matches("[0-9A-Fa-f]+")) throw malformed("invalid LocalAPI chunk size");
            long size;
            try { size = Long.parseLong(sizeText, 16); }
            catch (NumberFormatException e) { throw malformed("invalid LocalAPI chunk size"); }
            offset = lineEnd + 2;
            if (size == 0) {
                while (true) {
                    int trailerEnd = crlf(wire, offset);
                    if (trailerEnd < 0) throw malformed("truncated LocalAPI chunk trailer");
                    if (trailerEnd == offset) {
                        if (trailerEnd + 2 != wire.length) throw malformed("excess LocalAPI chunked bytes");
                        return decoded.toByteArray();
                    }
                    String trailer = new String(wire, offset, trailerEnd - offset, StandardCharsets.ISO_8859_1);
                    int colon = trailer.indexOf(':');
                    if (colon <= 0 || !HEADER_NAME.matcher(trailer.substring(0, colon)).matches()) {
                        throw malformed("invalid LocalAPI chunk trailer");
                    }
                    offset = trailerEnd + 2;
                }
            }
            if (size > MAX_RESPONSE_BYTES - decoded.size() || size > Integer.MAX_VALUE
                    || offset + (int) size + 2 > wire.length) {
                throw malformed("truncated or oversized LocalAPI chunk");
            }
            decoded.write(wire, offset, (int) size);
            offset += (int) size;
            if (wire[offset] != '\r' || wire[offset + 1] != '\n') {
                throw malformed("invalid LocalAPI chunk terminator");
            }
            offset += 2;
        }
    }

    private static int crlf(byte[] value, int start) {
        for (int index = start; index + 1 < value.length; index++) {
            if (value[index] == '\r' && value[index + 1] == '\n') return index;
        }
        return -1;
    }

    private static boolean readSome(SocketChannel channel, Selector selector, SelectionKey key,
                                    IoDeadline deadline, ByteBuffer buffer,
                                    ByteArrayOutputStream output)
            throws IOException, InterruptedException {
        while (true) {
            buffer.clear();
            int count = channel.read(buffer);
            if (count < 0) return false;
            if (count > 0) {
                output.write(buffer.array(), 0, count);
                return true;
            }
            await(selector, key, SelectionKey.OP_READ, deadline);
        }
    }

    private static void await(Selector selector, SelectionKey key, int operation,
                              IoDeadline deadline) throws IOException, InterruptedException {
        key.interestOps(operation);
        while (true) {
            if (Thread.interrupted()) throw new InterruptedException("LocalAPI lookup interrupted");
            int selected = selector.select(deadline.remainingMillis());
            if (Thread.interrupted()) throw new InterruptedException("LocalAPI lookup interrupted");
            if (selected > 0) {
                selector.selectedKeys().clear();
                return;
            }
        }
    }

    private static ParsedHeaders parseHeaders(byte[] encoded) throws MalformedLocalApiResponseException {
        String text = new String(encoded, StandardCharsets.ISO_8859_1);
        String[] lines = text.split("\r\n", -1);
        if (lines.length == 0 || !lines[0].matches("HTTP/1\\.[01] [0-9]{3}( .*)?")) {
            throw malformed("malformed LocalAPI HTTP status");
        }
        int status;
        try { status = Integer.parseInt(lines[0].substring(9, 12)); }
        catch (RuntimeException e) { throw malformed("malformed LocalAPI HTTP status"); }
        Map<String, String> headers = new LinkedHashMap<>();
        for (int index = 1; index < lines.length; index++) {
            String line = lines[index];
            if (line.isEmpty() || line.charAt(0) == ' ' || line.charAt(0) == '\t') {
                throw malformed("malformed LocalAPI HTTP header");
            }
            int colon = line.indexOf(':');
            if (colon <= 0 || !HEADER_NAME.matcher(line.substring(0, colon)).matches()) {
                throw malformed("malformed LocalAPI HTTP header");
            }
            String name = line.substring(0, colon).toLowerCase(Locale.ROOT);
            String value = line.substring(colon + 1).trim();
            if (value.codePoints().anyMatch(code -> (code <= 0x1f && code != '\t') || code == 0x7f)
                    || headers.putIfAbsent(name, value) != null) {
                throw malformed("duplicate or invalid LocalAPI HTTP header");
            }
        }
        return new ParsedHeaders(status, Map.copyOf(headers));
    }

    private static int headerBoundary(byte[] value) {
        for (int index = 0; index + 3 < value.length; index++) {
            if (value[index] == '\r' && value[index + 1] == '\n'
                    && value[index + 2] == '\r' && value[index + 3] == '\n') return index;
        }
        return -1;
    }

    private static List<String> strings(JsonNode node) {
        if (node.isMissingNode() || node.isNull()) return List.of();
        if (!node.isArray()) throw new IllegalArgumentException("tags must be an array");
        List<String> values = new ArrayList<>();
        node.forEach(value -> {
            if (!value.isTextual() || !TAILSCALE_TAG.matcher(value.textValue()).matches()
                    || containsControl(value.textValue())) {
                throw new IllegalArgumentException("tags must be well-formed tag: strings");
            }
            JsonValues.requireWellFormed(value.textValue(), "tag");
            values.add(value.textValue());
        });
        return List.copyOf(values);
    }

    private static Map<String, Object> objectOfArrays(JsonNode node) {
        if (node.isMissingNode() || node.isNull()) return Map.of();
        if (!node.isObject()) throw new IllegalArgumentException("CapMap must be an object");
        Map<String, Object> result = new LinkedHashMap<>();
        node.fields().forEachRemaining(field -> {
            if (!field.getValue().isArray()) throw new IllegalArgumentException("CapMap values must be arrays");
            result.put(field.getKey(), JSON.convertValue(field.getValue(), Object.class));
        });
        return result;
    }

    private static void requireJsonValueLimit(JsonNode root) {
        Deque<JsonNode> pending = new ArrayDeque<>();
        pending.add(root);
        int count = 0;
        while (!pending.isEmpty()) {
            JsonNode node = pending.removeFirst();
            if (++count > MAX_JSON_VALUES) throw new IllegalArgumentException("LocalAPI JSON has too many values");
            if (node.isContainerNode()) node.elements().forEachRemaining(pending::addLast);
        }
    }

    private static String text(JsonNode object, String name) {
        JsonNode value = object.get(name);
        return value != null && value.isTextual() && !value.textValue().isEmpty() ? value.textValue() : null;
    }
    private static String positiveNumericId(JsonNode value) {
        if (value == null || !value.isIntegralNumber() || !value.canConvertToLong()
                || value.longValue() <= 0) return null;
        return value.asText();
    }

    private static String whoisSourceAddress(PeerResolutionContext context) {
        if (context.assertedPeer() != null) return context.assertedPeer();
        if (context.sourceEndpoint() != null) return context.sourceEndpoint();
        return context.immediatePeer();
    }

    private static String normalizedSourceIp(PeerResolutionContext context) {
        try { return normalizeDestinationIp(whoisSourceAddress(context)); }
        catch (IllegalArgumentException ignored) { return null; }
    }

    private static String normalizedProxyIp(PeerResolutionContext context) {
        if (context.assertedPeer() == null) return null;
        try { return normalizeDestinationIp(context.immediatePeer()); }
        catch (IllegalArgumentException ignored) { return null; }
    }

    private static String normalizeDestinationIp(String endpoint) {
        String value = endpoint == null ? null : endpoint.trim();
        if (value == null || value.isEmpty() || containsControl(value)) {
            throw new IllegalArgumentException("destination IP is invalid");
        }
        if (value.charAt(0) == '[') {
            int close = value.indexOf(']');
            if (close < 2 || (close + 1 < value.length()
                    && !value.substring(close + 1).matches(":[0-9]+"))) {
                throw new IllegalArgumentException("destination IP is invalid");
            }
            value = value.substring(1, close);
        } else {
            int firstColon = value.indexOf(':');
            if (firstColon >= 0 && firstColon == value.lastIndexOf(':')
                    && value.substring(firstColon + 1).matches("[0-9]+")) {
                value = value.substring(0, firstColon);
            }
        }
        if (value.indexOf(':') >= 0) {
            if (value.indexOf('%') >= 0) throw new IllegalArgumentException("scoped destination IPv6 is unsupported");
            try {
                if (!(InetAddress.getByName(value) instanceof java.net.Inet6Address)) {
                    throw new IllegalArgumentException("destination IP is invalid");
                }
            } catch (IOException e) {
                throw new IllegalArgumentException("destination IP is invalid", e);
            }
            return value;
        }
        String[] parts = value.split("\\.", -1);
        if (parts.length != 4) throw new IllegalArgumentException("destination must be an IP literal");
        for (String part : parts) {
            if (!part.matches("0|[1-9][0-9]{0,2}")) throw new IllegalArgumentException("destination IP is invalid");
            if (Integer.parseInt(part) > 255) throw new IllegalArgumentException("destination IP is invalid");
        }
        return value;
    }
    private static void put(Map<String, Object> target, String name, String value) { if (value != null) target.put(name, value); }
    private static String url(String value) { return URLEncoder.encode(value, StandardCharsets.UTF_8); }
    private static PeerIdentityResult result(PeerIdentityStatus status) { return new PeerIdentityResult(PROVIDER, status); }
    private static boolean containsControl(String value) {
        return value.codePoints().anyMatch(code -> code <= 0x1f || code == 0x7f);
    }
    private static void validateToken(String token) {
        if (token != null && containsControl(token)) throw new IllegalArgumentException("LocalAPI token contains control characters");
    }

    private static MalformedLocalApiResponseException malformed(String message) {
        return new MalformedLocalApiResponseException(message);
    }

    private record ParsedHeaders(int status, Map<String, String> headers) {}

    private static final class IoDeadline {
        private final long startedNanos;
        private final long budgetNanos;

        IoDeadline(Duration duration) {
            startedNanos = System.nanoTime();
            long value;
            try { value = duration.toNanos(); }
            catch (ArithmeticException e) { value = Long.MAX_VALUE; }
            budgetNanos = value;
        }

        long remainingMillis() throws SocketTimeoutException {
            long remaining = budgetNanos - (System.nanoTime() - startedNanos);
            if (remaining <= 0) throw new SocketTimeoutException("LocalAPI lookup timed out");
            long millis = remaining / 1_000_000L + (remaining % 1_000_000L == 0 ? 0 : 1);
            return Math.min(Integer.MAX_VALUE, Math.max(1L, millis));
        }
    }

    private static final class MalformedLocalApiResponseException extends IOException {
        MalformedLocalApiResponseException(String message) { super(message); }
    }

    public static final class LocalApiPermissionException extends IOException {
        public LocalApiPermissionException(String message) { super(message); }
    }

}
