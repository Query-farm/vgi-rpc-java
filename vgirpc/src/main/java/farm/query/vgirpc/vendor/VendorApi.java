// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.vendor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import farm.query.vgirpc.external.LocationResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

/**
 * HTTP transport, retries, and the vendor's request/response shapes.
 *
 * <p>Every JSON key the vendor defines appears only in this class, so a change
 * to the vendor's wire format is a change here and nowhere else. Vendor API
 * requests and storage (signed URL) requests use two separate bare
 * {@link HttpClient}s with no authenticator, cookie handler or redirects, so a
 * signed-URL request carries exactly the headers the grant names and never the
 * bearer token.</p>
 */
final class VendorApi implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(VendorApi.class);
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final double MAX_BACKOFF_SECONDS = 5.0;

    static final String PUT_PATH = "/v1/blobs/put";
    static final String GET_PATH = "/v1/blobs/get";
    static final String DELETE_PATH = "/v1/blobs/delete";
    static final String DELETE_SCOPE_PATH = "/v1/blobs/delete_scope";
    static final String INFO_PATH = "/v1/info";
    static final int MAX_DELETE_IDS = 1000;

    final String baseUrl;
    final boolean loopbackHttp;
    private final String token;
    private final Duration timeout;
    private final int maxAttempts;
    private final double backoffSeconds;
    private final HttpClient api;
    private final HttpClient storage;

    VendorApi(String url, String token, Duration timeout, int maxAttempts, double backoffSeconds) {
        URI parsed;
        try {
            parsed = URI.create(url);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Blob vendor URL must be an absolute https URL: "
                    + LocationResolver.redactUrl(url));
        }
        String scheme = parsed.getScheme() == null ? "" : parsed.getScheme().toLowerCase(Locale.ROOT);
        if (!(scheme.equals("https") || scheme.equals("http")) || parsed.getHost() == null) {
            throw new IllegalArgumentException("Blob vendor URL must be an absolute https URL: "
                    + LocationResolver.redactUrl(url));
        }
        this.loopbackHttp = scheme.equals("http");
        if (loopbackHttp && !isLoopbackHost(parsed.getHost())) {
            // The bearer token would cross the network in plain text.
            throw new IllegalArgumentException(
                    "Blob vendor URL must use https (http is allowed only for loopback): "
                            + LocationResolver.redactUrl(url));
        }
        if (token == null || token.isEmpty()) throw new IllegalArgumentException("Blob vendor token is empty");
        if (maxAttempts < 1) throw new IllegalArgumentException("maxAttempts must be >= 1");
        this.baseUrl = url.endsWith("/") ? url.replaceAll("/+$", "") : url;
        this.token = token;
        this.timeout = timeout;
        this.maxAttempts = maxAttempts;
        this.backoffSeconds = backoffSeconds;
        this.api = newBareClient(timeout);
        this.storage = newBareClient(timeout);
    }

    private static HttpClient newBareClient(Duration timeout) {
        // No redirects: a redirect could carry the bearer token or a signed URL
        // to a host nobody vetted. No authenticator, no cookie handler.
        return HttpClient.newBuilder()
                .connectTimeout(timeout)
                .followRedirects(HttpClient.Redirect.NEVER)
                .version(HttpClient.Version.HTTP_1_1)
                .build();
    }

    static boolean isLoopbackHost(String host) {
        if (host == null) return false;
        String h = host.startsWith("[") && host.endsWith("]") ? host.substring(1, host.length() - 1) : host;
        if (h.equalsIgnoreCase("localhost")) return true;
        // Only literal addresses: never resolve a name here.
        if (!h.matches("[0-9.]+") && !h.contains(":")) return false;
        try {
            return InetAddress.getByName(h).isLoopbackAddress();
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public void close() {
        // java.net.http.HttpClient gained close() only in JDK 21 as AutoCloseable;
        // shutdownNow releases its selector thread promptly.
        api.shutdownNow();
        storage.shutdownNow();
    }

    // -- transport --------------------------------------------------------

    private record Response(int status, byte[] body, java.net.http.HttpHeaders headers) {}

    /**
     * Send one request, retrying transport errors, 429 and 5xx with backoff.
     * The request carries exactly {@code headers}.
     */
    private Response send(String method, String url, String op, Map<String, String> headers, byte[] content,
                          boolean storageHost, long maxBodyBytes) {
        HttpClient client = storageHost ? storage : api;
        String lastError = "";
        Integer status = null;
        for (int attempt = 0; attempt < maxAttempts; attempt++) {
            if (attempt > 0) {
                double delay = Math.min(backoffSeconds * Math.pow(2, attempt - 1), MAX_BACKOFF_SECONDS);
                delay *= 0.5 + ThreadLocalRandom.current().nextDouble() / 2;
                try {
                    Thread.sleep((long) (delay * 1000));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new VendorError("Blob vendor " + op + " interrupted: " + LocationResolver.redactUrl(url));
                }
            }
            HttpRequest.Builder rb = HttpRequest.newBuilder(URI.create(url)).timeout(timeout);
            headers.forEach(rb::header);
            rb.method(method, content == null
                    ? HttpRequest.BodyPublishers.noBody()
                    : HttpRequest.BodyPublishers.ofByteArray(content));
            HttpResponse<InputStream> resp;
            try {
                resp = client.send(rb.build(), HttpResponse.BodyHandlers.ofInputStream());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new VendorError("Blob vendor " + op + " interrupted: " + LocationResolver.redactUrl(url));
            } catch (IOException e) {
                // Never e.getMessage(): JDK messages can embed the full URL.
                status = null;
                lastError = e.getClass().getSimpleName();
                LOG.debug("Blob vendor {} transport error (attempt {}/{}): {} {}", op, attempt + 1, maxAttempts,
                        lastError, LocationResolver.redactUrl(url));
                continue;
            }
            int code = resp.statusCode();
            if (code == 429 || code >= 500) {
                status = code;
                lastError = "HTTP " + code;
                LOG.debug("Blob vendor {} got HTTP {} (attempt {}/{}): {}", op, code, attempt + 1, maxAttempts,
                        LocationResolver.redactUrl(url));
                closeQuietly(resp.body());
                continue;
            }
            try (InputStream in = resp.body()) {
                boolean ok = code / 100 == 2;
                byte[] body = readCapped(in, ok ? maxBodyBytes : 64 * 1024, !ok, op, url);
                return new Response(code, body, resp.headers());
            } catch (IOException e) {
                status = null;
                lastError = e.getClass().getSimpleName();
                LOG.debug("Blob vendor {} read error (attempt {}/{}): {} {}", op, attempt + 1, maxAttempts,
                        lastError, LocationResolver.redactUrl(url));
            }
        }
        throw new VendorError("Blob vendor " + op + " failed after " + maxAttempts + " attempts (" + lastError
                + "): " + LocationResolver.redactUrl(url), status, null);
    }

    private static byte[] readCapped(InputStream in, long cap, boolean truncate, String op, String url)
            throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] chunk = new byte[64 * 1024];
        long total = 0;
        int n;
        while ((n = in.read(chunk)) >= 0) {
            total += n;
            if (total > cap) {
                // Truncate error bodies quietly; a 2xx over the cap is a refusal.
                if (truncate) break;
                throw new VendorError("Blob vendor " + op + " response exceeds " + cap + " bytes: "
                        + LocationResolver.redactUrl(url));
            }
            out.write(chunk, 0, n);
        }
        return out.toByteArray();
    }

    private static void closeQuietly(InputStream in) {
        try {
            in.close();
        } catch (IOException ignored) {
            // nothing to do
        }
    }

    private JsonNode postJson(String path, String op, ObjectNode body) {
        try {
            return call("POST", path, op, JSON.writeValueAsBytes(body));
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new VendorError("Blob vendor " + op + " request could not be encoded");
        }
    }

    private JsonNode call(String method, String path, String op, byte[] content) {
        String url = baseUrl + path;
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("Authorization", "Bearer " + token);
        headers.put("Accept", "application/json");
        if (content != null) headers.put("Content-Type", "application/json");
        Response resp = send(method, url, op, headers, content, false, 16L << 20);
        if (resp.status() / 100 != 2) {
            String[] detail = errorDetail(resp.body());
            throw new VendorError("Blob vendor " + op + " failed: HTTP " + resp.status() + detail[1],
                    resp.status(), detail[0]);
        }
        if (resp.body().length == 0) return JSON.createObjectNode();
        JsonNode parsed;
        try {
            parsed = JSON.readTree(resp.body());
        } catch (IOException e) {
            throw new VendorError("Blob vendor " + op + " returned non-JSON", resp.status(), null);
        }
        if (parsed == null || !parsed.isObject()) {
            throw new VendorError("Blob vendor " + op + " returned a non-object JSON body", resp.status(), null);
        }
        return parsed;
    }

    /** {@code [code, ": code: message"]} out of a vendor {@code {error, message}} body. */
    private static String[] errorDetail(byte[] body) {
        try {
            JsonNode parsed = JSON.readTree(body);
            if (parsed != null && parsed.isObject()) {
                JsonNode c = parsed.get("error");
                JsonNode m = parsed.get("message");
                String code = c != null && c.isTextual() && !c.asText().isEmpty()
                        ? truncate(c.asText(), 64) : null;
                String message = m != null && m.isTextual() ? truncate(m.asText(), 200) : null;
                StringBuilder sb = new StringBuilder();
                for (String p : new String[] {code, message}) {
                    if (p == null || p.isEmpty()) continue;
                    sb.append(sb.length() == 0 ? ": " : ": ").append(p);
                }
                return new String[] {code, sb.toString()};
            }
        } catch (IOException | RuntimeException ignored) {
            // not JSON
        }
        return new String[] {null, ""};
    }

    private static String truncate(String s, int max) {
        return s.length() <= max ? s : s.substring(0, max);
    }

    /** The {@code <Code>} of an S3/R2 XML error body, if any. */
    static String s3ErrorCode(byte[] body) {
        String text = new String(body, StandardCharsets.ISO_8859_1);
        int start = text.indexOf("<Code>");
        int end = start < 0 ? -1 : text.indexOf("</Code>", start);
        if (start < 0 || end < 0) return null;
        String code = text.substring(start + 6, end);
        return !code.isEmpty() && code.length() <= 64 && code.chars().allMatch(Character::isLetterOrDigit)
                ? code : null;
    }

    // -- field helpers ----------------------------------------------------

    private static JsonNode field(JsonNode obj, String key, String op) {
        JsonNode v = obj.get(key);
        if (v == null) throw new VendorError("Blob vendor " + op + " response is missing '" + key + "'");
        return v;
    }

    private static String strField(JsonNode obj, String key, String op) {
        JsonNode v = field(obj, key, op);
        if (!v.isTextual() || v.asText().isEmpty()) {
            throw new VendorError("Blob vendor " + op + " response has a malformed '" + key + "'");
        }
        return v.asText();
    }

    private static String optStrField(JsonNode obj, String key, String op) {
        JsonNode v = obj.get(key);
        if (v == null || v.isNull() || (v.isTextual() && v.asText().isEmpty())) return null;
        if (!v.isTextual()) throw new VendorError("Blob vendor " + op + " response has a malformed '" + key + "'");
        return v.asText();
    }

    private static long longField(JsonNode obj, String key, String op) {
        JsonNode v = field(obj, key, op);
        if (!v.isIntegralNumber() || v.asLong() < 0) {
            throw new VendorError("Blob vendor " + op + " response has a malformed '" + key + "'");
        }
        return v.asLong();
    }

    private static Map<String, Long> longMap(JsonNode obj, String key) {
        Map<String, Long> out = new LinkedHashMap<>();
        JsonNode v = obj.get(key);
        if (v == null || !v.isObject()) return out;
        v.fields().forEachRemaining(e -> {
            if (e.getValue().isIntegralNumber()) out.put(e.getKey(), e.getValue().asLong());
        });
        return out;
    }

    private static List<String> hostsField(JsonNode obj, String key) {
        JsonNode v = obj.get(key);
        List<String> out = new ArrayList<>();
        if (v == null || v.isNull()) return out;
        if (!v.isArray()) throw new VendorError("Blob vendor info response has a malformed '" + key + "'");
        for (JsonNode h : v) {
            if (!h.isTextual() || h.asText().isEmpty()) {
                throw new VendorError("Blob vendor info response has a malformed '" + key + "'");
            }
            out.add(h.asText());
        }
        return out;
    }

    /** Parse {@code expires_at} as ISO 8601 text or epoch seconds/milliseconds. */
    static Instant parseExpires(JsonNode value) {
        if (value == null || value.isBoolean() || value.isNull()) {
            throw new VendorError("Blob vendor returned a malformed expires_at");
        }
        if (value.isNumber()) {
            double d = value.asDouble();
            double seconds = d > 1e11 ? d / 1000.0 : d;
            return Instant.ofEpochMilli((long) (seconds * 1000));
        }
        if (value.isTextual()) {
            String text = value.asText();
            try {
                return OffsetDateTime.parse(text.replace("Z", "+00:00")).toInstant();
            } catch (DateTimeParseException e) {
                try {
                    return java.time.LocalDateTime.parse(text).toInstant(java.time.ZoneOffset.UTC);
                } catch (DateTimeParseException e2) {
                    throw new VendorError("Blob vendor returned a malformed expires_at");
                }
            }
        }
        throw new VendorError("Blob vendor returned a malformed expires_at");
    }

    // -- wire mapping -----------------------------------------------------

    PutGrant put(long size, String sha256B64, String purpose, BlobScope scope, String contentEncoding,
                 String idempotencyKey) {
        ObjectNode body = JSON.createObjectNode();
        body.put("size", size);
        body.put("sha256_b64", sha256B64);
        body.put("purpose", purpose);
        if (scope != null) {
            ObjectNode s = body.putObject("scope");
            s.put("shard", scope.shard());
            s.put("execution_id", scope.executionId());
        }
        if (contentEncoding != null) body.put("content_encoding", contentEncoding);
        if (idempotencyKey != null) body.put("idempotency_key", idempotencyKey);
        JsonNode resp = postJson(PUT_PATH, "put", body);
        JsonNode method = resp.get("upload_method");
        if (method != null && !method.isNull() && !"PUT".equals(method.asText())) {
            throw new VendorError("Blob vendor put asked for unsupported upload_method '" + method.asText() + "'");
        }
        JsonNode rawHeaders = field(resp, "required_headers", "put");
        if (!rawHeaders.isObject()) {
            throw new VendorError("Blob vendor put response has malformed 'required_headers'");
        }
        Map<String, String> required = new LinkedHashMap<>();
        rawHeaders.fields().forEachRemaining(e -> {
            if (!e.getValue().isTextual()) {
                throw new VendorError("Blob vendor put response has malformed 'required_headers'");
            }
            required.put(e.getKey(), e.getValue().asText());
        });
        JsonNode dlExp = resp.get("download_expires_at");
        return new PutGrant(
                strField(resp, "object_id", "put"),
                strField(resp, "upload_url", "put"),
                required,
                optStrField(resp, "download_url", "put"),
                parseExpires(field(resp, "expires_at", "put")),
                optStrField(resp, "region", "put"),
                dlExp == null || dlExp.isNull() ? null : parseExpires(dlExp));
    }

    BlobLocation get(String objectId) {
        ObjectNode body = JSON.createObjectNode();
        body.put("object_id", objectId);
        JsonNode resp = postJson(GET_PATH, "get", body);
        return new BlobLocation(
                strField(resp, "download_url", "get"),
                longField(resp, "size", "get"),
                strField(resp, "sha256_b64", "get"),
                optStrField(resp, "content_encoding", "get"),
                parseExpires(field(resp, "expires_at", "get")));
    }

    long delete(List<String> objectIds) {
        ObjectNode body = JSON.createObjectNode();
        ArrayNode ids = body.putArray("object_ids");
        objectIds.forEach(ids::add);
        JsonNode resp = postJson(DELETE_PATH, "delete", body);
        JsonNode d = resp.get("deleted");
        return d != null && d.isIntegralNumber() ? d.asLong() : 0;
    }

    long deleteScope(BlobScope scope) {
        ObjectNode body = JSON.createObjectNode();
        body.put("shard", scope.shard());
        body.put("execution_id", scope.executionId());
        JsonNode resp = postJson(DELETE_SCOPE_PATH, "delete_scope", body);
        JsonNode d = resp.get("deleted");
        return d != null && d.isIntegralNumber() ? d.asLong() : 0;
    }

    VendorInfo info() {
        JsonNode resp = call("GET", INFO_PATH, "info", null);
        JsonNode region = resp.get("region");
        JsonNode tenant = resp.get("tenant");
        return new VendorInfo(
                region != null && region.isTextual() ? region.asText() : "",
                hostsField(resp, "upload_hosts"),
                hostsField(resp, "download_hosts"),
                tenant != null && tenant.isTextual() ? tenant.asText() : "",
                optStrField(resp, "jurisdiction", "info"),
                hostsField(resp, "url_prefixes"),
                longMap(resp, "limits"),
                longMap(resp, "ttl_seconds"));
    }

    // -- signed URLs --------------------------------------------------------

    /** PUT {@code data} to a granted URL with exactly its required headers. */
    void putObject(PutGrant grant, byte[] data, String contentEncoding) {
        Map<String, String> headers = new LinkedHashMap<>();
        boolean hasEncoding = false;
        for (Map.Entry<String, String> e : grant.requiredHeaders().entrySet()) {
            String lower = e.getKey().toLowerCase(Locale.ROOT);
            if (lower.equals("content-length")) {
                // The JDK client sets Content-Length itself (it is a restricted
                // header); check the grant asks for exactly this body.
                if (!e.getValue().equals(Long.toString(data.length))) {
                    throw new VendorError("Blob vendor granted Content-Length " + e.getValue() + " for a "
                            + data.length + "-byte object");
                }
                continue;
            }
            if (lower.equals("host")) continue;
            if (lower.equals("content-encoding")) hasEncoding = true;
            headers.put(e.getKey(), e.getValue());
        }
        if (contentEncoding != null && !hasEncoding) headers.put("Content-Encoding", contentEncoding);
        Response resp = send("PUT", grant.uploadUrl(), "upload", headers, data, true, 64 * 1024);
        if (resp.status() / 100 != 2) {
            String code = s3ErrorCode(resp.body());
            throw new VendorError("Signed upload failed: HTTP " + resp.status() + (code != null ? " " + code : "")
                    + ": " + LocationResolver.redactUrl(grant.uploadUrl()), resp.status(), code);
        }
    }

    /**
     * GET a signed download URL on the storage client, after {@code validator}
     * accepts it. Redirects are refused. A non-2xx answer is a
     * {@link VendorError} carrying the status and the storage's error code.
     */
    byte[] fetch(String url, Consumer<URI> validator, long maxBytes, String op) {
        URI uri;
        try {
            uri = URI.create(url);
        } catch (IllegalArgumentException e) {
            throw new VendorError("Blob " + op + " download rejected: URL is malformed");
        }
        try {
            validator.accept(uri);
        } catch (IllegalArgumentException e) {
            throw new VendorError("Blob " + op + " download rejected: " + e.getMessage());
        }
        Response resp = send("GET", url, "download", Map.of("Accept-Encoding", "identity"), null, true, maxBytes);
        int status = resp.status();
        if (status / 100 != 2) {
            String code = s3ErrorCode(resp.body());
            throw new VendorError("Blob " + op + " download failed: HTTP " + status + (code != null ? " " + code : "")
                    + ": " + LocationResolver.redactUrl(url), status, code);
        }
        return resp.body();
    }
}
