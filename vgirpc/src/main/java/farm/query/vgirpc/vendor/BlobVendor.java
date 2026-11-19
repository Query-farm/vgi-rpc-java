// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.vendor;

import farm.query.vgirpc.external.ExternalLocationConfig;
import farm.query.vgirpc.external.LocationResolver;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * URL pinning and environment-driven configuration for the blob vendor.
 *
 * <h2>Environment variables</h2>
 * <dl>
 *   <dt>{@value #ENV_URL}</dt>
 *   <dd>Base URL of the vendor, e.g. {@code https://blobs.queryfarm.cloud}.
 *       When unset, the {@code *FromEnv} helpers return {@code null}.</dd>
 *   <dt>{@value #ENV_TOKEN}</dt>
 *   <dd>Bearer token for the vendor. Falls back to {@value #ENV_TOKEN_FALLBACK}.</dd>
 *   <dt>{@value #ENV_THRESHOLD}</dt>
 *   <dd>Result-batch size at or above which the server externalizes (default 1 MiB).</dd>
 *   <dt>{@value #ENV_COMPRESSION}</dt>
 *   <dd>{@code none} (default) or {@code zstd}, optionally with a level
 *       ({@code zstd:3}).</dd>
 *   <dt>{@value #ENV_MAX_UPLOAD_BYTES}</dt>
 *   <dd>Optional size advertised to clients for vendor upload URLs.</dd>
 *   <dt>{@value #ENV_MAX_REQUEST_BYTES}</dt>
 *   <dd>Optional request-body cap, advertised and enforced; clients send larger
 *       requests through vendor upload URLs.</dd>
 * </dl>
 *
 * <p>Every vendor request carries {@code Authorization: Bearer <token>}. The
 * token and signed URLs are never logged: diagnostics keep only scheme, host,
 * port and path.</p>
 */
public final class BlobVendor {

    private BlobVendor() {}

    /** Vendor base URL. */
    public static final String ENV_URL = "VGI_BLOB_VENDOR_URL";
    /** Vendor bearer token. */
    public static final String ENV_TOKEN = "VGI_BLOB_VENDOR_TOKEN";
    /** Token fallback (the vendor shares the storage service's keys). */
    public static final String ENV_TOKEN_FALLBACK = "VGI_CF_DO_TOKEN";
    /** Result externalization threshold in bytes. */
    public static final String ENV_THRESHOLD = "VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES";
    /** Upload compression for externalized results. */
    public static final String ENV_COMPRESSION = "VGI_RPC_EXTERNAL_COMPRESSION";
    /** Advertised client-upload size. */
    public static final String ENV_MAX_UPLOAD_BYTES = "VGI_RPC_MAX_UPLOAD_BYTES";
    /** Advertised and enforced request-body cap. */
    public static final String ENV_MAX_REQUEST_BYTES = "VGI_RPC_MAX_REQUEST_BYTES";

    static final long DEFAULT_THRESHOLD = 1_048_576L;
    static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);
    static final double DEFAULT_INFO_TTL_SECONDS = 600.0;

    // ---------------------------------------------------------------------
    // URL pinning
    // ---------------------------------------------------------------------

    private record HostKey(String host, int port) {}

    private static int defaultPort(String scheme) {
        return "https".equals(scheme) ? 443 : 80;
    }

    private static HostKey hostKey(String host, int port, String scheme) {
        String h = host.toLowerCase(Locale.ROOT);
        if (h.startsWith("[") && h.endsWith("]")) h = h.substring(1, h.length() - 1);
        return new HostKey(h, port >= 0 ? port : defaultPort(scheme));
    }

    /**
     * Build a {@code urlValidator} that accepts only URLs on {@code hosts}.
     *
     * <p>A URL passes when its scheme is {@code https} (or {@code http} with
     * {@code allowHttp}), it carries no userinfo, and its host and port are
     * exactly one of {@code hosts}. An entry is {@code host} or
     * {@code host:port} ({@code [v6]:port} for IPv6); an entry without a port
     * matches the scheme's default port only. Subdomains do not match. With
     * {@code prefixes}, the URL (without its query) must also start with one of
     * them, and its path may not contain a {@code ..} segment.</p>
     *
     * @param hosts     allowed hosts
     * @param allowHttp also accept {@code http} URLs; only for loopback test vendors
     * @param prefixes  optional URL prefixes the URL must start with (may be empty)
     * @return a validator throwing {@link IllegalArgumentException} for a disallowed URL
     * @throws IllegalArgumentException if {@code hosts} is empty or an entry will not parse
     */
    public static Consumer<URI> pinnedHostsValidator(Iterable<String> hosts, boolean allowHttp,
                                                     Iterable<String> prefixes) {
        List<String> schemes = allowHttp ? List.of("https", "http") : List.of("https");
        Set<HostKey> allowed = new HashSet<>();
        for (String raw : hosts) {
            String entry = raw == null ? "" : raw.strip();
            URI parts;
            try {
                parts = URI.create("//" + entry);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Invalid pinned host entry: '" + raw + "'");
            }
            if (entry.isEmpty() || parts.getHost() == null || !parts.getRawPath().isEmpty()
                    || parts.getRawQuery() != null || parts.getRawUserInfo() != null) {
                throw new IllegalArgumentException("Invalid pinned host entry: '" + raw + "'");
            }
            if (parts.getPort() < 0) {
                for (String s : schemes) allowed.add(hostKey(parts.getHost(), -1, s));
            } else {
                allowed.add(hostKey(parts.getHost(), parts.getPort(), "https"));
            }
        }
        if (allowed.isEmpty()) throw new IllegalArgumentException("pinnedHostsValidator needs at least one host");
        Set<HostKey> frozen = Set.copyOf(allowed);
        List<String> urlPrefixes = new ArrayList<>();
        if (prefixes != null) for (String p : prefixes) if (p != null && !p.isEmpty()) urlPrefixes.add(p);
        List<String> frozenPrefixes = List.copyOf(urlPrefixes);
        String schemeText = String.join(" or ", schemes);

        return uri -> {
            String scheme = uri.getScheme() == null ? "" : uri.getScheme().toLowerCase(Locale.ROOT);
            if (!schemes.contains(scheme)) {
                throw new IllegalArgumentException("URL scheme '" + scheme + "' not allowed (only " + schemeText + ")");
            }
            if (uri.getRawUserInfo() != null) throw new IllegalArgumentException("URL userinfo is not allowed");
            if (uri.getHost() == null || !frozen.contains(hostKey(uri.getHost(), uri.getPort(), scheme))) {
                throw new IllegalArgumentException("URL host is not a pinned storage host: "
                        + LocationResolver.redactUrl(uri.toString()));
            }
            if (!frozenPrefixes.isEmpty()) {
                String path = uri.getRawPath() == null ? "" : uri.getRawPath();
                String bare = scheme + "://" + uri.getRawAuthority() + path;
                boolean dotdot = List.of(path.split("/", -1)).contains("..");
                if (dotdot || frozenPrefixes.stream().noneMatch(bare::startsWith)) {
                    throw new IllegalArgumentException("URL is outside the pinned storage prefixes: "
                            + LocationResolver.redactUrl(uri.toString()));
                }
            }
        };
    }

    /**
     * Build a {@code urlValidator} pinned to the vendor's storage hosts.
     *
     * <p>The hosts come from the vendor's {@code GET /v1/info}, fetched on first
     * use and cached for {@code ttlSeconds}; a failed refresh keeps the last
     * good list. Only {@code https} URLs pass ({@code http} only when the vendor
     * itself is a loopback {@code http} URL, i.e. a local test vendor).</p>
     *
     * @param url        vendor base URL
     * @param token      bearer token
     * @param ttlSeconds cache lifetime of the host list
     * @return a validator throwing {@link IllegalArgumentException} for a URL off the vendor's hosts
     */
    public static Consumer<URI> vendorUrlValidator(String url, String token, double ttlSeconds) {
        return new VendorHostsValidator(new VendorApi(url, token, DEFAULT_TIMEOUT, 3, 0.25), ttlSeconds);
    }

    /**
     * {@link #vendorUrlValidator(String, String, double)} with the default
     * 600-second cache.
     *
     * @param url   vendor base URL
     * @param token bearer token
     * @return the validator
     */
    public static Consumer<URI> vendorUrlValidator(String url, String token) {
        return vendorUrlValidator(url, token, DEFAULT_INFO_TTL_SECONDS);
    }

    // ---------------------------------------------------------------------
    // Environment
    // ---------------------------------------------------------------------

    static Map<String, String> resolveEnv(Map<String, String> env) {
        return env == null ? System.getenv() : env;
    }

    static String envOrEmpty(Map<String, String> env, String name) {
        String v = env.get(name);
        return v == null ? "" : v.strip();
    }

    /** {@code [url, token]} from the environment, or an error naming what is missing. */
    static String[] envUrlAndToken(Map<String, String> env) {
        String url = envOrEmpty(env, ENV_URL);
        if (url.isEmpty()) throw new IllegalArgumentException(ENV_URL + " is not set");
        String token = envOrEmpty(env, ENV_TOKEN);
        if (token.isEmpty()) token = envOrEmpty(env, ENV_TOKEN_FALLBACK);
        if (token.isEmpty()) {
            throw new IllegalArgumentException(ENV_URL + " is set but neither " + ENV_TOKEN + " nor "
                    + ENV_TOKEN_FALLBACK + " is");
        }
        return new String[] {url, token};
    }

    /**
     * Parse a positive integer environment variable.
     *
     * @param env          environment mapping ({@code null} for the process environment)
     * @param name         variable name
     * @param defaultValue value when unset (may be {@code null})
     * @return the value, or {@code defaultValue} when unset
     * @throws IllegalArgumentException if the value is not a positive integer
     */
    public static Long positiveLongFromEnv(Map<String, String> env, String name, Long defaultValue) {
        String raw = envOrEmpty(resolveEnv(env), name);
        if (raw.isEmpty()) return defaultValue;
        long value;
        try {
            value = Long.parseLong(raw);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(name + " must be an integer, got '" + raw + "'");
        }
        if (value <= 0) throw new IllegalArgumentException(name + " must be positive, got " + value);
        return value;
    }

    static ExternalLocationConfig.Compression parseCompression(String raw) {
        String text = raw == null ? "" : raw.strip().toLowerCase(Locale.ROOT);
        if (text.isEmpty() || text.equals("none") || text.equals("off") || text.equals("identity")) return null;
        int colon = text.indexOf(':');
        String name = colon < 0 ? text : text.substring(0, colon);
        String level = colon < 0 ? "" : text.substring(colon + 1);
        if (!name.equals("zstd")) {
            // The Java externalizer compresses with zstd only.
            throw new IllegalArgumentException(ENV_COMPRESSION
                    + " must be none or zstd (optionally ':<level>'), got '" + raw + "'");
        }
        if (level.isEmpty()) return ExternalLocationConfig.Compression.zstd();
        try {
            return ExternalLocationConfig.Compression.zstd(Integer.parseInt(level));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(ENV_COMPRESSION + " level must be an integer, got '" + raw + "'");
        }
    }

    /**
     * Server external-location config from the blob-vendor environment.
     *
     * <p>Uses a {@link VendorStorage} for uploads, pins inbound pointer URLs to
     * the vendor's hosts, and reads {@value #ENV_THRESHOLD} (default 1 MiB) and
     * {@value #ENV_COMPRESSION} (default none). Pass the config's
     * {@link ExternalLocationConfig#storage()} (a {@link VendorStorage}) as the
     * HTTP server's upload-URL provider to also vend client upload URLs.</p>
     *
     * @param env environment mapping ({@code null} for the process environment)
     * @return the config, or {@code null} when {@value #ENV_URL} is unset
     * @throws IllegalArgumentException if the vendor URL is set without a token,
     *     or a variable will not parse
     */
    public static ExternalLocationConfig externalConfigFromEnv(Map<String, String> env) {
        Map<String, String> environ = resolveEnv(env);
        if (envOrEmpty(environ, ENV_URL).isEmpty()) return null;
        VendorStorage storage = VendorStorage.fromEnv("rpc", environ);
        long threshold = positiveLongFromEnv(environ, ENV_THRESHOLD, DEFAULT_THRESHOLD);
        return ExternalLocationConfig.builder()
                .storage(storage)
                .thresholdBytes(threshold)
                .compression(parseCompression(environ.get(ENV_COMPRESSION)))
                .urlValidator(storage.urlValidator())
                .build();
    }

    /**
     * Client external-location config pinned to the blob vendor's hosts, for
     * clients that hold a vendor token (e.g. a worker calling another worker).
     *
     * @param env environment mapping ({@code null} for the process environment)
     * @return the config, or {@code null} when {@value #ENV_URL} is unset
     * @throws IllegalArgumentException if the vendor URL is set without a token
     */
    public static ExternalLocationConfig clientExternalConfigFromEnv(Map<String, String> env) {
        Map<String, String> environ = resolveEnv(env);
        if (envOrEmpty(environ, ENV_URL).isEmpty()) return null;
        String[] urlToken = envUrlAndToken(environ);
        return ExternalLocationConfig.builder().urlValidator(vendorUrlValidator(urlToken[0], urlToken[1])).build();
    }

    /**
     * Read {@value #ENV_MAX_UPLOAD_BYTES} (advertised client-upload size).
     *
     * @param env environment mapping ({@code null} for the process environment)
     * @return the size, or {@code null} when unset
     */
    public static Long maxUploadBytesFromEnv(Map<String, String> env) {
        return positiveLongFromEnv(env, ENV_MAX_UPLOAD_BYTES, null);
    }

    /**
     * Read {@value #ENV_MAX_REQUEST_BYTES} (advertised and enforced request cap).
     *
     * @param env environment mapping ({@code null} for the process environment)
     * @return the cap, or {@code null} when unset
     */
    public static Long maxRequestBytesFromEnv(Map<String, String> env) {
        return positiveLongFromEnv(env, ENV_MAX_REQUEST_BYTES, null);
    }
}
