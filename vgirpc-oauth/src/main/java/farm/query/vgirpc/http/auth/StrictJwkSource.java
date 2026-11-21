// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http.auth;

import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.JWKSelector;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.source.JWKSource;
import com.nimbusds.jose.proc.SecurityContext;
import farm.query.vgirpc.http.AuthUnavailableException;

import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.function.LongSupplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A JWKS source that fails closed, for {@link JwtAuthenticator}'s hardening options.
 *
 * <p>Mirrors vgi-rpc-python's {@code jwt_authenticate(jwks_max_age=...,
 * jwks_refresh_min_interval=..., jwks_fetch_timeout=...)}:
 * <ul>
 *   <li>Fetched keys are used for at most {@code maxAge}, or the response's shorter
 *       {@code Cache-Control: max-age}, measured from when the fetch started.</li>
 *   <li>When that passes the keys are discarded. If the re-fetch fails, verification fails with
 *       {@link AuthUnavailableException} (HTTP 503 with {@code Retry-After}); stale keys are never
 *       used.</li>
 *   <li>A token whose {@code kid} matches no cached key triggers a re-fetch, at most once per
 *       {@code refreshMinInterval}. Within the interval such a token is simply rejected.</li>
 * </ul>
 *
 * <p>Unlike nimbus's {@code JWKSourceBuilder} cache, which keeps serving the last good key set
 * through an outage, nothing here ever outlives its maximum age.
 */
final class StrictJwkSource implements JWKSource<SecurityContext> {

    /** Upper bound on a JWKS document; real ones are a few KiB. */
    private static final int MAX_JWKS_BYTES = 1 << 20;

    private static final Pattern MAX_AGE = Pattern.compile(
            "(?:^|,)\\s*max-age\\s*=\\s*\"?(\\d+)\"?\\s*(?=,|$)", Pattern.CASE_INSENSITIVE);

    /** A fetched key set and the monotonic time (nanos) it stops being usable. */
    private record Cached(JWKSet keys, long expiresAtNanos, boolean expires) {
        boolean usable(long now) { return !expires || now - expiresAtNanos < 0; }
    }

    private final URI uri;
    private final Long maxAgeNanos;
    private final Long refreshMinIntervalNanos;
    private final Duration fetchTimeout;
    private final LongSupplier clock;
    private final HttpClient http;

    private volatile Cached cache;
    private long lastFetchAttemptNanos;
    private boolean fetchAttempted;

    /**
     * @param url                    the JWKS endpoint
     * @param maxAgeSeconds          hard cap on key lifetime, or {@code null} for none
     * @param refreshMinIntervalSeconds minimum seconds between unknown-{@code kid} re-fetches, or
     *                               {@code null} to re-fetch on every unknown {@code kid}
     * @param fetchTimeout           timeout for each JWKS request
     * @param clock                  monotonic nanosecond clock (tests inject one)
     */
    StrictJwkSource(URL url, Long maxAgeSeconds, Long refreshMinIntervalSeconds,
                    Duration fetchTimeout, LongSupplier clock) {
        try {
            this.uri = url.toURI();
        } catch (Exception e) {
            throw new IllegalArgumentException("bad jwksUri: " + url, e);
        }
        this.maxAgeNanos = maxAgeSeconds == null ? null : Duration.ofSeconds(maxAgeSeconds).toNanos();
        this.refreshMinIntervalNanos = refreshMinIntervalSeconds == null
                ? null : Duration.ofSeconds(refreshMinIntervalSeconds).toNanos();
        this.fetchTimeout = fetchTimeout;
        this.clock = clock;
        this.http = HttpClient.newBuilder()
                .connectTimeout(fetchTimeout)
                .followRedirects(HttpClient.Redirect.NEVER)
                .build();
    }

    @Override
    public List<JWK> get(JWKSelector selector, SecurityContext context) {
        JWKSet keys = currentKeys();
        List<JWK> matches = selector.select(keys);
        if (!matches.isEmpty()) return matches;
        JWKSet refreshed = refreshForUnknownKid(keys);
        return refreshed == null ? List.of() : selector.select(refreshed);
    }

    /** Keys within their maximum age, fetching when there are none. */
    private JWKSet currentKeys() {
        Cached entry = cache;
        if (entry != null && entry.usable(clock.getAsLong())) return entry.keys();
        synchronized (this) {
            entry = cache;
            if (entry != null && entry.usable(clock.getAsLong())) return entry.keys();
            return fetchLocked();
        }
    }

    /**
     * Re-fetch after an unknown {@code kid}; {@code null} when rate limited. If another thread
     * refreshed while this one waited for the lock, its result is reused rather than stampeding
     * the endpoint during a key rotation.
     */
    private synchronized JWKSet refreshForUnknownKid(JWKSet seen) {
        Cached entry = cache;
        long now = clock.getAsLong();
        if (entry != null && entry.keys() != seen && entry.usable(now)) return entry.keys();
        if (refreshMinIntervalNanos != null && fetchAttempted
                && now - lastFetchAttemptNanos < refreshMinIntervalNanos) {
            return null;
        }
        return fetchLocked();
    }

    /** Fetch the JWKS and replace the cache. The caller holds the monitor. */
    private JWKSet fetchLocked() {
        long started = clock.getAsLong();
        lastFetchAttemptNanos = started;
        fetchAttempted = true;
        JWKSet keys;
        Optional<String> cacheControl;
        try {
            HttpRequest request = HttpRequest.newBuilder(uri)
                    .timeout(fetchTimeout)
                    .header("Accept", "application/json")
                    .GET()
                    .build();
            HttpResponse<InputStream> response = http.send(request, HttpResponse.BodyHandlers.ofInputStream());
            byte[] body;
            try (InputStream in = response.body()) {
                body = in.readNBytes(MAX_JWKS_BYTES + 1);
            }
            if (response.statusCode() != 200) {
                throw new IllegalStateException("JWKS endpoint answered HTTP " + response.statusCode());
            }
            if (body.length > MAX_JWKS_BYTES) {
                throw new IllegalStateException("JWKS document exceeds " + MAX_JWKS_BYTES + " bytes");
            }
            keys = JWKSet.parse(new String(body, StandardCharsets.UTF_8));
            cacheControl = response.headers().firstValue("Cache-Control");
        } catch (Exception e) {
            if (e instanceof InterruptedException) Thread.currentThread().interrupt();
            // Never fall back to stale keys: drop an expired entry so nothing can read it, and
            // make the caller fail closed with a 503 rather than claim the token is bad.
            Cached entry = cache;
            if (entry != null && !entry.usable(clock.getAsLong())) cache = null;
            throw new AuthUnavailableException("JWKS endpoint unavailable (" + e.getClass().getSimpleName() + ")");
        }
        if (maxAgeNanos == null) {
            cache = new Cached(keys, 0L, false);
        } else {
            long ttl = maxAgeNanos;
            Long advertised = cacheControlMaxAge(cacheControl.orElse(null));
            if (advertised != null) ttl = Math.min(ttl, Duration.ofSeconds(advertised).toNanos());
            cache = new Cached(keys, started + ttl, true);
        }
        return keys;
    }

    /**
     * The {@code max-age} directive of a {@code Cache-Control} header, in seconds, or {@code null}.
     *
     * @param value the header value, or {@code null}
     * @return the directive's value, or {@code null} when absent or unparseable
     */
    static Long cacheControlMaxAge(String value) {
        if (value == null || value.isEmpty()) return null;
        Matcher m = MAX_AGE.matcher(value);
        if (!m.find()) return null;
        try {
            return Long.parseLong(m.group(1));
        } catch (NumberFormatException e) {
            return Long.MAX_VALUE / 1_000_000_000L; // absurdly large: the max-age cap still applies
        }
    }
}
