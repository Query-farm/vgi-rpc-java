// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0
package farm.query.vgirpc.identity;

import java.time.Instant;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/** Immutable transport facts supplied to identity providers, never put on the VGI wire. */
public record PeerResolutionContext(
        String transport,
        String immediatePeer,
        String sourceEndpoint,
        String assertedPeer,
        String destinationAddress,
        String authority,
        String serviceName,
        Map<String, List<String>> headers,
        Map<String, Object> metadata,
        Instant deadline,
        long budgetNanos,
        long startedNanos) {

    /**
     * Source-compatible constructor for callers that predate the source-endpoint split.
     * The old immediate-peer value remains the trust-boundary value; no port is inferred.
     */
    public PeerResolutionContext(
            String transport, String immediatePeer, String assertedPeer, String destinationAddress,
            String authority, String serviceName, Map<String, List<String>> headers,
            Map<String, Object> metadata, Instant deadline, long budgetNanos, long startedNanos) {
        this(transport, immediatePeer, null, assertedPeer, destinationAddress, authority, serviceName,
                headers, metadata, deadline, budgetNanos, startedNanos);
    }

    /** Source-compatible constructor; derives a monotonic budget from the diagnostic deadline. */
    public PeerResolutionContext(
            String transport, String immediatePeer, String assertedPeer, String destinationAddress,
            String authority, String serviceName, Map<String, List<String>> headers,
            Map<String, Object> metadata, Instant deadline) {
        this(transport, immediatePeer, assertedPeer, destinationAddress, authority, serviceName,
                headers, metadata, deadline,
                deadline != null ? Math.max(0L, Duration.between(Instant.now(), deadline).toNanos()) : 0L,
                System.nanoTime());
    }

    /** Constructor that carries both the normalized trust peer and the source IP:port. */
    public PeerResolutionContext(
            String transport, String immediatePeer, String sourceEndpoint, String assertedPeer,
            String destinationAddress, String authority, String serviceName,
            Map<String, List<String>> headers, Map<String, Object> metadata, Instant deadline) {
        this(transport, immediatePeer, sourceEndpoint, assertedPeer, destinationAddress, authority,
                serviceName, headers, metadata, deadline,
                deadline != null ? Math.max(0L, Duration.between(Instant.now(), deadline).toNanos()) : 0L,
                System.nanoTime());
    }

    public PeerResolutionContext {
        if (transport == null || transport.isBlank()) throw new IllegalArgumentException("transport is required");
        Map<String, List<String>> copied = new LinkedHashMap<>();
        if (headers != null) {
            headers.forEach((name, values) -> {
                if (name == null || containsControl(name)) throw new IllegalArgumentException("invalid header name");
                String normalized = name.toLowerCase(Locale.ROOT);
                if (copied.containsKey(normalized)) {
                    throw new PeerIdentityRejectedException("case-varied duplicate peer identity header");
                }
                List<String> snapshot = values == null ? List.of() : List.copyOf(values);
                if (snapshot.stream().anyMatch(value -> value == null || containsControl(value))) {
                    throw new IllegalArgumentException("invalid header value");
                }
                copied.put(normalized, snapshot);
            });
        }
        headers = Collections.unmodifiableMap(copied);
        metadata = JsonValues.snapshotMap(metadata);
        if (budgetNanos < 0) throw new IllegalArgumentException("budgetNanos must be >= 0");
    }

    /** Remaining provider budget measured with the JVM monotonic clock. */
    public Duration remainingBudget() {
        if (budgetNanos == 0) return Duration.ZERO;
        return Duration.ofNanos(Math.max(0L, budgetNanos - (System.nanoTime() - startedNanos)));
    }

    /** Return one header or reject an ambiguous duplicate. */
    public String header(String name) {
        Objects.requireNonNull(name, "name");
        List<String> values = headers.getOrDefault(name.toLowerCase(Locale.ROOT), List.of());
        if (values.size() > 1) throw new PeerIdentityRejectedException("duplicate peer identity header: " + name);
        return values.isEmpty() ? null : values.getFirst();
    }

    private static boolean containsControl(String value) {
        return value.codePoints().anyMatch(code -> code <= 0x1f || code == 0x7f);
    }
}
