// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

import java.util.Collections;
import java.util.Map;

/**
 * Thread-local carrier for the authenticated {@link AuthContext} and per-request
 * transport metadata (e.g. {@code remote_addr}) that transports like the HTTP
 * server populate before invoking {@link RpcServer#serveOne}.
 *
 * <p>Mirrors the Python {@code _current_transport} contextvar. When nothing is
 * pushed, calls resolve to anonymous auth and empty metadata, which matches
 * the pipe / subprocess / unix transports.</p>
 */
public final class AuthScope {

    /**
     * The bundle published into the thread-local scope for the duration of a
     * request: the resolved {@link AuthContext} and the request's transport
     * metadata. Null components are normalised to {@link AuthContext#ANONYMOUS}
     * and an empty map.
     *
     * @param auth the authenticated principal for the current request
     * @param transportMetadata per-request transport metadata (e.g. {@code remote_addr})
     */
    public record Scope(AuthContext auth, Map<String, Object> transportMetadata) {
        /** Normalises {@code null} components to anonymous auth and an empty, unmodifiable map. */
        public Scope {
            auth = auth != null ? auth : AuthContext.ANONYMOUS;
            transportMetadata = transportMetadata != null
                    ? Collections.unmodifiableMap(transportMetadata)
                    : Collections.emptyMap();
        }
    }

    private static final Scope DEFAULT = new Scope(AuthContext.ANONYMOUS, Collections.emptyMap());
    private static final ThreadLocal<Scope> CURRENT = ThreadLocal.withInitial(() -> DEFAULT);

    private AuthScope() {}

    /**
     * Push a new scope for the current thread; the returned {@link AutoCloseable}
     * restores the previous scope on close (use in try-with-resources around
     * the dispatch).
     *
     * @param auth authenticated principal; {@code null} becomes {@link AuthContext#ANONYMOUS}
     * @param transportMetadata per-request transport metadata; {@code null} becomes empty
     * @return a handle that restores the previously active scope when closed
     */
    public static AutoCloseable push(AuthContext auth, Map<String, Object> transportMetadata) {
        Scope previous = CURRENT.get();
        CURRENT.set(new Scope(auth, transportMetadata));
        return () -> CURRENT.set(previous);
    }

    /**
     * Return the current scope (never null).
     *
     * @return the scope installed by the innermost active {@link #push}, or the
     *     anonymous default when no transport has pushed one
     */
    public static Scope current() {
        return CURRENT.get();
    }
}
