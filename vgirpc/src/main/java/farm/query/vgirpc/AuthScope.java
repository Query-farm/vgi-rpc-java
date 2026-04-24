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

    public static final class Scope {
        public final AuthContext auth;
        public final Map<String, Object> transportMetadata;

        public Scope(AuthContext auth, Map<String, Object> transportMetadata) {
            this.auth = auth != null ? auth : AuthContext.ANONYMOUS;
            this.transportMetadata = transportMetadata != null ? transportMetadata : Collections.emptyMap();
        }
    }

    private static final Scope DEFAULT = new Scope(AuthContext.ANONYMOUS, Collections.emptyMap());
    private static final ThreadLocal<Scope> CURRENT = ThreadLocal.withInitial(() -> DEFAULT);

    private AuthScope() {}

    /** Push a new scope for the current thread; the returned {@link AutoCloseable} restores on close. */
    public static AutoCloseable push(AuthContext auth, Map<String, Object> transportMetadata) {
        Scope previous = CURRENT.get();
        CURRENT.set(new Scope(auth, transportMetadata));
        return () -> CURRENT.set(previous);
    }

    /** Return the current scope (never null). */
    public static Scope current() {
        return CURRENT.get();
    }
}
