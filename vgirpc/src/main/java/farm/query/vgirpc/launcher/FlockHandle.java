// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.launcher;

import java.io.IOException;

/** A held {@code flock(2)} lock, acquired via {@link PosixLauncherSupport#tryLock}. {@link #close}
 *  releases it — never re-acquire a handle after closing it; get a new one. */
public interface FlockHandle extends AutoCloseable {
    @Override
    void close() throws IOException;
}
