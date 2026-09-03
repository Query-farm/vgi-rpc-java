// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.transport;

import java.io.IOException;

/** Owned, concurrency-safe HTTP client transport over {@code iroh-http/2}. */
public interface IrohHttpTransport extends AutoCloseable {
    IrohHttpResponse execute(IrohHttpRequest request) throws IOException;

    @Override void close() throws IOException;
}
