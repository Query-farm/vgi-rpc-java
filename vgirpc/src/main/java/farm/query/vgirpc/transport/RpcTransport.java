// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.transport;

import java.io.InputStream;
import java.io.OutputStream;

/** A bidirectional byte transport carrying serialised IPC streams. */
public interface RpcTransport extends AutoCloseable {

    /** Inbound half of the transport; Arrow IPC streams are read from it sequentially.
     * @return the inbound byte stream carrying response (client) or request (server) IPC streams. */
    InputStream reader();
    /** Outbound half of the transport; Arrow IPC streams are written to it sequentially.
     * @return the outbound byte stream carrying request (client) or response (server) IPC streams. */
    OutputStream writer();

    /** Flush and release the transport's underlying resources. */
    @Override
    void close();
}
