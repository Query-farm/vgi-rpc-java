// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.transport;

import java.io.InputStream;
import java.io.OutputStream;

/** A bidirectional byte transport carrying serialised IPC streams. */
public interface RpcTransport extends AutoCloseable {

    InputStream reader();
    OutputStream writer();

    @Override
    void close();
}
