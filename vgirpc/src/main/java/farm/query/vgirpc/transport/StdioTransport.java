// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.transport;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

/** Server-side pipe transport reading from stdin and writing to stdout. */
public final class StdioTransport implements RpcTransport {

    private final InputStream in;
    private final OutputStream out;

    public StdioTransport() {
        this.in = new BufferedInputStream(System.in, 1 << 16);
        this.out = new BufferedOutputStream(System.out, 1 << 16);
    }

    @Override public InputStream reader() { return in; }
    @Override public OutputStream writer() { return out; }

    @Override
    public void close() {
        try { out.flush(); } catch (Exception ignore) {}
    }
}
