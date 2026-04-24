// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.transport;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** Client-side transport that spawns a subprocess and talks to it over its stdio pipes. */
public final class SubprocessTransport implements RpcTransport {

    private final Process process;

    public SubprocessTransport(List<String> command) {
        try {
            ProcessBuilder pb = new ProcessBuilder(command);
            pb.redirectError(ProcessBuilder.Redirect.INHERIT);
            this.process = pb.start();
        } catch (IOException e) {
            throw new RuntimeException("failed to start subprocess: " + command, e);
        }
    }

    @Override public InputStream reader() { return process.getInputStream(); }
    @Override public OutputStream writer() { return process.getOutputStream(); }

    @Override
    public void close() {
        try { process.getOutputStream().close(); } catch (Exception ignore) {}
        try {
            if (!process.waitFor(2, TimeUnit.SECONDS)) process.destroyForcibly();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            process.destroyForcibly();
        }
    }
}
