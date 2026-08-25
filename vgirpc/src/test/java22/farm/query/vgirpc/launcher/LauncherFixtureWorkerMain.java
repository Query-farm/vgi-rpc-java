// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.launcher;

import farm.query.vgirpc.RpcServer;
import farm.query.vgirpc.transport.UnixSocketTransport;

import java.nio.file.Path;

/**
 * A minimal launcher-protocol-compliant worker ({@code --unix PATH --idle-timeout SEC},
 * one {@code UNIX:<path>} discovery line, self-shutdown when idle) used only by {@link
 * LauncherClientTest} — a trivial in-tree fixture rather than a dependency on the
 * separate {@code conformance}/{@code conformance-worker} modules for a single echo
 * method. {@link UnixSocketTransport#serveForever} already emits the discovery line and
 * implements the idle watchdog; this class only wires the CLI surface to it.
 */
public final class LauncherFixtureWorkerMain {

    private LauncherFixtureWorkerMain() {}

    /** Single-method service the test calls to prove the launched worker is genuinely live. */
    public interface Echo {
        String echo(String value);
    }

    public static void main(String[] args) throws Exception {
        String unixPath = null;
        double idleTimeoutSeconds = 0;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--unix" -> unixPath = args[++i];
                case "--idle-timeout" -> idleTimeoutSeconds = Double.parseDouble(args[++i]);
                default -> throw new IllegalArgumentException("unknown arg: " + args[i]);
            }
        }
        if (unixPath == null) throw new IllegalArgumentException("--unix is required");
        RpcServer server = new RpcServer(Echo.class, (Echo) value -> value);
        UnixSocketTransport.serveForever(Path.of(unixPath), server, (long) (idleTimeoutSeconds * 1000));
    }
}
