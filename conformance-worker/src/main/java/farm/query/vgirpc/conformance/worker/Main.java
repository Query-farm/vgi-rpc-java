// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.conformance.worker;

import farm.query.vgirpc.RpcServer;
import farm.query.vgirpc.conformance.ConformanceService;
import farm.query.vgirpc.conformance.ConformanceServiceImpl;
import farm.query.vgirpc.transport.StdioTransport;

public final class Main {

    private Main() {}

    public static void main(String[] args) {
        if (args.length == 0) {
            servePipe();
            return;
        }
        switch (args[0]) {
            case "--http" -> System.err.println("--http transport not yet implemented");
            case "--unix" -> System.err.println("--unix transport not yet implemented");
            default -> {
                System.err.println("unknown args: " + String.join(" ", args));
                System.exit(2);
            }
        }
    }

    private static void servePipe() {
        ConformanceService impl = new ConformanceServiceImpl();
        RpcServer server = new RpcServer(ConformanceService.class, impl);
        try (StdioTransport t = new StdioTransport()) {
            server.serve(t);
        }
    }
}
