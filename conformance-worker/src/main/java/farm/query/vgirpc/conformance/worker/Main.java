// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.conformance.worker;

import farm.query.vgirpc.RpcServer;
import farm.query.vgirpc.conformance.ConformanceService;
import farm.query.vgirpc.conformance.ConformanceServiceImpl;
import farm.query.vgirpc.http.HttpServer;
import farm.query.vgirpc.transport.StdioTransport;
import farm.query.vgirpc.transport.UnixSocketTransport;

import java.nio.file.Path;

public final class Main {

    private Main() {}

    public static void main(String[] args) throws Exception {
        ConformanceService impl = new ConformanceServiceImpl();
        RpcServer server = new RpcServer(ConformanceService.class, impl);

        if (args.length == 0) { servePipe(server); return; }
        switch (args[0]) {
            case "--http" -> serveHttp(server);
            case "--unix" -> {
                if (args.length < 2) { System.err.println("--unix requires a path"); System.exit(2); }
                serveUnix(server, Path.of(args[1]));
            }
            default -> { System.err.println("unknown args: " + String.join(" ", args)); System.exit(2); }
        }
    }

    private static void servePipe(RpcServer server) {
        try (StdioTransport t = new StdioTransport()) { server.serve(t); }
    }

    private static void serveHttp(RpcServer server) throws Exception {
        HttpServer http = new HttpServer(server);
        http.start();
        System.out.println("PORT:" + http.port());
        System.out.flush();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try { http.stop(); } catch (Exception ignore) {}
        }));
        http.join();
    }

    private static void serveUnix(RpcServer server, Path path) throws Exception {
        UnixSocketTransport.serveForever(path, server);
    }
}
