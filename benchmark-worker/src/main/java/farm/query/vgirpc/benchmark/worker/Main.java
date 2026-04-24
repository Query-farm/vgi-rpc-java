// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.benchmark.worker;

import farm.query.vgirpc.RpcServer;
import farm.query.vgirpc.benchmark.BenchmarkService;
import farm.query.vgirpc.benchmark.BenchmarkServiceImpl;
import farm.query.vgirpc.http.HttpServer;
import farm.query.vgirpc.transport.StdioTransport;
import farm.query.vgirpc.transport.UnixSocketTransport;

import java.nio.file.Path;

/**
 * Benchmark worker — identical CLI contract to the conformance worker
 * (no args = pipe, {@code --http} = HTTP + prints {@code PORT:<n>}, {@code --unix <path>}).
 */
public final class Main {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(Main.class);

    private Main() {}

    public static void main(String[] args) throws Exception {
        BenchmarkService impl = new BenchmarkServiceImpl();
        RpcServer server = new RpcServer(BenchmarkService.class, impl);

        if (args.length == 0) {
            try (StdioTransport t = new StdioTransport()) { server.serve(t); }
            return;
        }
        switch (args[0]) {
            case "--http" -> {
                HttpServer http = new HttpServer(server);
                http.start();
                System.out.println("PORT:" + http.port());
                System.out.flush();
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    try { http.stop(); } catch (Exception e) { LOG.warn("http stop failed during shutdown", e); }
                }));
                http.join();
            }
            case "--unix" -> {
                if (args.length < 2) { System.err.println("--unix requires a path"); System.exit(2); }
                UnixSocketTransport.serveForever(Path.of(args[1]), server);
            }
            default -> { System.err.println("unknown args: " + String.join(" ", args)); System.exit(2); }
        }
    }
}
