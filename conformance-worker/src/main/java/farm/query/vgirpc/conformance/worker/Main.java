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

        // Optional flags (order independent after the mode flag)
        byte[] signingKey = null;
        long tokenTtl = 0;
        String mode = null;
        String unixPath = null;
        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            switch (a) {
                case "--http" -> mode = "http";
                case "--unix" -> {
                    mode = "unix";
                    if (i + 1 < args.length) { unixPath = args[++i]; }
                    else { System.err.println("--unix requires a path"); System.exit(2); }
                }
                case "--signing-key" -> {
                    if (i + 1 >= args.length) { System.err.println("--signing-key requires a hex value"); System.exit(2); }
                    signingKey = parseHex(args[++i]);
                }
                case "--token-ttl" -> {
                    if (i + 1 >= args.length) { System.err.println("--token-ttl requires a seconds value"); System.exit(2); }
                    tokenTtl = Long.parseLong(args[++i]);
                }
                default -> {
                    System.err.println("unknown arg: " + a);
                    System.exit(2);
                }
            }
        }
        if (mode == null) { servePipe(server); return; }
        switch (mode) {
            case "http" -> serveHttp(server, signingKey, tokenTtl);
            case "unix" -> serveUnix(server, Path.of(unixPath));
            default -> { System.err.println("unknown mode: " + mode); System.exit(2); }
        }
    }

    private static byte[] parseHex(String hex) {
        int len = hex.length();
        if ((len & 1) != 0) throw new IllegalArgumentException("hex string must have even length");
        byte[] out = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            out[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4) | Character.digit(hex.charAt(i + 1), 16));
        }
        return out;
    }

    private static void servePipe(RpcServer server) {
        try (StdioTransport t = new StdioTransport()) { server.serve(t); }
    }

    private static void serveHttp(RpcServer server, byte[] signingKey, long tokenTtl) throws Exception {
        HttpServer http = new HttpServer(server, "", 0, signingKey, tokenTtl);
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
