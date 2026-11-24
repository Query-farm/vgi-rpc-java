// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.conformance.driver;

import com.fasterxml.jackson.databind.ObjectMapper;
import farm.query.vgirpc.RpcError;
import farm.query.vgirpc.http.HttpRpcConnection;
import farm.query.vgirpc.schema.ProtocolName;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The {@code conformance_negotiating_client} hook of the shared suite (PROTOCOL_VERSIONS_V1 §9,
 * {@code TestClientNegotiation}): an HTTP client of this port bound to
 * {@code conformance.Negotiated.v1}, declaring a newest version and a supported window,
 * negotiating per §6.
 *
 * <pre>conformance-client-driver --negotiating-client &lt;base-url&gt; &lt;version&gt; &lt;version-range&gt;</pre>
 *
 * <p>Each line {@code ping} on stdin makes one {@code ping()} call on the same connection, so
 * negotiation state carries across calls, and prints one JSON line: {@code {"result": ...}} or
 * {@code {"error_kind", "error_code", "error_message"}}. EOF exits.</p>
 */
final class NegotiatingClient {

    private NegotiatingClient() {}

    /** The protocol the shared suite's scripted server hosts. */
    @ProtocolName("conformance.Negotiated.v1")
    public interface Negotiated {
        /**
         * Echo the version the server saw.
         *
         * @return the {@code vgi_rpc.protocol_version} the request carried
         */
        String ping();
    }

    static void run(String[] args, PrintStream control) throws java.io.IOException {
        if (args.length != 4) {
            System.err.println("usage: --negotiating-client <base-url> <version> <version-range>");
            System.exit(2);
        }
        ObjectMapper json = new ObjectMapper();
        try (HttpRpcConnection conn = HttpRpcConnection.builder(args[1])
                .protocolVersion(args[2])
                .protocolVersionRange(args[3])
                .build()) {
            Negotiated client = conn.proxy(Negotiated.class);
            BufferedReader in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
            String line;
            while ((line = in.readLine()) != null) {
                Map<String, Object> reply = new LinkedHashMap<>();
                if (!"ping".equals(line.strip())) {
                    reply.put("error_kind", "driver_error");
                    reply.put("error_message", "unknown command " + line);
                } else {
                    try {
                        reply.put("result", client.ping());
                    } catch (RpcError e) {
                        reply.put("error_kind", e.errorKind() == null ? "" : e.errorKind());
                        reply.put("error_code", e.errorCode());
                        reply.put("error_message", e.errorMessage());
                    } catch (RuntimeException e) {
                        reply.put("error_kind", "");
                        reply.put("error_message", String.valueOf(e.getMessage()));
                    }
                }
                control.println(json.writeValueAsString(reply));
                control.flush();
            }
        }
    }
}
