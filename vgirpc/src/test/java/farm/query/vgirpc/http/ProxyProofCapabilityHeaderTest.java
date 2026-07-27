// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import farm.query.vgirpc.RpcServer;
import farm.query.vgirpc.http.auth.ProxyProof;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code VGI-Proxy-Proof-Required} end to end: a live server advertises the
 * capability in {@code require} mode and only there.
 *
 * <p>The negative cases are the point. The gate reaches the server as an opaque
 * {@link Authenticator}, so nothing stops a worker from advertising a posture it
 * does not hold — and a worker in {@code allow} claiming to reject unproofed
 * requests is exactly the misconfiguration the header exists to surface. Mirrors
 * the shared conformance pair {@code TestProxyProof::test_require_mode_advertises
 * _the_capability} / {@code ::test_allow_mode_does_not_advertise_the_capability}.
 */
final class ProxyProofCapabilityHeaderTest {

    public interface EchoService {
        String echo(String value);
    }

    public static final class EchoImpl implements EchoService {
        @Override public String echo(String value) { return value; }
    }

    private HttpServer server;
    private String base;

    @AfterEach
    void stop() throws Exception {
        if (server != null) server.stop();
    }

    /** Boot a server with the given advertisement and (optionally) a real gate. */
    private void start(boolean advertise, ProxyProof.Mode gateMode) throws Exception {
        HttpServer.Config.Builder cb = HttpServer.Config.builder()
                .prefix("/vgi")
                .proxyProofRequired(advertise);
        if (gateMode != null) {
            byte[] secret = new byte[32];
            Arrays.fill(secret, (byte) 0x11);
            cb.authenticator(ProxyProof.require(
                    ProxyProof.Config.of(gateMode, "test-origin",
                            Map.of("test-proxy", new ProxyProof.Secret(secret, "test-proxy"))),
                    null));
        }
        server = new HttpServer(new RpcServer(EchoService.class, new EchoImpl()), cb.build());
        server.start();
        base = "http://127.0.0.1:" + server.port() + "/vgi";
    }

    /** {@code require}: the header is present and says {@code true}. */
    @Test
    void require_mode_advertises_the_capability() throws Exception {
        start(true, ProxyProof.Mode.REQUIRE);
        assertEquals("true", header(get(base + "/health")).orElseThrow(
                () -> new AssertionError("require mode must advertise "
                        + ProxyProof.PROOF_REQUIRED_HEADER)));
    }

    /** It is a capability header, so it rides the canonical discovery probe too. */
    @Test
    void require_mode_advertises_on_the_options_probe() throws Exception {
        start(true, ProxyProof.Mode.REQUIRE);
        assertEquals("true", header(options(base + "/health")).orElseThrow(
                () -> new AssertionError("OPTIONS is the capability-discovery target")));
    }

    /** {@code allow} never denies, so it must not claim it does — even with a
     *  gate genuinely installed. */
    @Test
    void allow_mode_does_not_advertise_the_capability() throws Exception {
        start(false, ProxyProof.Mode.ALLOW);
        assertTrue(header(get(base + "/health")).isEmpty(),
                "allow mode must not advertise " + ProxyProof.PROOF_REQUIRED_HEADER);
    }

    /** {@code off} installs no gate at all and must look exactly like a worker
     *  from before the feature existed. */
    @Test
    void off_mode_does_not_advertise_the_capability() throws Exception {
        start(false, null);
        assertTrue(header(get(base + "/health")).isEmpty(),
                "off mode must not advertise " + ProxyProof.PROOF_REQUIRED_HEADER);
    }

    // ---- helpers ---------------------------------------------------------

    private static Optional<String> header(HttpResponse<?> resp) {
        return resp.headers().firstValue(ProxyProof.PROOF_REQUIRED_HEADER);
    }

    private static HttpResponse<String> get(String url) throws Exception {
        try (HttpClient client = newClient()) {
            HttpResponse<String> resp = client.send(
                    HttpRequest.newBuilder(URI.create(url))
                            .timeout(Duration.ofSeconds(10)).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(200, resp.statusCode());
            return resp;
        }
    }

    private static HttpResponse<Void> options(String url) throws Exception {
        try (HttpClient client = newClient()) {
            HttpResponse<Void> resp = client.send(
                    HttpRequest.newBuilder(URI.create(url))
                            .timeout(Duration.ofSeconds(10))
                            .method("OPTIONS", HttpRequest.BodyPublishers.noBody()).build(),
                    HttpResponse.BodyHandlers.discarding());
            assertEquals(200, resp.statusCode());
            return resp;
        }
    }

    private static HttpClient newClient() {
        return HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    }
}
