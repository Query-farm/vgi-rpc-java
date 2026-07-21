// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import com.github.luben.zstd.Zstd;
import farm.query.vgirpc.http.HttpServer.ResponseEncoding;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Response-codec negotiation: the client's stated order decides, VGI's own
 * {@code X-VGI-Accept-Encoding} outranks the generic {@code Accept-Encoding},
 * {@code identity} is an explicit "send it uncompressed", and a codec that only
 * the custom header offered is announced on {@code X-VGI-Content-Encoding}.
 */
final class ResponseEncodingTest {

    /** Configured producible sets: the default, the zstd-off narrowing, and off. */
    private static final List<String> BOTH = HttpServer.Config.DEFAULT_SUPPORTED_ENCODINGS;
    private static final List<String> GZIP_ONLY = List.of(MediaTypes.GZIP);
    private static final List<String> NOTHING = List.of();

    private static HttpServletRequest req(Map<String, String> headers) {
        return HttpRequestStub.withHeaders(headers);
    }

    // ---- the merged walk -------------------------------------------------

    /** The browser/WASM case: fetch() cannot set Accept-Encoding, so the custom
     *  header is all the server sees — and the answer must be announced on the
     *  custom response header too. */
    @Test
    void custom_header_alone_chooses_codec_and_uses_custom_response_header() {
        ResponseEncoding c = HttpServer.chooseResponseEncoding(
                req(Map.of(HttpHeaders.X_VGI_ACCEPT_ENCODING, "zstd, gzip")), BOTH);
        assertEquals(MediaTypes.ZSTD, c.encoding());
        assertTrue(c.usedCustomHeader());
    }

    /** The cpp-httplib regression case: the injected Accept-Encoding lists gzip
     *  before zstd, but VGI states zstd first — zstd must win. It is present in
     *  both headers, so it is announced on the standard Content-Encoding. */
    @Test
    void custom_order_beats_standard_gzip_first() {
        ResponseEncoding c = HttpServer.chooseResponseEncoding(
                req(Map.of(
                        HttpHeaders.ACCEPT_ENCODING, "deflate, gzip, br, zstd",
                        HttpHeaders.X_VGI_ACCEPT_ENCODING, "zstd, gzip")), BOTH);
        assertEquals(MediaTypes.ZSTD, c.encoding());
        assertFalse(c.usedCustomHeader());
    }

    @Test
    void no_encoding_headers_means_uncompressed() {
        ResponseEncoding c = HttpServer.chooseResponseEncoding(req(Map.of()), BOTH);
        assertNull(c.encoding());
    }

    @Test
    void only_unknown_codecs_offered_means_uncompressed() {
        ResponseEncoding c = HttpServer.chooseResponseEncoding(
                req(Map.of(HttpHeaders.ACCEPT_ENCODING, "br, deflate")), BOTH);
        assertNull(c.encoding());
    }

    /** Producibility is checked inside the walk, not as a pre-filter: with zstd
     *  gated off the only offered codec is unproducible, so nothing is chosen. */
    @Test
    void only_disabled_codec_offered_means_uncompressed() {
        ResponseEncoding c = HttpServer.chooseResponseEncoding(
                req(Map.of(HttpHeaders.X_VGI_ACCEPT_ENCODING, "zstd")), GZIP_ONLY);
        assertNull(c.encoding());
    }

    /** A server that can produce nothing compresses nothing, however eager the
     *  client is — the behavioural half of an empty {@code VGI-Supported-Encodings}. */
    @Test
    void server_producing_nothing_answers_uncompressed() {
        ResponseEncoding c = HttpServer.chooseResponseEncoding(
                req(Map.of(
                        HttpHeaders.X_VGI_ACCEPT_ENCODING, "zstd, gzip",
                        HttpHeaders.ACCEPT_ENCODING, "gzip, zstd")), NOTHING);
        assertNull(c.encoding());
    }

    @Test
    void disabled_zstd_falls_through_to_gzip() {
        ResponseEncoding c = HttpServer.chooseResponseEncoding(
                req(Map.of(HttpHeaders.X_VGI_ACCEPT_ENCODING, "zstd, gzip")), GZIP_ONLY);
        assertEquals(MediaTypes.GZIP, c.encoding());
        assertTrue(c.usedCustomHeader());
    }

    /** q-values are parsed off and ignored — never honoured as a ranking. */
    @Test
    void q_values_are_stripped_and_order_preserved() {
        ResponseEncoding c = HttpServer.chooseResponseEncoding(
                req(Map.of(HttpHeaders.ACCEPT_ENCODING, "zstd;q=0.5, gzip")), BOTH);
        assertEquals(MediaTypes.ZSTD, c.encoding());
        assertFalse(c.usedCustomHeader());
    }

    /** A codec in the standard header but not the custom one is not "custom". */
    @Test
    void codec_from_standard_header_only_is_not_custom() {
        ResponseEncoding c = HttpServer.chooseResponseEncoding(
                req(Map.of(
                        HttpHeaders.X_VGI_ACCEPT_ENCODING, "zstd",
                        HttpHeaders.ACCEPT_ENCODING, "gzip")), GZIP_ONLY);
        assertEquals(MediaTypes.GZIP, c.encoding());
        assertFalse(c.usedCustomHeader());
    }

    // ---- identity --------------------------------------------------------

    /** identity ahead of everything the server can produce = "off, please". */
    @Test
    void identity_first_in_custom_header_wins_over_producible_codecs() {
        ResponseEncoding c = HttpServer.chooseResponseEncoding(
                req(Map.of(
                        HttpHeaders.X_VGI_ACCEPT_ENCODING, "identity",
                        HttpHeaders.ACCEPT_ENCODING, "gzip, zstd")), BOTH);
        assertNull(c.encoding());
        assertFalse(c.usedCustomHeader());
    }

    @Test
    void identity_first_stamps_no_encoding_header_at_all() throws Exception {
        Map<String, String> out = new HashMap<>();
        byte[] body = "arrow-ipc-body".repeat(64).getBytes(StandardCharsets.UTF_8);
        ResponseEncoding c = HttpServer.chooseResponseEncoding(
                req(Map.of(HttpHeaders.X_VGI_ACCEPT_ENCODING, "identity, zstd")), BOTH);
        byte[] written = HttpServer.encodeArrowBody(responseStub(out), c, body, 3);

        assertTrue(out.isEmpty(), "identity stamps neither Content-Encoding nor X-VGI-Content-Encoding");
        assertArrayEquals(body, written);
    }

    /** identity only wins when it is reached first — a producible codec ahead of
     *  it still takes the response. */
    @Test
    void identity_after_a_producible_codec_loses() {
        ResponseEncoding c = HttpServer.chooseResponseEncoding(
                req(Map.of(HttpHeaders.X_VGI_ACCEPT_ENCODING, "zstd, identity")), BOTH);
        assertEquals(MediaTypes.ZSTD, c.encoding());
        assertTrue(c.usedCustomHeader());
    }

    @Test
    void identity_is_honoured_in_the_standard_header_too() {
        ResponseEncoding c = HttpServer.chooseResponseEncoding(
                req(Map.of(HttpHeaders.ACCEPT_ENCODING, "identity, gzip")), BOTH);
        assertNull(c.encoding());
    }

    /** q-values are ignored for identity as for everything else: order alone decides. */
    @Test
    void identity_q_zero_is_not_special_cased() {
        ResponseEncoding c = HttpServer.chooseResponseEncoding(
                req(Map.of(HttpHeaders.ACCEPT_ENCODING, "identity;q=0, gzip")), BOTH);
        assertNull(c.encoding());
    }

    /** The custom header is still walked first: identity there beats a compressed
     *  codec listed first in the standard header. */
    @Test
    void identity_in_custom_header_outranks_standard_header_codecs() {
        ResponseEncoding c = HttpServer.chooseResponseEncoding(
                req(Map.of(
                        HttpHeaders.X_VGI_ACCEPT_ENCODING, "identity",
                        HttpHeaders.ACCEPT_ENCODING, "zstd")), BOTH);
        assertNull(c.encoding());
    }

    // ---- header parsing --------------------------------------------------

    @Test
    void parse_recognises_identity_and_keeps_order() {
        assertEquals(List.of(MediaTypes.GZIP, MediaTypes.ZSTD, MediaTypes.IDENTITY),
                HttpServer.parseEncodingList("  GZip ;q=1.0 , zstd, gzip , br , Identity "));
    }

    @Test
    void parse_of_missing_or_empty_header_is_empty() {
        assertEquals(List.of(), HttpServer.parseEncodingList(null));
        assertEquals(List.of(), HttpServer.parseEncodingList(""));
        assertEquals(List.of(), HttpServer.parseEncodingList("  , ,"));
    }

    // ---- the configured set (VGI-Supported-Encodings) ---------------------

    @Test
    void default_set_is_server_preference_order_without_identity() {
        assertEquals(List.of(MediaTypes.ZSTD, MediaTypes.GZIP),
                HttpServer.Config.DEFAULT_SUPPORTED_ENCODINGS);
        assertFalse(HttpServer.Config.DEFAULT_SUPPORTED_ENCODINGS.contains(MediaTypes.IDENTITY));
    }

    /** The default is the env-var preset unless {@code VGI_HTTP_DISABLE_ZSTD}
     *  is set — the historical knob, now just one narrowing of the general set. */
    @Test
    void default_set_honours_the_disable_zstd_env_var() {
        List<String> expected = System.getenv("VGI_HTTP_DISABLE_ZSTD") == null
                ? HttpServer.Config.DEFAULT_SUPPORTED_ENCODINGS
                : List.of(MediaTypes.GZIP);
        assertEquals(expected, HttpServer.Config.defaultSupportedEncodings());
        assertEquals(expected, HttpServer.Config.defaults().supportedEncodings());
    }

    @Test
    void configured_set_is_normalised_and_deduplicated() {
        assertEquals(List.of(MediaTypes.GZIP, MediaTypes.ZSTD),
                HttpServer.normalizeEncodings(List.of(" GZip ", "zstd", "GZIP")));
        assertEquals(List.of(), HttpServer.normalizeEncodings(List.of()));
    }

    /** An empty set is a real configuration; {@code null} is "unset" and takes
     *  the default — the two must not be conflated. */
    @Test
    void empty_set_survives_config_and_is_not_treated_as_unset() {
        HttpServer.Config off = HttpServer.Config.builder().supportedEncodings(List.of()).build();
        assertEquals(List.of(), off.supportedEncodings());

        HttpServer.Config unset = HttpServer.Config.builder().supportedEncodings(null).build();
        assertEquals(HttpServer.Config.defaultSupportedEncodings(), unset.supportedEncodings());
    }

    @Test
    void configured_set_may_be_narrowed_to_one_codec() {
        assertEquals(List.of(MediaTypes.GZIP),
                HttpServer.Config.builder()
                        .supportedEncodings(List.of(MediaTypes.GZIP)).build().supportedEncodings());
    }

    /** identity is never a member of the producible set: "off" is the empty set,
     *  not a set containing identity. */
    @Test
    void identity_is_rejected_as_a_configured_codec() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> HttpServer.Config.builder()
                        .supportedEncodings(List.of(MediaTypes.IDENTITY)).build());
        assertTrue(e.getMessage().contains(MediaTypes.IDENTITY), e.getMessage());
    }

    @Test
    void unknown_configured_codec_is_rejected() {
        assertThrows(IllegalArgumentException.class,
                () -> HttpServer.Config.builder().supportedEncodings(List.of("br")).build());
        assertThrows(IllegalArgumentException.class,
                () -> HttpServer.normalizeEncodings(java.util.Collections.singletonList(null)));
    }

    // ---- stamping + compression -----------------------------------------

    @Test
    void custom_choice_is_stamped_on_x_vgi_content_encoding() throws Exception {
        Map<String, String> out = new HashMap<>();
        byte[] body = "arrow-ipc-body".repeat(64).getBytes(StandardCharsets.UTF_8);
        byte[] written = HttpServer.encodeArrowBody(responseStub(out),
                new ResponseEncoding(MediaTypes.ZSTD, true), body, 3);

        assertEquals(Map.of(HttpHeaders.X_VGI_CONTENT_ENCODING, MediaTypes.ZSTD), out);
        assertArrayEquals(body, Zstd.decompress(written, body.length));
    }

    @Test
    void standard_choice_is_stamped_on_content_encoding() throws Exception {
        Map<String, String> out = new HashMap<>();
        byte[] body = "arrow-ipc-body".repeat(64).getBytes(StandardCharsets.UTF_8);
        byte[] written = HttpServer.encodeArrowBody(responseStub(out),
                new ResponseEncoding(MediaTypes.ZSTD, false), body, 3);

        assertEquals(Map.of(HttpHeaders.CONTENT_ENCODING, MediaTypes.ZSTD), out);
        assertArrayEquals(body, Zstd.decompress(written, body.length));
    }

    @Test
    void no_codec_stamps_nothing_and_passes_the_body_through() throws Exception {
        Map<String, String> out = new HashMap<>();
        byte[] body = "arrow-ipc-body".getBytes(StandardCharsets.UTF_8);
        byte[] written = HttpServer.encodeArrowBody(responseStub(out),
                new ResponseEncoding(null, false), body, 3);

        assertTrue(out.isEmpty());
        assertArrayEquals(body, written);
    }

    /** Minimal {@link HttpServletResponse} recording {@code setHeader} calls. */
    private static HttpServletResponse responseStub(Map<String, String> sink) {
        return (HttpServletResponse) Proxy.newProxyInstance(
                ResponseEncodingTest.class.getClassLoader(),
                new Class<?>[]{HttpServletResponse.class},
                (proxy, method, args) -> {
                    if ("setHeader".equals(method.getName()) && args != null && args.length == 2) {
                        sink.put((String) args[0], (String) args[1]);
                        return null;
                    }
                    return switch (method.getName()) {
                        case "hashCode" -> 0;
                        case "toString" -> "stub";
                        case "equals"   -> proxy == args[0];
                        default         -> null;
                    };
                });
    }
}
