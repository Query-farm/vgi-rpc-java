// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0
package farm.query.vgirpc.http;

import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import farm.query.vgirpc.identity.IdentityAssurance;
import farm.query.vgirpc.identity.PeerIdentity;
import farm.query.vgirpc.identity.PeerIdentityProvider;
import farm.query.vgirpc.identity.PeerIdentityRejectedException;
import farm.query.vgirpc.identity.PeerIdentityResult;
import farm.query.vgirpc.identity.PeerIdentityStatus;
import farm.query.vgirpc.identity.PeerResolutionContext;
import farm.query.vgirpc.identity.PeerSubjectKind;
import farm.query.vgirpc.identity.SubjectStability;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Strict Tailscale Serve HTTP identity and capability evidence. */
public final class TailscalePeerIdentityProviders {
    private static final String PROVIDER = "tailscale";
    private static final int MAX_HEADER_BYTES = 65_536;
    private static final int MAX_JSON_VALUES = 4_096;
    private static final Pattern ENCODED_WORD = Pattern.compile("^=\\?utf-8\\?q\\?([^?]*)\\?=$",
            Pattern.CASE_INSENSITIVE);
    private static final ObjectMapper JSON = new ObjectMapper(
            com.fasterxml.jackson.core.JsonFactory.builder()
                    .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
                    .streamReadConstraints(StreamReadConstraints.builder()
                            .maxDocumentLength(MAX_HEADER_BYTES).maxNestingDepth(16)
                            .maxStringLength(MAX_HEADER_BYTES).maxNumberLength(256).build())
                    .build());

    private TailscalePeerIdentityProviders() {}

    /**
     * Trust Tailscale Serve headers only from exact immediate proxy peers.
     * Funnel must not be used: it does not establish a Tailnet caller identity.
     */
    public static PeerIdentityProvider serve(String issuer, Set<String> trustedProxyAddresses) {
        if (issuer == null || issuer.isBlank() || containsControl(issuer)) {
            throw new IllegalArgumentException("issuer and exact trusted proxy addresses are required");
        }
        Set<String> proxies = ExactIpAddresses.trusted(trustedProxyAddresses);
        return new PeerIdentityProvider() {
            @Override public String provider() { return PROVIDER; }

            @Override public PeerIdentityResult resolve(PeerResolutionContext context) {
                if (!ExactIpAddresses.contains(proxies, context.immediatePeer())) {
                    return result(PeerIdentityStatus.UNTRUSTED_PROXY);
                }
                try {
                    String login = context.header("Tailscale-User-Login");
                    String displayName = context.header("Tailscale-User-Name");
                    String rawCapabilities = context.header("Tailscale-App-Capabilities");
                    String funnel = context.header("Tailscale-Funnel-Request");
                    if ("?1".equals(funnel)) {
                        return result(PeerIdentityStatus.NOT_APPLICABLE);
                    }
                    if (funnel != null) return result(PeerIdentityStatus.INVALID);
                    if (login == null && rawCapabilities == null) {
                        return result(displayName == null ? PeerIdentityStatus.NO_MATCH : PeerIdentityStatus.INVALID);
                    }
                    Map<String, Object> capabilities = parseCapabilities(rawCapabilities);
                    if (login == null && capabilities.isEmpty()) return result(PeerIdentityStatus.NO_MATCH);
                    Map<String, Object> attributes = new LinkedHashMap<>();
                    PeerSubjectKind kind = PeerSubjectKind.UNKNOWN;
                    SubjectStability stability = SubjectStability.NONE;
                    String subject = null;
                    boolean verified = false;
                    if (login != null) {
                        login = decodeHeader(login);
                        if (login.isBlank()) return result(PeerIdentityStatus.INVALID);
                        kind = PeerSubjectKind.USER;
                        stability = SubjectStability.LOGIN;
                        subject = "login:" + login;
                        verified = true;
                        attributes.put("user_login", login);
                        if (displayName != null) attributes.put("user_display_name", decodeHeader(displayName));
                    } else if (displayName != null) {
                        return result(PeerIdentityStatus.INVALID);
                    }
                    PeerIdentity identity = new PeerIdentity(PROVIDER, "serve_proxy",
                            IdentityAssurance.CONFIGURED_PROXY, issuer, "http", kind, subject,
                            stability, verified, attributes, capabilities, rawCapabilities != null,
                            null, context.immediatePeer());
                    return PeerIdentityResult.available(identity);
                } catch (IOException | IllegalArgumentException | PeerIdentityRejectedException e) {
                    return result(PeerIdentityStatus.INVALID);
                }
            }
        };
    }

    private static Map<String, Object> parseCapabilities(String raw) throws IOException {
        if (raw == null) return Map.of();
        String decoded = decodeHeader(raw, MAX_HEADER_BYTES);
        JsonNode root = JSON.readTree(decoded);
        if (root == null || !root.isObject()) throw new IllegalArgumentException("capabilities must be an object");
        requireJsonValueLimit(root);
        Map<String, Object> capabilities = new LinkedHashMap<>();
        var fields = root.fields();
        int count = 0;
        while (fields.hasNext()) {
            var field = fields.next();
            if (++count > 1024 || !field.getValue().isArray()) {
                throw new IllegalArgumentException("capability values must be arrays");
            }
            capabilities.put(field.getKey(), JSON.convertValue(field.getValue(), Object.class));
        }
        return capabilities;
    }

    private static String decodeHeader(String raw) throws CharacterCodingException {
        return decodeHeader(raw, 4096);
    }

    private static String decodeHeader(String raw, int maxBytes) throws CharacterCodingException {
        if (utf8Length(raw) > maxBytes || containsControl(raw)) {
            throw new IllegalArgumentException("invalid Tailscale identity header");
        }
        Matcher matcher = ENCODED_WORD.matcher(raw);
        if (!matcher.matches()) return raw;
        String encoded = matcher.group(1).replace('_', ' ');
        byte[] bytes = new byte[encoded.length()];
        int length = 0;
        for (int index = 0; index < encoded.length();) {
            char character = encoded.charAt(index);
            if (character == '=') {
                if (index + 2 >= encoded.length()) throw new IllegalArgumentException("invalid RFC 2047 escape");
                int high = Character.digit(encoded.charAt(index + 1), 16);
                int low = Character.digit(encoded.charAt(index + 2), 16);
                if (high < 0 || low < 0) throw new IllegalArgumentException("invalid RFC 2047 escape");
                bytes[length++] = (byte) ((high << 4) | low);
                index += 3;
            } else {
                if (character > 0x7f) throw new IllegalArgumentException("invalid RFC 2047 bytes");
                bytes[length++] = (byte) character;
                index++;
            }
        }
        String decoded = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes, 0, length)).toString();
        if (utf8Length(decoded) > maxBytes || containsControl(decoded)) {
            throw new IllegalArgumentException("decoded identity contains controls or is oversized");
        }
        return decoded;
    }

    private static void requireJsonValueLimit(JsonNode root) {
        Deque<JsonNode> pending = new ArrayDeque<>();
        pending.add(root);
        int count = 0;
        while (!pending.isEmpty()) {
            JsonNode node = pending.removeFirst();
            if (++count > MAX_JSON_VALUES) throw new IllegalArgumentException("capabilities contain too many values");
            if (node.isContainerNode()) node.elements().forEachRemaining(pending::addLast);
        }
    }

    private static PeerIdentityResult result(PeerIdentityStatus status) {
        return new PeerIdentityResult(PROVIDER, status);
    }

    private static boolean containsControl(String value) {
        return value.codePoints().anyMatch(code -> code <= 0x1f || code == 0x7f);
    }

    private static int utf8Length(String value) throws CharacterCodingException {
        return StandardCharsets.UTF_8.newEncoder().onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT).encode(CharBuffer.wrap(value)).remaining();
    }
}
