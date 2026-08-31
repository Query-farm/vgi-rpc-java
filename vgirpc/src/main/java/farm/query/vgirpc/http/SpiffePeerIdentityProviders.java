// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0
package farm.query.vgirpc.http;

import farm.query.vgirpc.identity.IdentityAssurance;
import farm.query.vgirpc.identity.PeerIdentity;
import farm.query.vgirpc.identity.PeerIdentityProvider;
import farm.query.vgirpc.identity.PeerIdentityRejectedException;
import farm.query.vgirpc.identity.PeerIdentityResult;
import farm.query.vgirpc.identity.PeerIdentityStatus;
import farm.query.vgirpc.identity.PeerResolutionContext;
import farm.query.vgirpc.identity.PeerSubjectKind;
import farm.query.vgirpc.identity.SubjectStability;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Trusted-HTTP-proxy SPIFFE evidence providers.
 *
 * <p>Every provider requires exact immediate-peer values and produces only
 * {@link IdentityAssurance#CONFIGURED_PROXY} evidence. The adjacent proxy must replace all
 * configured identity headers and the backend must be unreachable around that proxy.</p>
 */
public final class SpiffePeerIdentityProviders {
    private static final String PROVIDER = "spiffe";
    private static final int DEFAULT_MAX_HEADER_BYTES = 16_384;
    private static final Pattern TRUST_DOMAIN =
            Pattern.compile("[a-z0-9](?:[a-z0-9._-]{0,253}[a-z0-9])?");
    private static final Pattern PATH =
            Pattern.compile("/(?:[A-Za-z0-9._-]+)(?:/[A-Za-z0-9._-]+)*");
    private static final Pattern XFCC_KEY = Pattern.compile("[A-Za-z][A-Za-z0-9_-]*");
    private static final Pattern SHA256 = Pattern.compile("[0-9A-Fa-f]{64}");
    private static final Set<String> XFCC_FIELDS =
            Set.of("by", "hash", "cert", "chain", "subject", "uri", "dns", "issuer");
    private static final Set<String> XFCC_MULTI_FIELDS = Set.of("by", "uri", "dns");
    private static final Set<String> XFCC_PERCENT_FIELDS = Set.of("by", "uri", "cert", "chain");

    private SpiffePeerIdentityProviders() {}

    /** Strict X.509-SVID header provider with an explicit positive verification header. */
    public static PeerIdentityProvider x509Header(
            Set<String> trustDomains,
            Set<String> trustedProxyAddresses,
            String certificateHeader,
            String verificationHeader,
            String verificationValue,
            int maxHeaderBytes) {
        Config config = config(trustDomains, trustedProxyAddresses);
        requireHeader(certificateHeader, "certificateHeader");
        requireHeader(verificationHeader, "verificationHeader");
        if (certificateHeader.equalsIgnoreCase(verificationHeader)
                || containsControl(verificationValue) || maxHeaderBytes <= 0) {
            throw new IllegalArgumentException("distinct certificate/verification headers and a positive size limit are required");
        }
        return certificateProvider(config, certificateHeader, verificationHeader,
                verificationValue, maxHeaderBytes, "verified_certificate_header");
    }

    /** nginx mTLS evidence using {@code $ssl_client_escaped_cert} and {@code $ssl_client_verify}. */
    public static PeerIdentityProvider nginx(Set<String> trustDomains, Set<String> trustedProxyAddresses) {
        return nginx(trustDomains, trustedProxyAddresses, "X-SSL-Client-Cert",
                "X-SSL-Client-Verify", DEFAULT_MAX_HEADER_BYTES);
    }

    /** Configurable nginx mTLS header profile. */
    public static PeerIdentityProvider nginx(Set<String> trustDomains, Set<String> trustedProxyAddresses,
            String certificateHeader, String verificationHeader, int maxHeaderBytes) {
        return namedCertificate(trustDomains, trustedProxyAddresses, certificateHeader,
                verificationHeader, "SUCCESS", "nginx_mtls", maxHeaderBytes);
    }

    /** Azure Application Gateway strict-mode mTLS server-variable evidence. */
    public static PeerIdentityProvider azureApplicationGateway(
            Set<String> trustDomains, Set<String> trustedProxyAddresses) {
        return azureApplicationGateway(trustDomains, trustedProxyAddresses, "X-Client-Certificate",
                "X-Client-Certificate-Verification", DEFAULT_MAX_HEADER_BYTES);
    }

    /** Configurable Azure Application Gateway mTLS header profile. */
    public static PeerIdentityProvider azureApplicationGateway(
            Set<String> trustDomains, Set<String> trustedProxyAddresses,
            String certificateHeader, String verificationHeader, int maxHeaderBytes) {
        return namedCertificate(trustDomains, trustedProxyAddresses, certificateHeader,
                verificationHeader, "SUCCESS", "azure_application_gateway_mtls_strict", maxHeaderBytes);
    }

    /**
     * AWS ALB verify-mode evidence.
     *
     * <p>ALB has no per-request verified boolean. Listener verify mode, header replacement,
     * backend isolation, and the configured trust store are therefore operator-enforced parts
     * of this provider's trust boundary. Passthrough mode is not valid for this adapter.</p>
     */
    public static PeerIdentityProvider awsAlb(
            Set<String> trustDomains, Set<String> trustedProxyAddresses) {
        return awsAlb(trustDomains, trustedProxyAddresses, "X-Amzn-Mtls-Clientcert-Leaf",
                DEFAULT_MAX_HEADER_BYTES);
    }

    /** Configurable AWS ALB verify-mode leaf header profile. */
    public static PeerIdentityProvider awsAlb(Set<String> trustDomains, Set<String> trustedProxyAddresses,
            String leafHeader, int maxHeaderBytes) {
        Config config = config(trustDomains, trustedProxyAddresses);
        requireHeader(leafHeader, "leafHeader");
        if (maxHeaderBytes <= 0) throw new IllegalArgumentException("maxHeaderBytes must be positive");
        return certificateProvider(config, leafHeader, null, "", maxHeaderBytes, "aws_alb_mtls_verify");
    }

    /** GCP Application Load Balancer frontend-mTLS custom-header evidence. */
    public static PeerIdentityProvider gcpLoadBalancer(
            Set<String> trustDomains, Set<String> trustedProxyAddresses) {
        return gcpLoadBalancer(trustDomains, trustedProxyAddresses, "X-Client-Cert-Spiffe-Id",
                "X-Client-Cert-Present", "X-Client-Cert-Chain-Verified", "X-Client-Cert-Error");
    }

    /** Configurable GCP frontend-mTLS custom-header profile. */
    public static PeerIdentityProvider gcpLoadBalancer(
            Set<String> trustDomains, Set<String> trustedProxyAddresses, String spiffeIdHeader,
            String presentHeader, String chainVerifiedHeader, String errorHeader) {
        Config config = config(trustDomains, trustedProxyAddresses);
        List<String> headers = List.of(spiffeIdHeader, presentHeader, chainVerifiedHeader, errorHeader);
        headers.forEach(header -> requireHeader(header, "GCP header"));
        if (headers.stream().map(name -> name.toLowerCase(Locale.ROOT)).distinct().count() != headers.size()) {
            throw new IllegalArgumentException("GCP mTLS header names must be distinct");
        }
        return provider(context -> {
            if (!ExactIpAddresses.contains(config.proxies(), context.immediatePeer())) {
                return result(PeerIdentityStatus.UNTRUSTED_PROXY);
            }
            try {
                String present = context.header(presentHeader);
                String verified = context.header(chainVerifiedHeader);
                String spiffeId = context.header(spiffeIdHeader);
                String error = context.header(errorHeader);
                if ("false".equals(present) && (verified == null || "false".equals(verified)) && spiffeId == null) {
                    return result(PeerIdentityStatus.NO_MATCH);
                }
                if (!"true".equals(present) || !"true".equals(verified)
                        || (error != null && !error.isEmpty()) || spiffeId == null) {
                    return result(PeerIdentityStatus.INVALID);
                }
                SpiffeId id = parseSpiffeId(spiffeId, config.domains());
                return PeerIdentityResult.available(identity(id, "gcp_load_balancer_mtls", context,
                        Map.of("client_certificate_present", true,
                                "client_certificate_chain_verified", true)));
            } catch (IllegalArgumentException | PeerIdentityRejectedException e) {
                return result(PeerIdentityStatus.INVALID);
            }
        });
    }

    /** Strict Envoy SANITIZE_SET text-format XFCC evidence. */
    public static PeerIdentityProvider envoyXfcc(
            Set<String> trustDomains, Set<String> trustedProxyAddresses) {
        return envoyXfcc(trustDomains, trustedProxyAddresses, "X-Forwarded-Client-Cert",
                DEFAULT_MAX_HEADER_BYTES);
    }

    /** Configurable strict Envoy SANITIZE_SET XFCC profile. */
    public static PeerIdentityProvider envoyXfcc(Set<String> trustDomains,
            Set<String> trustedProxyAddresses, String header, int maxHeaderBytes) {
        Config config = config(trustDomains, trustedProxyAddresses);
        requireHeader(header, "header");
        if (maxHeaderBytes <= 0) throw new IllegalArgumentException("maxHeaderBytes must be positive");
        return provider(context -> {
            if (!ExactIpAddresses.contains(config.proxies(), context.immediatePeer())) {
                return result(PeerIdentityStatus.UNTRUSTED_PROXY);
            }
            try {
                String raw = context.header(header);
                if (raw == null) return result(PeerIdentityStatus.NO_MATCH);
                Map<String, List<String>> fields = parseSingleXfcc(raw, maxHeaderBytes);
                List<String> uris = fields.getOrDefault("uri", List.of());
                List<String> hashes = fields.getOrDefault("hash", List.of());
                if (uris.size() != 1 || hashes.size() != 1 || !SHA256.matcher(hashes.getFirst()).matches()) {
                    return result(PeerIdentityStatus.INVALID);
                }
                SpiffeId id = parseSpiffeId(uris.getFirst(), config.domains());
                Map<String, Object> attributes = new LinkedHashMap<>();
                attributes.put("certificate_sha256", hashes.getFirst().toLowerCase(Locale.ROOT));
                if (fields.containsKey("by")) attributes.put("proxy_identities", List.copyOf(fields.get("by")));
                return PeerIdentityResult.available(identity(id, "envoy_xfcc_sanitize_set", context, attributes));
            } catch (IllegalArgumentException | PeerIdentityRejectedException e) {
                return result(PeerIdentityStatus.INVALID);
            }
        });
    }

    /** Validate a canonical workload SPIFFE ID and return its trust domain. */
    public static String validateSpiffeId(String value, Set<String> trustDomains) {
        return parseSpiffeId(value, Set.copyOf(trustDomains)).trustDomain();
    }

    private static PeerIdentityProvider namedCertificate(
            Set<String> domains, Set<String> proxies, String certificateHeader,
            String verificationHeader, String verificationValue, String evidenceSource, int maxHeaderBytes) {
        Config config = config(domains, proxies);
        requireHeader(certificateHeader, "certificateHeader");
        requireHeader(verificationHeader, "verificationHeader");
        if (certificateHeader.equalsIgnoreCase(verificationHeader) || maxHeaderBytes <= 0) {
            throw new IllegalArgumentException("distinct headers and a positive maxHeaderBytes are required");
        }
        return certificateProvider(config, certificateHeader, verificationHeader, verificationValue,
                maxHeaderBytes, evidenceSource);
    }

    private static PeerIdentityProvider certificateProvider(
            Config config, String certificateHeader, String verificationHeader,
            String verificationValue, int maxHeaderBytes, String evidenceSource) {
        return provider(context -> {
            if (!ExactIpAddresses.contains(config.proxies(), context.immediatePeer())) {
                return result(PeerIdentityStatus.UNTRUSTED_PROXY);
            }
            try {
                String raw = context.header(certificateHeader);
                if (raw == null) return result(PeerIdentityStatus.NO_MATCH);
                if (!isAscii(raw) || utf8Length(raw) > maxHeaderBytes) return result(PeerIdentityStatus.INVALID);
                if (verificationHeader != null && !verificationValue.equals(context.header(verificationHeader))) {
                    return result(PeerIdentityStatus.INVALID);
                }
                String pem = strictPercentDecode(raw, true);
                if (!isAscii(pem) || utf8Length(pem) > maxHeaderBytes
                        || count(pem, "-----BEGIN CERTIFICATE-----") != 1
                        || count(pem, "-----END CERTIFICATE-----") != 1
                        || !pem.strip().endsWith("-----END CERTIFICATE-----")) {
                    return result(PeerIdentityStatus.INVALID);
                }
                X509Certificate certificate = parseCertificate(pem);
                SpiffeId id = validateCertificate(certificate, config.domains());
                return PeerIdentityResult.available(identity(id, evidenceSource, context, Map.of()));
            } catch (IllegalArgumentException | PeerIdentityRejectedException | CertificateException e) {
                return result(PeerIdentityStatus.INVALID);
            }
        });
    }

    private static X509Certificate parseCertificate(String pem) throws CertificateException {
        CertificateFactory factory = CertificateFactory.getInstance("X.509");
        return (X509Certificate) factory.generateCertificate(
                new ByteArrayInputStream(pem.getBytes(StandardCharsets.US_ASCII)));
    }

    private static SpiffeId validateCertificate(X509Certificate certificate, Set<String> trustDomains)
            throws CertificateException {
        certificate.checkValidity();
        Set<String> critical = certificate.getCriticalExtensionOIDs();
        Collection<List<?>> sans = certificate.getSubjectAlternativeNames();
        if (sans == null) throw new CertificateException("X.509-SVID requires SAN");
        List<String> uriSans = new ArrayList<>();
        for (List<?> san : sans) {
            if (san.size() >= 2 && Integer.valueOf(6).equals(san.get(0)) && san.get(1) instanceof String uri) {
                uriSans.add(uri);
            }
        }
        if (uriSans.size() != 1) throw new CertificateException("X.509-SVID requires exactly one URI SAN");
        if (certificate.getSubjectX500Principal().getName().isEmpty()
                && (critical == null || !critical.contains("2.5.29.17"))) {
            throw new CertificateException("subjectless X.509-SVID requires critical SAN");
        }
        if (certificate.getBasicConstraints() >= 0) throw new CertificateException("X.509-SVID leaf cannot be a CA");
        boolean[] usage = certificate.getKeyUsage();
        if (usage == null || usage.length == 0 || !usage[0]
                || (usage.length > 5 && usage[5]) || (usage.length > 6 && usage[6])
                || critical == null || !critical.contains("2.5.29.15")) {
            throw new CertificateException("invalid X.509-SVID key usage");
        }
        List<String> extended = certificate.getExtendedKeyUsage();
        if (extended != null && (!extended.contains("1.3.6.1.5.5.7.3.1")
                || !extended.contains("1.3.6.1.5.5.7.3.2"))) {
            throw new CertificateException("invalid X.509-SVID extended key usage");
        }
        try {
            return parseSpiffeId(uriSans.getFirst(), trustDomains);
        } catch (IllegalArgumentException e) {
            throw new CertificateException("invalid SPIFFE ID", e);
        }
    }

    private static SpiffeId parseSpiffeId(String value, Set<String> trustDomains) {
        if (value == null || value.isEmpty() || !isAscii(value) || utf8Length(value) > 2048 || value.indexOf('%') >= 0) {
            throw new IllegalArgumentException("SPIFFE ID is empty, non-ASCII, encoded, or oversized");
        }
        URI uri;
        try {
            uri = URI.create(value);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("invalid SPIFFE ID", e);
        }
        String authority = uri.getRawAuthority();
        String path = uri.getRawPath();
        if (!"spiffe".equals(uri.getScheme()) || authority == null || uri.getRawUserInfo() != null
                || uri.getPort() != -1 || uri.getRawQuery() != null || uri.getRawFragment() != null
                || !TRUST_DOMAIN.matcher(authority).matches() || !PATH.matcher(path).matches()) {
            throw new IllegalArgumentException("SPIFFE ID is not canonical");
        }
        for (String segment : path.substring(1).split("/")) {
            if (segment.equals(".") || segment.equals("..")) throw new IllegalArgumentException("SPIFFE ID has dot segment");
        }
        if (!trustDomains.contains(authority)) throw new IllegalArgumentException("SPIFFE trust domain is not allowed");
        return new SpiffeId(value, authority);
    }

    private static Map<String, List<String>> parseSingleXfcc(String raw, int maxBytes) {
        if (!isAscii(raw) || utf8Length(raw) > maxBytes || containsControl(raw)) {
            throw new IllegalArgumentException("invalid XFCC bytes");
        }
        List<String> elements = splitXfcc(raw, ',');
        if (elements.size() != 1 || elements.getFirst().isBlank()) {
            throw new IllegalArgumentException("XFCC must contain one SANITIZE_SET element");
        }
        Map<String, List<String>> fields = new HashMap<>();
        for (String rawPair : splitXfcc(elements.getFirst(), ';')) {
            String pair = rawPair.strip();
            int equals = pair.indexOf('=');
            if (equals <= 0) throw new IllegalArgumentException("malformed XFCC field");
            String rawKey = pair.substring(0, equals).strip();
            String key = rawKey.toLowerCase(Locale.ROOT);
            if (!XFCC_KEY.matcher(rawKey).matches() || !XFCC_FIELDS.contains(key)) {
                throw new IllegalArgumentException("unsupported XFCC field");
            }
            String value = xfccValue(pair.substring(equals + 1).strip());
            if (XFCC_PERCENT_FIELDS.contains(key)) value = strictPercentDecode(value, false);
            if (!XFCC_MULTI_FIELDS.contains(key) && fields.containsKey(key)) {
                throw new IllegalArgumentException("duplicate XFCC singleton");
            }
            fields.computeIfAbsent(key, ignored -> new ArrayList<>()).add(value);
        }
        return fields;
    }

    private static List<String> splitXfcc(String value, char delimiter) {
        List<String> parts = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean quoted = false;
        boolean escaped = false;
        for (int i = 0; i < value.length(); i++) {
            char character = value.charAt(i);
            if (escaped) {
                if (character != '"' && character != '\\') throw new IllegalArgumentException("unsupported XFCC escape");
                current.append(character);
                escaped = false;
            } else if (quoted && character == '\\') {
                escaped = true;
            } else if (character == '"') {
                quoted = !quoted;
                current.append(character);
            } else if (character == delimiter && !quoted) {
                parts.add(current.toString());
                current.setLength(0);
            } else {
                current.append(character);
            }
        }
        if (quoted || escaped) throw new IllegalArgumentException("unterminated XFCC quote");
        parts.add(current.toString());
        return parts;
    }

    private static String xfccValue(String value) {
        if (value.startsWith("\"") || value.endsWith("\"")) {
            if (value.length() < 2 || value.charAt(0) != '"' || value.charAt(value.length() - 1) != '"') {
                throw new IllegalArgumentException("malformed XFCC quoted value");
            }
            value = value.substring(1, value.length() - 1);
        } else if (value.indexOf(',') >= 0 || value.indexOf(';') >= 0 || value.indexOf('=') >= 0) {
            throw new IllegalArgumentException("unquoted XFCC delimiter");
        }
        if (value.isEmpty()) throw new IllegalArgumentException("empty XFCC value");
        return value;
    }

    private static String strictPercentDecode(String value, boolean allowLineBreaks) {
        byte[] output = new byte[value.length()];
        int length = 0;
        for (int i = 0; i < value.length();) {
            char character = value.charAt(i);
            if (character == '%') {
                if (i + 2 >= value.length()) throw new IllegalArgumentException("invalid percent escape");
                int high = Character.digit(value.charAt(i + 1), 16);
                int low = Character.digit(value.charAt(i + 2), 16);
                if (high < 0 || low < 0) throw new IllegalArgumentException("invalid percent escape");
                output[length++] = (byte) ((high << 4) | low);
                i += 3;
            } else {
                if (character > 0x7f) throw new IllegalArgumentException("non-ASCII encoded header");
                output[length++] = (byte) character;
                i++;
            }
        }
        try {
            String decoded = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(output, 0, length)).toString();
            if (decoded.codePoints().anyMatch(code -> (code <= 0x1f && !(allowLineBreaks && (code == '\r' || code == '\n')))
                    || code == 0x7f)) {
                throw new IllegalArgumentException("decoded header contains controls");
            }
            return decoded;
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException("invalid UTF-8 percent escape", e);
        }
    }

    private static PeerIdentity identity(
            SpiffeId id, String evidenceSource, PeerResolutionContext context, Map<String, Object> attributes) {
        return new PeerIdentity(PROVIDER, evidenceSource, IdentityAssurance.CONFIGURED_PROXY,
                "spiffe://" + id.trustDomain(), "http", PeerSubjectKind.WORKLOAD, id.value(),
                SubjectStability.STABLE, true, attributes, Map.of(), false,
                context.assertedPeer(), context.immediatePeer());
    }

    private static Config config(Set<String> trustDomains, Set<String> proxies) {
        if (trustDomains == null || proxies == null || trustDomains.isEmpty() || proxies.isEmpty()) {
            throw new IllegalArgumentException("trustDomains and trustedProxyAddresses must not be empty");
        }
        Set<String> domains = Set.copyOf(trustDomains);
        Set<String> copiedProxies = ExactIpAddresses.trusted(proxies);
        if (domains.stream().anyMatch(domain -> domain == null || !TRUST_DOMAIN.matcher(domain).matches())) {
            throw new IllegalArgumentException("invalid SPIFFE trust domain");
        }
        return new Config(domains, copiedProxies);
    }

    private static PeerIdentityProvider provider(Resolver resolver) {
        return new PeerIdentityProvider() {
            @Override public String provider() { return PROVIDER; }
            @Override public PeerIdentityResult resolve(PeerResolutionContext context) { return resolver.resolve(context); }
        };
    }

    private static PeerIdentityResult result(PeerIdentityStatus status) {
        return new PeerIdentityResult(PROVIDER, status);
    }

    private static void requireHeader(String value, String name) {
        if (value == null || value.isBlank() || containsControl(value)) {
            throw new IllegalArgumentException(name + " is invalid");
        }
    }

    private static boolean containsControl(String value) {
        return value == null || value.codePoints().anyMatch(code -> code <= 0x1f || code == 0x7f);
    }

    private static boolean isAscii(String value) {
        return value.chars().allMatch(character -> character <= 0x7f);
    }

    private static int utf8Length(String value) {
        return value.getBytes(StandardCharsets.UTF_8).length;
    }

    private static int count(String text, String needle) {
        int found = 0;
        for (int at = 0; (at = text.indexOf(needle, at)) >= 0; at += needle.length()) found++;
        return found;
    }

    private record Config(Set<String> domains, Set<String> proxies) {}
    private record SpiffeId(String value, String trustDomain) {}
    @FunctionalInterface private interface Resolver { PeerIdentityResult resolve(PeerResolutionContext context); }
}
