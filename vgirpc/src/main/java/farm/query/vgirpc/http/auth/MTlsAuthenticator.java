// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http.auth;

import farm.query.vgirpc.AuthContext;
import farm.query.vgirpc.http.AuthException;
import farm.query.vgirpc.http.Authenticator;
import jakarta.servlet.http.HttpServletRequest;

import java.io.ByteArrayInputStream;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Authenticators that extract client identity from a reverse proxy that has
 * already terminated mTLS. Three variants matching the Python reference:
 *
 * <ul>
 *   <li>{@link #xfcc(String)} — Envoy {@code x-forwarded-client-cert}.
 *       No cert parsing required; identity comes from the {@code Subject=}
 *       / {@code URI=} fields.</li>
 *   <li>{@link #byFingerprint(Map, String, String)} — PEM certificate in a
 *       proxy header; SHA-256 (or configured algorithm) hex lookup into a
 *       static map.</li>
 *   <li>{@link #bySubjectCn(String, Set, String)} — PEM certificate in a
 *       proxy header; principal = CN, with optional allow-list.</li>
 * </ul>
 *
 * <p><strong>Header spoofing risk.</strong> The upstream proxy MUST strip any
 * client-supplied {@code X-SSL-Client-Cert} / {@code x-forwarded-client-cert}
 * headers before forwarding; these authenticators trust the header
 * unconditionally.</p>
 */
public final class MTlsAuthenticator {

    private MTlsAuthenticator() {}

    /** XFCC flavour: no PEM parsing, principal from {@code Subject=CN=...} or {@code URI=}. */
    public static Authenticator xfcc(String domain) {
        String dom = domain != null ? domain : "mtls";
        return request -> {
            String header = request.getHeader("x-forwarded-client-cert");
            if (header == null || header.isEmpty()) {
                throw new AuthException("Missing x-forwarded-client-cert header");
            }
            List<XfccElement> elements = XfccParser.parse(header);
            if (elements.isEmpty()) {
                throw new AuthException("Empty x-forwarded-client-cert header");
            }
            XfccElement element = elements.get(0);
            String principal = XfccParser.extractCn(element.subject());
            if (principal.isEmpty() && element.uri() != null) principal = element.uri();
            Map<String, Object> claims = new LinkedHashMap<>();
            if (element.hash() != null) claims.put("hash", element.hash());
            if (element.subject() != null) claims.put("subject", element.subject());
            if (element.uri() != null) claims.put("uri", element.uri());
            if (!element.dns().isEmpty()) claims.put("dns", element.dns());
            if (element.by() != null) claims.put("by", element.by());
            return new AuthContext(dom, true, principal, claims);
        };
    }

    /** PEM fingerprint lookup. Fingerprints must be lowercase hex without colons. */
    public static Authenticator byFingerprint(Map<String, AuthContext> fingerprints,
                                               String header, String algorithm) {
        Objects.requireNonNull(fingerprints, "fingerprints");
        String hdr = header != null ? header : "X-SSL-Client-Cert";
        String algo = algorithm != null ? algorithm : "SHA-256";
        return request -> {
            X509Certificate cert = parseHeaderCert(request, hdr);
            byte[] fp;
            try {
                MessageDigest md = MessageDigest.getInstance(algo);
                fp = md.digest(cert.getEncoded());
            } catch (Exception e) {
                throw new AuthException("Fingerprint computation failed: " + e.getMessage());
            }
            String hex = toHex(fp);
            AuthContext ctx = fingerprints.get(hex);
            if (ctx == null) {
                throw new AuthException("Unknown certificate fingerprint: " + hex);
            }
            return ctx;
        };
    }

    /** PEM subject CN with optional allow-list. */
    public static Authenticator bySubjectCn(String header, Set<String> allowedSubjects, String domain) {
        String hdr = header != null ? header : "X-SSL-Client-Cert";
        String dom = domain != null ? domain : "mtls";
        return request -> {
            X509Certificate cert = parseHeaderCert(request, hdr);
            String dn = cert.getSubjectX500Principal().getName(); // RFC 2253 / RFC 4514
            String cn = XfccParser.extractCn(dn);
            if (allowedSubjects != null && !allowedSubjects.contains(cn)) {
                throw new AuthException("Subject CN '" + cn + "' not in allowed subjects");
            }
            Map<String, Object> claims = new LinkedHashMap<>();
            claims.put("subject_dn", dn);
            claims.put("serial", cert.getSerialNumber().toString(16));
            claims.put("not_valid_after", cert.getNotAfter().toInstant().toString());
            return new AuthContext(dom, true, cn, claims);
        };
    }

    // --- PEM parsing helpers ---------------------------------------------

    private static X509Certificate parseHeaderCert(HttpServletRequest req, String header) throws AuthException {
        String raw = req.getHeader(header);
        if (raw == null || raw.isEmpty()) {
            throw new AuthException("Missing " + header + " header");
        }
        String pem = URLDecoder.decode(raw, StandardCharsets.UTF_8);
        if (!pem.startsWith("-----BEGIN CERTIFICATE-----")) {
            throw new AuthException("Header value is not a PEM certificate");
        }
        try {
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            return (X509Certificate) cf.generateCertificate(
                    new ByteArrayInputStream(pem.getBytes(StandardCharsets.UTF_8)));
        } catch (CertificateException e) {
            throw new AuthException("Failed to parse PEM certificate: " + e.getMessage());
        }
    }

    private static String toHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(Character.forDigit((b >> 4) & 0xF, 16));
            sb.append(Character.forDigit(b & 0xF, 16));
        }
        return sb.toString();
    }
}
