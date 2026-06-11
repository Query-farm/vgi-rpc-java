// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http.auth;

import farm.query.vgirpc.AuthContext;
import farm.query.vgirpc.http.AuthException;
import farm.query.vgirpc.http.Authenticator;
import farm.query.vgirpc.http.HttpHeaders;
import farm.query.vgirpc.http.InvalidCredentials;
import farm.query.vgirpc.http.MissingCredentials;
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

    /**
     * XFCC flavour: no PEM parsing, principal from {@code Subject=CN=...} or {@code URI=}.
     *
     * <p>Uses the first element of the {@code x-forwarded-client-cert} header
     * (the certificate presented to the outermost proxy) and copies its
     * {@code hash} / {@code subject} / {@code uri} / {@code dns} / {@code by}
     * fields into the context claims.</p>
     *
     * @param domain the {@link AuthContext} domain label; {@code null} defaults to {@code "mtls"}
     * @return an authenticator deriving identity from the XFCC header
     */
    public static Authenticator xfcc(String domain) {
        String dom = domain != null ? domain : "mtls";
        return request -> {
            String header = request.getHeader(HttpHeaders.X_FORWARDED_CLIENT_CERT);
            if (header == null || header.isEmpty()) {
                throw new MissingCredentials("Missing " + HttpHeaders.X_FORWARDED_CLIENT_CERT + " header");
            }
            List<XfccElement> elements = XfccParser.parse(header);
            if (elements.isEmpty()) {
                throw new InvalidCredentials("Empty " + HttpHeaders.X_FORWARDED_CLIENT_CERT + " header");
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

    /**
     * PEM fingerprint lookup. Fingerprints must be lowercase hex without colons.
     *
     * @param fingerprints fingerprint → context map; certificates whose digest
     *        is absent from the map are rejected with {@link InvalidCredentials}
     * @param header name of the proxy header carrying the URL-encoded PEM
     *        certificate; {@code null} defaults to {@code X-SSL-Client-Cert}
     * @param algorithm {@link MessageDigest} algorithm used to fingerprint the
     *        DER-encoded certificate; {@code null} defaults to {@code SHA-256}
     * @return an authenticator that resolves the client certificate's digest in {@code fingerprints}
     */
    public static Authenticator byFingerprint(Map<String, AuthContext> fingerprints,
                                               String header, String algorithm) {
        Objects.requireNonNull(fingerprints, "fingerprints");
        String hdr = header != null ? header : HttpHeaders.X_SSL_CLIENT_CERT;
        String algo = algorithm != null ? algorithm : "SHA-256";
        return request -> {
            X509Certificate cert = parseHeaderCert(request, hdr);
            byte[] fp;
            try {
                MessageDigest md = MessageDigest.getInstance(algo);
                fp = md.digest(cert.getEncoded());
            } catch (Exception e) {
                throw new InvalidCredentials("Fingerprint computation failed: " + e.getMessage());
            }
            String hex = toHex(fp);
            AuthContext ctx = fingerprints.get(hex);
            if (ctx == null) {
                throw new InvalidCredentials("Unknown certificate fingerprint: " + hex);
            }
            return ctx;
        };
    }

    /**
     * PEM subject CN with optional allow-list. The principal is the
     * certificate's subject CN; {@code subject_dn}, {@code serial}, and
     * {@code not_valid_after} ride along as claims.
     *
     * @param header name of the proxy header carrying the URL-encoded PEM
     *        certificate; {@code null} defaults to {@code X-SSL-Client-Cert}
     * @param allowedSubjects CNs to accept; {@code null} accepts any CN,
     *        otherwise out-of-list CNs are rejected with {@link InvalidCredentials}
     * @param domain the {@link AuthContext} domain label; {@code null} defaults to {@code "mtls"}
     * @return an authenticator deriving identity from the certificate subject CN
     */
    public static Authenticator bySubjectCn(String header, Set<String> allowedSubjects, String domain) {
        String hdr = header != null ? header : HttpHeaders.X_SSL_CLIENT_CERT;
        String dom = domain != null ? domain : "mtls";
        return request -> {
            X509Certificate cert = parseHeaderCert(request, hdr);
            String dn = cert.getSubjectX500Principal().getName(); // RFC 2253 / RFC 4514
            String cn = XfccParser.extractCn(dn);
            if (allowedSubjects != null && !allowedSubjects.contains(cn)) {
                throw new InvalidCredentials("Subject CN '" + cn + "' not in allowed subjects");
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
            throw new MissingCredentials("Missing " + header + " header");
        }
        String pem = URLDecoder.decode(raw, StandardCharsets.UTF_8);
        if (!pem.startsWith("-----BEGIN CERTIFICATE-----")) {
            throw new InvalidCredentials("Header value is not a PEM certificate");
        }
        try {
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            return (X509Certificate) cf.generateCertificate(
                    new ByteArrayInputStream(pem.getBytes(StandardCharsets.UTF_8)));
        } catch (CertificateException e) {
            throw new InvalidCredentials("Failed to parse PEM certificate: " + e.getMessage());
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
