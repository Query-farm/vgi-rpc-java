// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

import farm.query.vgirpc.errors.ErrorDetail;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Protocol version negotiation, v1 ({@code tools/cross-port/specs/PROTOCOL_VERSIONS_V1.md} in
 * vgi-rpc-python).
 *
 * <p>Every server advertises, per protocol it gates, the inclusive window of
 * {@code major.minor} versions it accepts:</p>
 * <pre>VGI-Protocol-Versions: vgi.v3=3.4-3.4, vgi.secret.v1=1.0-1.0</pre>
 * <p>on every HTTP response, and as the {@value #KEY} metadata value on reflection responses,
 * {@code __transport_options__} replies and {@value #UNSUPPORTED} refusals on every transport.
 * A client keeps an {@link EndpointNegotiation} per endpoint, learns the server's windows from
 * whatever it sees first, and speaks the highest version both sides support.</p>
 *
 * <p>This class is pure: parsing, formatting and the negotiation state. The server and client
 * wiring live with the transports.</p>
 */
public final class ProtocolVersions {

    private ProtocolVersions() {}

    /** HTTP response header carrying the server's windows; on every response. */
    public static final String HEADER = "VGI-Protocol-Versions";
    /** HTTP request header carrying the client's own windows; informational. */
    public static final String CLIENT_HEADER = "VGI-Client-Protocol-Versions";
    /** Metadata key carrying the server's windows (reflection, refusals, transport options). */
    public static final String KEY = "vgi_rpc.protocol_versions";
    /** Request metadata key carrying the client's own windows; informational. */
    public static final String CLIENT_KEY = "vgi_rpc.protocol_versions_client";
    /** The {@code error_kind} of a refusal. */
    public static final String UNSUPPORTED = "protocol_version_unsupported";
    /** The pre-negotiation refusal kind, still recognised from older servers. */
    public static final String LEGACY_MISMATCH = "protocol_version_mismatch";
    /** The reflection protocol, exempt from the gate and always retry-safe. */
    public static final String REFLECTION = "vgi_rpc.Reflection.v1";

    private static final Pattern MAJOR_MINOR = Pattern.compile("^(0|[1-9]\\d*)\\.(0|[1-9]\\d*)$");
    private static final Pattern SEMVER = Pattern.compile("^(0|[1-9]\\d*)\\.(0|[1-9]\\d*)\\.(0|[1-9]\\d*)$");
    private static final Pattern NAME = Pattern.compile("^[A-Za-z0-9_.:/-]+$");
    private static final Pattern LEGACY_SERVER_VERSION =
            Pattern.compile("(?:Server:\\s*|server requires\\s+)(\\d+\\.\\d+)(?:\\.\\d+)?");

    /**
     * {@code MAJOR.MINOR} (a full {@code MAJOR.MINOR.PATCH} is accepted and its patch dropped).
     *
     * @param text the version text
     * @return {@code {major, minor}}
     * @throws IllegalArgumentException when not canonical
     */
    static int[] parseMajorMinor(String text) {
        String t = text.strip();
        Matcher m = MAJOR_MINOR.matcher(t);
        if (!m.matches()) m = SEMVER.matcher(t);
        if (!m.matches()) throw new IllegalArgumentException("not a MAJOR.MINOR version: '" + text + "'");
        try {
            return new int[] {Integer.parseInt(m.group(1)), Integer.parseInt(m.group(2))};
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("not a MAJOR.MINOR version: '" + text + "'");
        }
    }

    /**
     * Canonical semver {@code MAJOR.MINOR.PATCH} as three ints, or {@code null}.
     *
     * @param v the text
     * @return the parts, or {@code null} when not canonical
     */
    static int[] parseSemver(String v) {
        if (v == null) return null;
        Matcher m = SEMVER.matcher(v);
        if (!m.matches()) return null;
        try {
            return new int[] {Integer.parseInt(m.group(1)), Integer.parseInt(m.group(2)),
                    Integer.parseInt(m.group(3))};
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static int compare(int aMajor, int aMinor, int bMajor, int bMinor) {
        return aMajor != bMajor ? Integer.compare(aMajor, bMajor) : Integer.compare(aMinor, bMinor);
    }

    /**
     * An inclusive {@code min..max} window of {@code major.minor} versions.
     *
     * @param minMajor oldest accepted major
     * @param minMinor oldest accepted minor
     * @param maxMajor newest accepted major
     * @param maxMinor newest accepted minor
     */
    public record VersionRange(int minMajor, int minMinor, int maxMajor, int maxMinor) {

        /** Reject an inverted window. */
        public VersionRange {
            if (compare(minMajor, minMinor, maxMajor, maxMinor) > 0) {
                throw new IllegalArgumentException("protocol version range " + minMajor + "." + minMinor + "-"
                        + maxMajor + "." + maxMinor + " has min above max");
            }
        }

        /**
         * Parse {@code MIN-MAX} (each {@code MAJOR.MINOR}), or a single {@code MAJOR.MINOR}
         * meaning {@code V-V}.
         *
         * @param text the range text, e.g. {@code "3.4-3.4"}
         * @return the range
         * @throws IllegalArgumentException when the text is not a range
         */
        public static VersionRange parse(String text) {
            String t = text.strip();
            int dash = t.indexOf('-');
            int[] lo = parseMajorMinor(dash < 0 ? t : t.substring(0, dash));
            int[] hi = dash < 0 ? lo : parseMajorMinor(t.substring(dash + 1));
            return new VersionRange(lo[0], lo[1], hi[0], hi[1]);
        }

        /**
         * The single-version window of a semver's {@code major.minor}.
         *
         * @param version {@code MAJOR.MINOR.PATCH} (or {@code MAJOR.MINOR})
         * @return {@code V-V}
         */
        public static VersionRange exactly(String version) {
            int[] mm = parseMajorMinor(version);
            return new VersionRange(mm[0], mm[1], mm[0], mm[1]);
        }

        /**
         * Whether a {@code major.minor} lies inside the window.
         *
         * @param major the major
         * @param minor the minor
         * @return whether it is accepted
         */
        public boolean contains(int major, int minor) {
            return compare(minMajor, minMinor, major, minor) <= 0 && compare(major, minor, maxMajor, maxMinor) <= 0;
        }

        /** @return the newest version, {@code MAJOR.MINOR} */
        public String max() { return maxMajor + "." + maxMinor; }

        /** @return the oldest version, {@code MAJOR.MINOR} */
        public String min() { return minMajor + "." + minMinor; }

        /** Wire form: {@code MIN-MAX}, always both ends. */
        @Override
        public String toString() { return min() + "-" + max(); }
    }

    /**
     * Render {@code name=MIN-MAX[, name=MIN-MAX]...} in iteration order.
     *
     * @param ranges protocol name to window, in advertising order
     * @return the header / metadata value
     */
    public static String format(Map<String, VersionRange> ranges) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, VersionRange> e : ranges.entrySet()) {
            if (!sb.isEmpty()) sb.append(", ");
            sb.append(e.getKey()).append('=').append(e.getValue());
        }
        return sb.toString();
    }

    /**
     * Parse a {@code VGI-Protocol-Versions} value. Tolerant: a malformed entry is skipped, and
     * a repeated name keeps its first window.
     *
     * @param text the header or metadata value, or {@code null}
     * @return protocol name to window, in order; empty when absent or wholly unreadable
     */
    public static Map<String, VersionRange> parse(String text) {
        Map<String, VersionRange> out = new LinkedHashMap<>();
        if (text == null) return out;
        for (String entry : text.split(",")) {
            String e = entry.strip();
            int eq = e.indexOf('=');
            if (eq < 0) continue;
            String name = e.substring(0, eq).strip();
            if (!NAME.matcher(name).matches() || out.containsKey(name)) continue;
            try {
                out.put(name, VersionRange.parse(e.substring(eq + 1)));
            } catch (IllegalArgumentException ignored) {
                // skipped: an unreadable entry is "that protocol is unadvertised"
            }
        }
        return out;
    }

    /**
     * The highest {@code major.minor} inside both windows, or {@code null} when they are disjoint.
     *
     * @param a one window
     * @param b the other
     * @return {@code {major, minor}}, or {@code null}
     */
    public static int[] highestCommon(VersionRange a, VersionRange b) {
        int[] top = compare(a.maxMajor(), a.maxMinor(), b.maxMajor(), b.maxMinor()) <= 0
                ? new int[] {a.maxMajor(), a.maxMinor()} : new int[] {b.maxMajor(), b.maxMinor()};
        if (!a.contains(top[0], top[1]) || !b.contains(top[0], top[1])) return null;
        return top;
    }

    /**
     * What a client bound to one protocol speaks.
     *
     * @param protocol the wire name
     * @param version the declared version (the newest the client speaks), {@code MAJOR.MINOR.PATCH}
     * @param range the window it supports; its max is {@code version}'s major.minor
     */
    public record ClientVersioning(String protocol, String version, VersionRange range) {

        /** Validate the declaration. */
        public ClientVersioning {
            Objects.requireNonNull(protocol, "protocol");
            int[] v = parseSemver(version);
            if (v == null) throw new IllegalArgumentException("client protocol version must be MAJOR.MINOR.PATCH, got '"
                    + version + "'");
            if (range == null) range = new VersionRange(v[0], v[1], v[0], v[1]);
            if (range.maxMajor() != v[0] || range.maxMinor() != v[1]) {
                throw new IllegalArgumentException("client protocol version range " + range + " must end at "
                        + version + "'s major.minor (" + v[0] + "." + v[1] + ")");
            }
        }

        /**
         * A client's versioning from its version and an optional window text.
         *
         * @param protocol the wire name
         * @param version the newest version spoken
         * @param range {@code MIN-MAX}, or {@code null} for exactly {@code version}
         * @return the versioning
         */
        public static ClientVersioning of(String protocol, String version, String range) {
            return new ClientVersioning(protocol, version, range == null || range.isBlank()
                    ? null : VersionRange.parse(range));
        }

        /** @return the client's patch level, sent with whatever major.minor is negotiated */
        public int patch() { return parseSemver(version)[2]; }

        /** @return the client's {@value #CLIENT_HEADER} / {@value #CLIENT_KEY} value */
        public String advertised() { return protocol + "=" + range; }

        /**
         * {@code MAJOR.MINOR.PATCH} to send for a negotiated major.minor.
         *
         * @param mm the negotiated {@code {major, minor}}
         * @return the wire version, with the client's patch
         */
        public String wireVersion(int[] mm) { return mm[0] + "." + mm[1] + "." + patch(); }
    }

    /**
     * The client and server windows for a protocol do not overlap; raised client-side, before
     * sending. Carries the {@value #UNSUPPORTED} kind a server refusal does.
     */
    public static final class NoCommonVersionError extends RpcError {
        private static final long serialVersionUID = 1L;
        private final transient VersionRange clientRange;
        private final transient VersionRange serverRange;

        /**
         * @param protocol the protocol
         * @param clientRange what the client supports
         * @param serverRange what the server advertised
         * @param endpoint where the server is, for the message; may be empty
         */
        public NoCommonVersionError(String protocol, VersionRange clientRange, VersionRange serverRange,
                                    String endpoint) {
            super("ProtocolVersionError", message(protocol, clientRange, serverRange, endpoint), "", "",
                    UNSUPPORTED, "FAILED_PRECONDITION", List.of(new ErrorDetail.ErrorInfo(Map.of(
                            KEY, protocol + "=" + serverRange,
                            CLIENT_KEY, protocol + "=" + clientRange)).toJson()));
            this.clientRange = clientRange;
            this.serverRange = serverRange;
        }

        private static String message(String protocol, VersionRange client, VersionRange server, String endpoint) {
            String where = endpoint == null || endpoint.isEmpty() ? "" : " at " + endpoint;
            String advice = compare(server.maxMajor(), server.maxMinor(), client.minMajor(), client.minMinor()) < 0
                    ? "The server is too old (this client is too new for it): upgrade the worker to one supporting "
                            + protocol + " " + client.min() + " or newer, or use a client that supports "
                            + protocol + " " + server.max() + "."
                    : "The client is too old (the server is too new for it): upgrade the client to one supporting "
                            + protocol + " " + server.min() + " or newer, or use a worker that supports "
                            + protocol + " " + client.max() + ".";
            return UNSUPPORTED + ": no common " + protocol + " version. This client supports " + protocol + " "
                    + client + "; the server" + where + " supports " + protocol + " " + server + ". " + advice;
        }

        /** @return the client's window */
        public VersionRange clientRange() { return clientRange; }

        /** @return the server's window */
        public VersionRange serverRange() { return serverRange; }
    }

    /**
     * Whether {@code err} is a server's version refusal (this spec's kind, or the pre-spec one).
     *
     * @param err an error raised by a call
     * @return whether it is a version refusal
     */
    public static boolean isVersionRefusal(Throwable err) {
        return err instanceof RpcError e
                && (UNSUPPORTED.equals(e.errorKind()) || LEGACY_MISMATCH.equals(e.errorKind()));
    }

    /**
     * Whether a refused call may be re-sent once at a renegotiated version: reflection, and
     * methods declared {@code no_side_effects} / {@code idempotent}.
     *
     * @param protocol the protocol the call was addressed to
     * @param idempotency the method's declared idempotency level, or {@code null}
     * @return whether a single automatic retry is allowed
     */
    public static boolean isRetrySafe(String protocol, String idempotency) {
        return REFLECTION.equals(protocol) || "no_side_effects".equals(idempotency) || "idempotent".equals(idempotency);
    }

    /**
     * Negotiation state for one endpoint (one worker location / connection). Thread-safe.
     *
     * <p>The decision, per protocol (§6): the server advertised a window for it, so the highest
     * common version (or {@link NoCommonVersionError}); else a pre-spec refusal named a single
     * server version, so that one; else the client's own newest version.</p>
     */
    public static final class EndpointNegotiation {
        private final String endpoint;
        private Map<String, VersionRange> advertised = new LinkedHashMap<>();
        private final Map<String, int[]> legacy = new LinkedHashMap<>();
        private boolean seen;

        /** @param endpoint where the server is; used only in error messages */
        public EndpointNegotiation(String endpoint) { this.endpoint = endpoint == null ? "" : endpoint; }

        /** @return where the server is */
        public String endpoint() { return endpoint; }

        /** @return a copy of the windows learned so far */
        public synchronized Map<String, VersionRange> advertised() { return new LinkedHashMap<>(advertised); }

        /** @return whether any response so far carried the advertisement */
        public synchronized boolean hasAdvertisement() { return seen; }

        /**
         * Record an advertisement read from a response; absent ({@code null}) is ignored. A present
         * value replaces what was learned (a redeploy behind one URL changes it).
         *
         * @param value the header / metadata value, or {@code null}
         */
        public synchronized void observe(String value) {
            if (value == null) return;
            advertised = parse(value);
            seen = true;
            legacy.clear();
        }

        /**
         * Learn from a {@value #UNSUPPORTED} (or pre-spec mismatch) refusal.
         *
         * @param protocol the protocol the refused request was addressed to
         * @param err the refusal
         */
        public void observeRefusal(String protocol, RpcError err) {
            ErrorDetail.ErrorInfo info = err.errorInfo();
            Map<String, VersionRange> ranges = info == null ? Map.of() : parse(info.metadata().get(KEY));
            if (!ranges.isEmpty()) {
                synchronized (this) {
                    advertised = new LinkedHashMap<>(ranges);
                    seen = true;
                    legacy.clear();
                }
                return;
            }
            int[] old = legacyServerVersion(err);
            if (old != null) {
                synchronized (this) {
                    if (!seen) legacy.put(protocol, old);
                }
            }
        }

        /**
         * The {@code major.minor} to speak for {@code client}'s protocol.
         *
         * @param client the client's versioning
         * @return {@code {major, minor}}
         * @throws NoCommonVersionError when the windows do not overlap
         */
        public int[] choose(ClientVersioning client) {
            VersionRange server;
            int[] old;
            synchronized (this) {
                server = advertised.get(client.protocol());
                old = legacy.get(client.protocol());
            }
            if (server != null) {
                int[] common = highestCommon(client.range(), server);
                if (common == null) throw new NoCommonVersionError(client.protocol(), client.range(), server, endpoint);
                return common;
            }
            if (old != null) {
                if (!client.range().contains(old[0], old[1])) {
                    throw new NoCommonVersionError(client.protocol(), client.range(),
                            new VersionRange(old[0], old[1], old[0], old[1]), endpoint);
                }
                return old;
            }
            return new int[] {client.range().maxMajor(), client.range().maxMinor()};
        }

        /**
         * The {@code vgi_rpc.protocol_version} value to send for {@code client}'s protocol.
         *
         * @param client the client's versioning
         * @return {@code MAJOR.MINOR.PATCH}
         * @throws NoCommonVersionError when the windows do not overlap
         */
        public String wireVersion(ClientVersioning client) { return client.wireVersion(choose(client)); }
    }

    /**
     * The single server version a pre-spec refusal names ({@code Server: X.Y.Z} in the message,
     * or {@code server requires X.Y.Z} in a precondition description), when one is present.
     *
     * @param err the refusal
     * @return {@code {major, minor}}, or {@code null}
     */
    static int[] legacyServerVersion(RpcError err) {
        if (!isVersionRefusal(err)) return null;
        List<String> texts = new java.util.ArrayList<>();
        texts.add(err.errorMessage());
        ErrorDetail.PreconditionFailure failure = err.preconditionFailure();
        if (failure != null) {
            for (ErrorDetail.PreconditionViolation v : failure.violations()) texts.add(v.description());
        }
        for (String text : texts) {
            if (text == null) continue;
            Matcher m = LEGACY_SERVER_VERSION.matcher(text);
            if (m.find()) return parseMajorMinor(m.group(1));
        }
        return null;
    }
}
