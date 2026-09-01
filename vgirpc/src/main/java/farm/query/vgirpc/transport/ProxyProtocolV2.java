// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.transport;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/** Strict, bounded PROXY protocol v2 parsing for trusted TCP listeners. */
public final class ProxyProtocolV2 {
    /** Default upper bound: the fixed preamble, address block, and at most 520 bytes of TLVs. */
    public static final int DEFAULT_MAXIMUM_BYTES = 536;
    /** Experimental TLV reserved for the VGI Iroh bridge EndpointId contract. */
    public static final int VGI_IROH_ENDPOINT_TLV = 0xe0;

    private static final int FIXED_BYTES = 16;
    private static final byte[] SIGNATURE = {
        0x0d, 0x0a, 0x0d, 0x0a, 0x00, 0x0d, 0x0a, 0x51, 0x55, 0x49, 0x54, 0x0a
    };

    private ProxyProtocolV2() {}

    /** Asserted TCP source and destination endpoints from one trusted preamble. */
    public record Address(InetSocketAddress source, InetSocketAddress destination) {}

    /** Lowercase hexadecimal Iroh EndpointId from the dedicated PROXY/UNSPEC form. */
    public record IrohIdentity(String endpointId) {}

    record ForwardedPeer(Address address, IrohIdentity irohIdentity) {}

    /**
     * Read exactly one bounded preamble. Bytes following its declared length remain unread.
     *
     * <p>The caller is responsible for applying a deadline when the input is network-backed.
     */
    public static Address read(InputStream input, int maximumBytes) throws IOException {
        return read(input, maximumBytes, null);
    }

    /** Parse one exact preamble, accepting only PROXY with TCP over IPv4 or IPv6. */
    public static Address parse(byte[] preamble, int maximumBytes) throws IOException {
        return parseForwarded(preamble, maximumBytes, false).address();
    }

    /** Parse the opt-in PROXY/UNSPEC form carrying one bridge-verified EndpointId. */
    public static IrohIdentity parseIrohIdentity(byte[] preamble, int maximumBytes)
            throws IOException {
        ForwardedPeer peer = parseForwarded(preamble, maximumBytes, true);
        if (peer.irohIdentity() == null) {
            throw invalid("VGI Iroh identity requires PROXY/UNSPEC");
        }
        return peer.irohIdentity();
    }

    /** Read exactly one dedicated Iroh preamble while preserving following VGI bytes. */
    public static IrohIdentity readIrohIdentity(InputStream input, int maximumBytes)
            throws IOException {
        ForwardedPeer peer = readForwarded(input, maximumBytes, null, true);
        if (peer.irohIdentity() == null) {
            throw invalid("VGI Iroh identity requires PROXY/UNSPEC");
        }
        return peer.irohIdentity();
    }

    private static ForwardedPeer parseForwarded(
            byte[] preamble, int maximumBytes, boolean allowIrohIdentity) throws IOException {
        int limit = validateMaximum(maximumBytes);
        if (preamble == null || preamble.length < FIXED_BYTES) {
            throw invalid("truncated PROXY v2 fixed preamble");
        }
        if (preamble.length > limit) throw invalid("PROXY v2 preamble exceeds configured limit");
        if (!Arrays.equals(SIGNATURE, Arrays.copyOfRange(preamble, 0, SIGNATURE.length))) {
            throw invalid("missing PROXY v2 signature");
        }
        if ((preamble[12] & 0xf0) != 0x20) throw invalid("unsupported PROXY protocol version");
        if ((preamble[12] & 0x0f) != 0x01) {
            throw invalid("PROXY v2 LOCAL command is not accepted");
        }
        int expected = FIXED_BYTES + unsignedShort(preamble, 14);
        if (preamble.length != expected) throw invalid("truncated or overlong PROXY v2 preamble");

        int familyProtocol = preamble[13] & 0xff;
        int addressBytes;
        InetSocketAddress source;
        InetSocketAddress destination;
        if (familyProtocol == 0x00 && allowIrohIdentity) {
            addressBytes = 0;
            source = null;
            destination = null;
        } else if (familyProtocol == 0x11) {
            addressBytes = 12;
            requireBody(preamble, addressBytes, "IPv4");
            source = endpoint(Arrays.copyOfRange(preamble, 16, 20), unsignedShort(preamble, 24));
            destination = endpoint(Arrays.copyOfRange(preamble, 20, 24), unsignedShort(preamble, 26));
        } else if (familyProtocol == 0x21) {
            addressBytes = 36;
            requireBody(preamble, addressBytes, "IPv6");
            source = endpoint(normalizeMapped(Arrays.copyOfRange(preamble, 16, 32)),
                    unsignedShort(preamble, 48));
            destination = endpoint(normalizeMapped(Arrays.copyOfRange(preamble, 32, 48)),
                    unsignedShort(preamble, 50));
        } else {
            throw invalid("PROXY v2 requires TCP over IPv4 or IPv6");
        }

        int offset = FIXED_BYTES + addressBytes;
        byte[] endpointId = null;
        while (offset < preamble.length) {
            if (preamble.length - offset < 3) throw invalid("truncated PROXY v2 TLV header");
            int type = preamble[offset] & 0xff;
            int length = unsignedShort(preamble, offset + 1);
            offset += 3;
            if (length > preamble.length - offset) throw invalid("truncated PROXY v2 TLV value");
            if (type == VGI_IROH_ENDPOINT_TLV && allowIrohIdentity) {
                if (endpointId != null) throw invalid("duplicate VGI Iroh identity TLV");
                if (length != 33 || (preamble[offset] & 0xff) != 1) {
                    throw invalid("invalid VGI Iroh identity TLV");
                }
                endpointId = Arrays.copyOfRange(preamble, offset + 1, offset + 33);
            }
            offset += length;
        }
        if (familyProtocol == 0x00 && endpointId == null) {
            throw invalid("PROXY/UNSPEC requires one VGI Iroh identity TLV");
        }
        if (endpointId != null && familyProtocol != 0x00) {
            throw invalid("VGI Iroh identity requires PROXY/UNSPEC");
        }
        Address address = source != null ? new Address(source, destination) : null;
        IrohIdentity iroh = endpointId != null
                ? new IrohIdentity(java.util.HexFormat.of().formatHex(endpointId)) : null;
        return new ForwardedPeer(address, iroh);
    }

    static Address read(Socket socket, Duration timeout, int maximumBytes) throws IOException {
        return readSocket(socket, timeout, maximumBytes, false).address();
    }

    static ForwardedPeer readAllowingIroh(
            Socket socket, Duration timeout, int maximumBytes) throws IOException {
        return readSocket(socket, timeout, maximumBytes, true);
    }

    private static ForwardedPeer readSocket(
            Socket socket, Duration timeout, int maximumBytes, boolean allowIrohIdentity)
            throws IOException {
        if (timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("PROXY v2 preamble timeout must be positive");
        }
        long convertedTimeoutNanos;
        try {
            convertedTimeoutNanos = timeout.toNanos();
        } catch (ArithmeticException error) {
            convertedTimeoutNanos = Long.MAX_VALUE;
        }
        final long timeoutNanos = convertedTimeoutNanos;
        long started = System.nanoTime();
        int previousTimeout = socket.getSoTimeout();
        try {
            return readForwarded(socket.getInputStream(), maximumBytes, () -> {
                long elapsed = System.nanoTime() - started;
                long remaining = timeoutNanos - Math.max(0L, elapsed);
                if (remaining <= 0L) throw new SocketTimeoutException("PROXY v2 preamble timed out");
                long millis = Math.max(1L, ((remaining - 1L) / 1_000_000L) + 1L);
                socket.setSoTimeout((int) Math.min(Integer.MAX_VALUE, millis));
            }, allowIrohIdentity);
        } catch (SocketTimeoutException error) {
            throw new SocketTimeoutException("PROXY v2 preamble timed out");
        } finally {
            try {
                socket.setSoTimeout(previousTimeout);
            } catch (IOException ignored) {
                // Socket closure after rejection makes restoration irrelevant.
            }
        }
    }

    static Set<String> normalizeTrustedAddresses(Set<String> configured) {
        if (configured == null) return Set.of();
        Set<String> normalized = new LinkedHashSet<>();
        for (String value : configured) {
            String address = canonicalLiteral(value);
            if (!normalized.add(address)) {
                throw new IllegalArgumentException("duplicate trusted proxy IP after normalization");
            }
        }
        return Set.copyOf(normalized);
    }

    static boolean isTrusted(Set<String> normalized, InetAddress address) {
        return normalized.contains(canonicalBytes(address.getAddress()));
    }

    private static Address read(InputStream input, int maximumBytes, BeforeRead beforeRead)
            throws IOException {
        return readForwarded(input, maximumBytes, beforeRead, false).address();
    }

    private static ForwardedPeer readForwarded(
            InputStream input, int maximumBytes, BeforeRead beforeRead, boolean allowIrohIdentity)
            throws IOException {
        int limit = validateMaximum(maximumBytes);
        byte[] fixed = new byte[FIXED_BYTES];
        readExactly(input, fixed, 0, fixed.length, beforeRead, "fixed preamble");
        int total = FIXED_BYTES + unsignedShort(fixed, 14);
        if (total > limit) throw invalid("PROXY v2 preamble exceeds configured limit");
        byte[] preamble = Arrays.copyOf(fixed, total);
        readExactly(input, preamble, FIXED_BYTES, total - FIXED_BYTES, beforeRead, "body");
        return parseForwarded(preamble, limit, allowIrohIdentity);
    }

    private static void readExactly(InputStream input, byte[] buffer, int offset, int length,
                                    BeforeRead beforeRead, String part) throws IOException {
        int end = offset + length;
        while (offset < end) {
            if (beforeRead != null) beforeRead.run();
            int count = input.read(buffer, offset, end - offset);
            if (count < 0) throw new EOFException("truncated PROXY v2 " + part);
            if (count == 0) continue;
            offset += count;
        }
    }

    private static void requireBody(byte[] preamble, int addressBytes, String family) throws IOException {
        if (preamble.length - FIXED_BYTES < addressBytes) {
            throw invalid("truncated PROXY v2 TCP/" + family + " address block");
        }
    }

    private static InetSocketAddress endpoint(byte[] address, int port) throws IOException {
        try {
            return new InetSocketAddress(InetAddress.getByAddress(address), port);
        } catch (UnknownHostException error) {
            throw invalid("invalid PROXY v2 address");
        }
    }

    private static byte[] normalizeMapped(byte[] address) {
        if (address.length == 16 && isIpv4Mapped(address)) return Arrays.copyOfRange(address, 12, 16);
        return address;
    }

    private static int validateMaximum(int maximumBytes) {
        int value = maximumBytes == 0 ? DEFAULT_MAXIMUM_BYTES : maximumBytes;
        if (value < FIXED_BYTES) throw new IllegalArgumentException("maximum PROXY v2 bytes must be at least 16");
        return value;
    }

    private static int unsignedShort(byte[] input, int offset) {
        return ((input[offset] & 0xff) << 8) | (input[offset + 1] & 0xff);
    }

    private static String canonicalLiteral(String value) {
        if (value == null || value.isEmpty() || !value.equals(value.strip())
                || value.indexOf('/') >= 0 || value.indexOf('%') >= 0
                || value.indexOf('[') >= 0 || value.indexOf(']') >= 0) {
            throw new IllegalArgumentException("trusted proxy must be an exact IP literal");
        }
        byte[] address = value.indexOf(':') >= 0 ? parseIpv6(value) : parseIpv4(value);
        return canonicalBytes(address);
    }

    private static String canonicalBytes(byte[] address) {
        byte[] normalized = normalizeMapped(address);
        try {
            return InetAddress.getByAddress(normalized).getHostAddress();
        } catch (UnknownHostException impossible) {
            throw new IllegalArgumentException("invalid IP address length", impossible);
        }
    }

    private static byte[] parseIpv4(String value) {
        String[] parts = value.split("\\.", -1);
        if (parts.length != 4) throw new IllegalArgumentException("invalid IPv4 literal");
        byte[] address = new byte[4];
        for (int index = 0; index < parts.length; index++) {
            String part = parts[index];
            if (part.isEmpty() || part.length() > 3 || (part.length() > 1 && part.charAt(0) == '0')
                    || !part.chars().allMatch(character -> character >= '0' && character <= '9')) {
                throw new IllegalArgumentException("invalid IPv4 literal");
            }
            int octet;
            try {
                octet = Integer.parseInt(part);
            } catch (NumberFormatException error) {
                throw new IllegalArgumentException("invalid IPv4 literal", error);
            }
            if (octet > 255) throw new IllegalArgumentException("invalid IPv4 literal");
            address[index] = (byte) octet;
        }
        return address;
    }

    private static byte[] parseIpv6(String value) {
        int compression = value.indexOf("::");
        if (compression >= 0 && compression != value.lastIndexOf("::")) {
            throw new IllegalArgumentException("invalid IPv6 literal");
        }
        String leftText = compression < 0 ? value : value.substring(0, compression);
        String rightText = compression < 0 ? "" : value.substring(compression + 2);
        List<Integer> left = parseIpv6Side(leftText, compression < 0);
        List<Integer> right = parseIpv6Side(rightText, compression >= 0 && !rightText.isEmpty());
        int groups = left.size() + right.size();
        if ((compression < 0 && groups != 8) || (compression >= 0 && groups >= 8)) {
            throw new IllegalArgumentException("invalid IPv6 literal");
        }
        List<Integer> expanded = new ArrayList<>(8);
        expanded.addAll(left);
        if (compression >= 0) {
            for (int count = groups; count < 8; count++) expanded.add(0);
        }
        expanded.addAll(right);
        byte[] address = new byte[16];
        for (int index = 0; index < expanded.size(); index++) {
            int group = expanded.get(index);
            address[index * 2] = (byte) (group >>> 8);
            address[index * 2 + 1] = (byte) group;
        }
        return address;
    }

    private static List<Integer> parseIpv6Side(String side, boolean allowIpv4AtEnd) {
        if (side.isEmpty()) return List.of();
        String[] tokens = side.split(":", -1);
        List<Integer> groups = new ArrayList<>(tokens.length);
        for (int index = 0; index < tokens.length; index++) {
            String token = tokens[index];
            if (token.isEmpty()) throw new IllegalArgumentException("invalid IPv6 literal");
            if (token.indexOf('.') >= 0) {
                if (!allowIpv4AtEnd || index != tokens.length - 1) {
                    throw new IllegalArgumentException("invalid embedded IPv4 literal");
                }
                byte[] ipv4 = parseIpv4(token);
                groups.add(((ipv4[0] & 0xff) << 8) | (ipv4[1] & 0xff));
                groups.add(((ipv4[2] & 0xff) << 8) | (ipv4[3] & 0xff));
            } else {
                if (token.length() > 4 || !token.chars().allMatch(ProxyProtocolV2::isHexDigit)) {
                    throw new IllegalArgumentException("invalid IPv6 literal");
                }
                groups.add(Integer.parseInt(token, 16));
            }
        }
        return groups;
    }

    private static boolean isHexDigit(int character) {
        return (character >= '0' && character <= '9')
                || (character >= 'a' && character <= 'f')
                || (character >= 'A' && character <= 'F');
    }

    private static boolean isIpv4Mapped(byte[] address) {
        for (int index = 0; index < 10; index++) if (address[index] != 0) return false;
        return (address[10] & 0xff) == 0xff && (address[11] & 0xff) == 0xff;
    }

    private static IOException invalid(String message) {
        return new IOException(message);
    }

    @FunctionalInterface
    private interface BeforeRead { void run() throws IOException; }

}
