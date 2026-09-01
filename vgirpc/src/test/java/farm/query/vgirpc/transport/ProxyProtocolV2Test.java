// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.transport;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ProxyProtocolV2Test {
    private static final byte[] SIGNATURE = {
        0x0d, 0x0a, 0x0d, 0x0a, 0x00, 0x0d, 0x0a, 0x51, 0x55, 0x49, 0x54, 0x0a
    };

    @Test
    void parsesIpv4AndLeavesFollowingVgiBytesUnread() throws Exception {
        byte[] preamble = ipv4(new byte[] {1, 2, 3, 4, 10, 0, 0, 8}, 1234, 9400,
                new byte[] {(byte) 0xee, 0, 2, 7, 8});
        byte[] following = {42, 43, 44, 45};
        byte[] input = concat(preamble, following);
        ByteArrayInputStream stream = new ByteArrayInputStream(input);

        ProxyProtocolV2.Address result = ProxyProtocolV2.read(stream, 536);

        assertEquals("1.2.3.4", result.source().getAddress().getHostAddress());
        assertEquals(1234, result.source().getPort());
        assertEquals("10.0.0.8", result.destination().getAddress().getHostAddress());
        assertEquals(9400, result.destination().getPort());
        assertArrayEquals(following, stream.readAllBytes());
    }

    @Test
    void parsesIpv6AndNormalizesMappedIpv4() throws Exception {
        byte[] source = new byte[16];
        source[10] = (byte) 0xff;
        source[11] = (byte) 0xff;
        source[12] = (byte) 192;
        source[13] = 0;
        source[14] = 2;
        source[15] = 10;
        byte[] destination = InetAddress.getByName("2001:db8::1").getAddress();
        byte[] preamble = ipv6(source, destination, 80, 9400, new byte[0]);

        ProxyProtocolV2.Address result = ProxyProtocolV2.parse(preamble, 536);

        assertEquals("192.0.2.10", result.source().getAddress().getHostAddress());
        assertEquals("2001:db8:0:0:0:0:0:1", result.destination().getAddress().getHostAddress());
    }

    @Test
    void rejectsUnsafeCommandsFamiliesAndMalformedTlvs() {
        byte[] valid = ipv4(new byte[] {1, 2, 3, 4, 5, 6, 7, 8}, 1, 2, new byte[0]);
        byte[] local = valid.clone();
        local[12] = 0x20;
        byte[] versionOne = valid.clone();
        versionOne[12] = 0x11;
        byte[] unspec = valid.clone();
        unspec[13] = 0x01;
        byte[] udp = valid.clone();
        udp[13] = 0x12;
        byte[] malformedTlv = ipv4(new byte[] {1, 2, 3, 4, 5, 6, 7, 8}, 1, 2,
                new byte[] {1, 0, 2, 9});

        for (byte[] value : new byte[][] {local, versionOne, unspec, udp, malformedTlv}) {
            assertThrows(IOException.class, () -> ProxyProtocolV2.parse(value, 536));
        }
    }

    @Test
    void rejectsTruncationOverlongDataAndOversizedDeclarationBeforeAllocation() {
        byte[] valid = ipv4(new byte[] {1, 2, 3, 4, 5, 6, 7, 8}, 1, 2, new byte[0]);
        assertThrows(IOException.class,
                () -> ProxyProtocolV2.parse(Arrays.copyOf(valid, 15), 536));
        assertThrows(IOException.class,
                () -> ProxyProtocolV2.parse(concat(valid, new byte[] {0}), 536));

        byte[] oversizedFixed = Arrays.copyOf(valid, 16);
        oversizedFixed[14] = 2;
        oversizedFixed[15] = 9;
        assertThrows(IOException.class,
                () -> ProxyProtocolV2.read(new ByteArrayInputStream(oversizedFixed), 536));
    }

    @Test
    void trustedAddressesAreExactDnsFreeAndMappedEquivalent() throws Exception {
        Set<String> trusted = ProxyProtocolV2.normalizeTrustedAddresses(Set.of(
                "127.0.0.1", "2001:db8::1"));
        assertTrue(ProxyProtocolV2.isTrusted(trusted, InetAddress.getByName("127.0.0.1")));
        assertTrue(ProxyProtocolV2.isTrusted(trusted, InetAddress.getByName("2001:db8::1")));
        assertThrows(IllegalArgumentException.class,
                () -> ProxyProtocolV2.normalizeTrustedAddresses(Set.of("localhost")));
        assertThrows(IllegalArgumentException.class,
                () -> ProxyProtocolV2.normalizeTrustedAddresses(Set.of("127.0.0.0/8")));
        assertThrows(IllegalArgumentException.class,
                () -> ProxyProtocolV2.normalizeTrustedAddresses(Set.of(
                        "192.0.2.10", "::ffff:192.0.2.10")));
    }

    @Test
    void requiredModeFailsClosedWithoutExplicitTrustAndDefaultsOff() {
        TcpServerOptions defaults = TcpServerOptions.defaults();
        assertFalse(defaults.proxyProtocolV2Required());
        assertEquals(Set.of(), defaults.trustedProxyAddresses());
        assertThrows(IllegalArgumentException.class, () -> TcpServerOptions.builder()
                .proxyProtocolV2Required(true)
                .build());
        assertThrows(IllegalArgumentException.class, () -> TcpServerOptions.builder()
                .trustedProxyAddresses(Set.of("proxy.internal"))
                .build());
    }

    static byte[] ipv4(byte[] addresses, int sourcePort, int destinationPort, byte[] tlvs) {
        if (addresses.length != 8) throw new IllegalArgumentException("eight address bytes required");
        ByteArrayOutputStream result = fixed(0x11, 12 + tlvs.length);
        result.writeBytes(addresses);
        ports(result, sourcePort, destinationPort);
        result.writeBytes(tlvs);
        return result.toByteArray();
    }

    private static byte[] ipv6(byte[] source, byte[] destination,
                               int sourcePort, int destinationPort, byte[] tlvs) {
        ByteArrayOutputStream result = fixed(0x21, 36 + tlvs.length);
        result.writeBytes(source);
        result.writeBytes(destination);
        ports(result, sourcePort, destinationPort);
        result.writeBytes(tlvs);
        return result.toByteArray();
    }

    private static ByteArrayOutputStream fixed(int family, int length) {
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        result.writeBytes(SIGNATURE);
        result.write(0x21);
        result.write(family);
        result.write((length >>> 8) & 0xff);
        result.write(length & 0xff);
        return result;
    }

    private static void ports(ByteArrayOutputStream result, int source, int destination) {
        result.write((source >>> 8) & 0xff);
        result.write(source & 0xff);
        result.write((destination >>> 8) & 0xff);
        result.write(destination & 0xff);
    }

    private static byte[] concat(byte[] first, byte[] second) {
        byte[] result = Arrays.copyOf(first, first.length + second.length);
        System.arraycopy(second, 0, result, first.length, second.length);
        return result;
    }
}
