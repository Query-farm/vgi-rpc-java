// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0
package farm.query.vgirpc.http;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/** DNS-free parsing and normalization for exact trusted-proxy IP literals. */
final class ExactIpAddresses {
    private ExactIpAddresses() {}

    static Set<String> trusted(Set<String> configured) {
        if (configured == null || configured.isEmpty()) {
            throw new IllegalArgumentException("exact trusted proxy IP literals are required");
        }
        Set<String> normalized = new LinkedHashSet<>();
        for (String value : configured) {
            String address = normalize(value);
            if (!normalized.add(address)) {
                throw new IllegalArgumentException("duplicate trusted proxy IP after normalization");
            }
        }
        return Set.copyOf(normalized);
    }

    static boolean contains(Set<String> normalizedTrusted, String physicalPeer) {
        try {
            return normalizedTrusted.contains(normalize(physicalPeer));
        } catch (IllegalArgumentException ignored) {
            return false;
        }
    }

    private static String normalize(String value) {
        if (value == null || value.isEmpty() || !value.equals(value.strip())
                || value.indexOf('/') >= 0 || value.indexOf('%') >= 0
                || value.indexOf('[') >= 0 || value.indexOf(']') >= 0) {
            throw new IllegalArgumentException("trusted proxy must be an exact IP literal");
        }
        byte[] address = value.indexOf(':') >= 0 ? parseIpv6(value) : parseIpv4(value);
        if (address.length == 16 && isIpv4Mapped(address)) {
            address = new byte[] {address[12], address[13], address[14], address[15]};
        }
        StringBuilder canonical = new StringBuilder(address.length == 4 ? "4:" : "6:");
        for (byte octet : address) {
            canonical.append(Character.forDigit((octet >>> 4) & 0xf, 16));
            canonical.append(Character.forDigit(octet & 0xf, 16));
        }
        return canonical.toString();
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
                continue;
            }
            if (token.length() > 4 || !token.chars().allMatch(ExactIpAddresses::isHexDigit)) {
                throw new IllegalArgumentException("invalid IPv6 literal");
            }
            groups.add(Integer.parseInt(token, 16));
        }
        return groups;
    }

    private static boolean isHexDigit(int character) {
        return (character >= '0' && character <= '9')
                || (character >= 'a' && character <= 'f')
                || (character >= 'A' && character <= 'F');
    }

    private static boolean isIpv4Mapped(byte[] address) {
        for (int index = 0; index < 10; index++) {
            if (address[index] != 0) return false;
        }
        return (address[10] & 0xff) == 0xff && (address[11] & 0xff) == 0xff;
    }
}
