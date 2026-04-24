// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http.auth;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class XfccParserTest {

    @Test
    void parses_single_element_with_hash_and_subject() {
        List<XfccElement> els = XfccParser.parse("Hash=abc123;Subject=\"CN=alice,O=acme\";URI=spiffe%3A%2F%2Ftest");
        assertEquals(1, els.size());
        XfccElement e = els.get(0);
        assertEquals("abc123", e.hash());
        assertEquals("CN=alice,O=acme", e.subject());
        assertEquals("spiffe://test", e.uri());
    }

    @Test
    void parses_multiple_dns_entries() {
        List<XfccElement> els = XfccParser.parse("DNS=a.example;DNS=b.example;Hash=h");
        XfccElement e = els.get(0);
        assertEquals(List.of("a.example", "b.example"), e.dns());
        assertEquals("h", e.hash());
    }

    @Test
    void splits_multiple_elements_across_comma() {
        List<XfccElement> els = XfccParser.parse("Hash=a;Subject=\"CN=client,O=x\", Hash=b;Subject=\"CN=proxy,O=y\"");
        assertEquals(2, els.size());
        assertEquals("a", els.get(0).hash());
        assertEquals("CN=client,O=x", els.get(0).subject());
        assertEquals("b", els.get(1).hash());
    }

    @Test
    void comma_inside_quoted_subject_is_not_a_separator() {
        List<XfccElement> els = XfccParser.parse("Subject=\"CN=alice,O=acme,C=US\";Hash=xyz");
        assertEquals(1, els.size());
        assertEquals("CN=alice,O=acme,C=US", els.get(0).subject());
        assertEquals("xyz", els.get(0).hash());
    }

    @Test
    void empty_or_whitespace_returns_empty_list() {
        assertTrue(XfccParser.parse("").isEmpty());
        assertTrue(XfccParser.parse("   ").isEmpty());
        assertTrue(XfccParser.parse(null).isEmpty());
    }

    @Test
    void unknown_keys_are_silently_ignored() {
        List<XfccElement> els = XfccParser.parse("Hash=h;Unknown=value;Subject=\"CN=a\"");
        XfccElement e = els.get(0);
        assertEquals("h", e.hash());
        assertEquals("CN=a", e.subject());
        assertNull(e.uri());
    }

    @Test
    void extract_cn_from_rfc4514_dn() {
        assertEquals("alice", XfccParser.extractCn("CN=alice,O=acme,C=US"));
        assertEquals("bob", XfccParser.extractCn("O=acme,CN=bob"));
        assertEquals("", XfccParser.extractCn("O=acme,C=US"));
        assertEquals("", XfccParser.extractCn(""));
        assertEquals("", XfccParser.extractCn(null));
    }

    @Test
    void quoted_backslash_escape_is_unwrapped() {
        // Header value: Subject="CN=al\"ice"
        List<XfccElement> els = XfccParser.parse("Subject=\"CN=al\\\"ice\"");
        assertNotNull(els.get(0).subject());
        assertEquals("CN=al\"ice", els.get(0).subject());
    }
}
