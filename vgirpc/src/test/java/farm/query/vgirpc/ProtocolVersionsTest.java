// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

import farm.query.vgirpc.ProtocolVersions.ClientVersioning;
import farm.query.vgirpc.ProtocolVersions.EndpointNegotiation;
import farm.query.vgirpc.ProtocolVersions.NoCommonVersionError;
import farm.query.vgirpc.ProtocolVersions.VersionRange;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** PROTOCOL_VERSIONS_V1 §4.0 syntax and §6 choosing, unit-sized. */
final class ProtocolVersionsTest {

    @Test
    void parseIsTolerantAndFormatWritesBothEnds() {
        Map<String, VersionRange> m = ProtocolVersions.parse(
                " vgi.v3=3.4 , bad entry, x=1.0-0.9, vgi.secret.v1 = 1.0-1.0, vgi.v3=9.9-9.9, y=3.04-3.5");
        assertEquals("vgi.v3=3.4-3.4, vgi.secret.v1=1.0-1.0", ProtocolVersions.format(m));
        assertTrue(ProtocolVersions.parse("").isEmpty());
        assertTrue(ProtocolVersions.parse(null).isEmpty());
        assertEquals("3.4-3.5", VersionRange.parse("3.4.1-3.5.0").toString());
        assertThrows(IllegalArgumentException.class, () -> VersionRange.parse("3.5-3.4"));
    }

    @Test
    void highestCommonOrNull() {
        assertArrayEquals(new int[] {3, 4}, ProtocolVersions.highestCommon(
                VersionRange.parse("3.4-3.5"), VersionRange.parse("3.4-3.4")));
        assertNull(ProtocolVersions.highestCommon(VersionRange.parse("3.5-3.6"), VersionRange.parse("3.4-3.4")));
    }

    @Test
    void negotiationFollowsTheSpecOrder() {
        ClientVersioning client = ClientVersioning.of("p", "3.5.2", "3.4-3.5");
        EndpointNegotiation n = new EndpointNegotiation("http://w");
        assertEquals("3.5.2", n.wireVersion(client), "nothing learned: own newest");
        n.observe("p=3.4-3.4");
        assertEquals("3.4.2", n.wireVersion(client), "advertised: highest common plus own patch");
        n.observe(null);
        assertEquals("3.4.2", n.wireVersion(client), "an absent header changes nothing");
        n.observe("other=1.0-1.0");
        assertEquals("3.5.2", n.wireVersion(client), "a present header replaces; p unadvertised");
        n.observe("p=3.6-3.7");
        NoCommonVersionError e = assertThrows(NoCommonVersionError.class, () -> n.wireVersion(client));
        assertEquals(ProtocolVersions.UNSUPPORTED, e.errorKind());
        assertTrue(e.errorMessage().contains("3.4-3.5") && e.errorMessage().contains("3.6-3.7"), e.errorMessage());
        assertTrue(e.errorMessage().contains("The client is too old"), e.errorMessage());
    }

    @Test
    void aLegacyRefusalNamesTheVersion() {
        ClientVersioning client = ClientVersioning.of("p", "3.5.2", "3.4-3.5");
        EndpointNegotiation n = new EndpointNegotiation("");
        RpcError legacy = new RpcError("ProtocolVersionError",
                "VGI client/worker protocol_version mismatch.\n  Client: 3.5.2\n  Server: 3.4.0\n", "", "",
                ProtocolVersions.LEGACY_MISMATCH);
        n.observeRefusal("p", legacy);
        assertEquals("3.4.2", n.wireVersion(client));
    }

    @Test
    void aRefusalCarryingTheWindowsTeachesThem() {
        ClientVersioning client = ClientVersioning.of("p", "3.5.2", "3.4-3.5");
        EndpointNegotiation n = new EndpointNegotiation("");
        Map<String, String> md = new LinkedHashMap<>();
        md.put(ProtocolVersions.KEY, "p=3.4-3.4");
        RpcError refusal = new RpcError("ProtocolVersionError", "protocol_version_unsupported: ...", "", "",
                ProtocolVersions.UNSUPPORTED, "FAILED_PRECONDITION",
                List.of(new farm.query.vgirpc.errors.ErrorDetail.ErrorInfo(md).toJson()));
        n.observeRefusal("p", refusal);
        assertEquals("3.4.2", n.wireVersion(client));
    }

    @Test
    void clientVersioningValidates() {
        assertThrows(IllegalArgumentException.class, () -> ClientVersioning.of("p", "3.5", null));
        assertThrows(IllegalArgumentException.class, () -> ClientVersioning.of("p", "3.5.0", "3.4-3.6"));
        assertEquals("p=3.5-3.5", ClientVersioning.of("p", "3.5.1", null).advertised());
        assertTrue(ProtocolVersions.isRetrySafe("vgi_rpc.Reflection.v1", null));
        assertTrue(!ProtocolVersions.isRetrySafe("p", null));
    }
}
