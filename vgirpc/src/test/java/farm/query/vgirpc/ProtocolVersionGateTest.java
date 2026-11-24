// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

import farm.query.vgirpc.schema.ProtocolVersion;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The application-protocol-version gate at the dispatch boundary.
 *
 * <p>vgi-rpc's Python, Go, Rust and TypeScript servers all refuse a request
 * whose declared {@code protocol_version} is incompatible with their own. Java
 * did not: it read the client's version off the request metadata, carried it
 * into the access log, and never compared it. A Java worker built against
 * protocol 1.4.0 would therefore serve a 1.0.0 client without complaint —
 * silently mis-serving it, which is precisely what the handshake exists to
 * prevent.
 *
 * <p>Nothing caught it because the cross-language test that would have
 * (protocol_version/version_mismatch.test) gates on a fixture worker Java did
 * not ship, so it skipped — and a skipped file reported as a pass.
 */
final class ProtocolVersionGateTest {

    @ProtocolVersion("1.4.0")
    interface Versioned {
        default void ping() {}
    }

    /** Drive the public gate directly so the cases below stay unit-sized. */
    private static String check(RpcServer server, String clientVersion) {
        ProtocolVersionError err = server.gateVersion(server.protocolName(), clientVersion);
        return err == null ? null : err.getMessage();
    }

    private static RpcServer server() {
        RpcServer s = new RpcServer(Versioned.class, new Versioned() {});
        s.setProtocolVersion("1.4.0");
        return s;
    }

    @Test
    void anExactMajorMinorMatchPasses() throws Exception {
        assertNull(check(server(), "1.4.0"));
    }

    @Test
    void patchIsIgnored() throws Exception {
        // A patch release on either side must not break a working pair —
        // otherwise every bugfix is a coordinated upgrade.
        assertNull(check(server(), "1.4.99"));
    }

    @Test
    void anOlderClientIsToldToUpgradeItself() throws Exception {
        String msg = check(server(), "1.0.0");
        assertNotNull(msg, "1.0.0 against 1.4.0 must be refused");
        assertTrue(msg.startsWith("protocol_version_unsupported: client speaks Versioned 1.0;"), msg);
        assertTrue(msg.contains("this server supports Versioned 1.4-1.4"), msg);
        assertTrue(msg.contains("The client is too old (this server is too new for it)"), msg);
    }

    @Test
    void aNewerClientIsToldTheServerIsBehind() throws Exception {
        // Direction matters: "mismatch" alone leaves an operator holding two
        // version numbers and no idea which side to move.
        String msg = check(server(), "99.0.0");
        assertNotNull(msg);
        assertTrue(msg.contains("The server is too old (the client is too new for it)"), msg);
        assertTrue(msg.contains("use a client that supports Versioned 1.4."), msg);
    }

    @Test
    void aClientThatDeclaresNothingIsRefused() throws Exception {
        // A peer that cannot say what it speaks is not a peer that can be
        // trusted to speak it.
        String msg = check(server(), null);
        assertNotNull(msg);
        assertTrue(msg.contains("the client did not send vgi_rpc.protocol_version for Versioned"), msg);
        assertTrue(msg.contains("this server supports Versioned 1.4-1.4"), msg);
    }

    @Test
    void aMalformedVersionIsRefused() throws Exception {
        for (String bad : new String[] {"1.4", "banana", "1.4.0-rc1", "1.-4.0", ""}) {
            String msg = check(server(), bad);
            assertNotNull(msg, "'" + bad + "' is not canonical semver and must be refused");
            assertTrue(msg.contains("malformed protocol_version"), msg);
        }
    }

    @Test
    void aWindowOverrideWidensAcceptanceAndTheAdvertisement() {
        RpcServer s = server();
        assertTrue(s.protocolVersions().equals("Versioned=1.4-1.4"), s.protocolVersions());
        s.setProtocolVersionRanges(java.util.Map.of("Versioned", "1.3-1.4"));
        assertTrue(s.protocolVersions().equals("Versioned=1.3-1.4"), s.protocolVersions());
        assertNull(check(s, "1.3.9"));
        assertNotNull(check(s, "1.2.0"));
        s.setProtocolVersionRanges(java.util.Map.of("NotHosted", "1.0-1.0"));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, s::protocolVersions);
    }

    @Test
    void theClientWindowIsQuotedNeverARefusal() {
        RpcServer s = server();
        assertNull(s.gateVersion("Versioned", "1.4.0", "not a window at all"));
        String msg = s.gateVersion("Versioned", "1.5.0", "Versioned=1.4-1.5").getMessage();
        assertTrue(msg.contains("client speaks Versioned 1.5 (supports 1.4-1.5)"), msg);
    }

    @Test
    void anUnsetServerVersionOptsOut() {
        // The gate's caller skips it when the version is empty. Assert the
        // opt-out is real, so embedders that never call setProtocolVersion are
        // not suddenly rejecting every request.
        RpcServer s = new RpcServer(Versioned.class, new Versioned() {});
        assertTrue(s.protocolVersion().isEmpty(),
                "a server that was never told a version must not enforce one");
    }
}
