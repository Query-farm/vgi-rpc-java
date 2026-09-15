// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

import farm.query.vgirpc.identity.Identity;
import farm.query.vgirpc.identity.IdentityImpl;
import farm.query.vgirpc.identity.TokenIdentity;
import farm.query.vgirpc.marshal.Marshalling;
import farm.query.vgirpc.transport.RpcTransport;
import farm.query.vgirpc.wire.Allocators;
import farm.query.vgirpc.wire.IpcStreamWriter;
import farm.query.vgirpc.wire.Metadata;
import farm.query.vgirpc.wire.Wire;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * An access record names the protocol that owns the dispatched method -- and carries its digest.
 *
 * <p>{@code access-log-spec.md} §3 makes {@code protocol} the owning protocol's wire name, "not a
 * server-wide default", and {@code protocol_hash} "the registry key when decoding archived
 * records". This port used to read both from the server: {@code DispatchInfo.protocol} came from
 * {@code protocolName()}, the application protocol, and {@code protocolHash} from the retired
 * describe-payload digest. It then declined to log the framework protocols at all, which was the
 * honest response to the first half -- no record beats a confidently mislabelled one -- and left
 * the second half standing for the primary.
 *
 * <p>Both halves fail <em>silently</em>. A mislabelled record is well-formed, passes the schema,
 * and produces a plausible dashboard while a consumer keying on {@code protocol_hash} decodes it
 * against the wrong description. Nothing errors. And only a call to a <em>secondary</em> protocol
 * can catch the first, because for an application method the primary <em>is</em> the owning
 * binding -- which is how several ports shipped it with green suites.
 */
@Timeout(30)
final class AccessRecordIdentityTest {

    /** The application protocol under test. Its wire name is the interface's simple name. */
    public interface Ledger {
        String echo(String value);
    }

    static final class LedgerImpl implements Ledger {
        @Override public String echo(String value) { return value; }
    }

    /**
     * {@code Ledger}'s canonical digest, pinned as a literal.
     *
     * <p>Pinned rather than recomputed because the bug this file exists for has two independent
     * halves, and recomputing would only catch one. Asserting the record carries
     * {@code bindingHash(...)} proves it names the right protocol; it proves nothing about
     * <em>which digest definition</em> that is. Go logged the right protocol's hash computed the
     * legacy, Arrow-IPC-bytes way -- the right answer to the wrong question, and unkeyable
     * against the canonical registry. A literal is the only assertion that notices.
     *
     * <p>Derived from {@link farm.query.vgirpc.hash.ProtocolHash}, whose cross-port agreement is
     * pinned separately by {@code ProtocolHashTest.matchesThePythonReferenceDigest}.
     */
    private static final String LEDGER_CANONICAL_HASH =
            "40afc20052d9c1f5b1e6966f81b8a9125773e628c0a5aeeef154b7d6346ecda1";

    // --- the accessor --------------------------------------------------------

    @Test
    void theHostedProtocolsDoNotShareADigest() {
        // The precondition that makes everything below meaningful: if reflection, identity and
        // the application hashed alike, logging the primary everywhere would be both invisible
        // and harmless, and none of these assertions would prove anything.
        RpcServer srv = identityServer();
        String app = srv.protocolIdentityFor("Ledger").protocolHash();
        String refl = srv.protocolIdentityFor(Reflection.PROTOCOL_NAME).protocolHash();
        String ident = srv.protocolIdentityFor(Identity.PROTOCOL_NAME).protocolHash();
        assertNotEquals(app, refl);
        assertNotEquals(app, ident);
        assertNotEquals(refl, ident);
    }

    @Test
    void theAccessorReturnsTheOwningBindingsIdentity() {
        RpcServer srv = identityServer();
        srv.setProtocolVersion("2.1.0");

        RpcServer.ProtocolIdentity app = srv.protocolIdentityFor("Ledger");
        assertEquals("Ledger", app.name());
        assertEquals(srv.protocolHash(), app.protocolHash());
        assertEquals("2.1.0", app.protocolVersion());

        RpcServer.ProtocolIdentity refl = srv.protocolIdentityFor(Reflection.PROTOCOL_NAME);
        assertEquals(Reflection.PROTOCOL_NAME, refl.name());
        assertEquals(Reflection.bindingHash(Reflection.PROTOCOL_NAME, Reflection.methodTable()), refl.protocolHash());
        // Reflection declares no version of its own, and the application's label is not its:
        // a record stamped 2.1.0 for a reflection call attributes the app's contract to it.
        assertEquals("", refl.protocolVersion());

        RpcServer.ProtocolIdentity ident = srv.protocolIdentityFor(Identity.PROTOCOL_NAME);
        assertEquals(Identity.PROTOCOL_NAME, ident.name());
        assertEquals(Reflection.bindingHash(Identity.PROTOCOL_NAME, srv.identityMethodTable()),
                ident.protocolHash());
        assertEquals("", ident.protocolVersion());
    }

    @Test
    void anUnroutedFrameworkEndpointFallsBackToThePrimary() {
        // __transport_options__ and __upload_url__ belong to no protocol. The spec prescribes
        // the server's primary for those, so this is the specified behaviour, not a gap in it.
        RpcServer srv = identityServer();
        assertEquals(srv.protocolName(), srv.protocolIdentityFor(null).name());
        assertEquals(srv.protocolHash(), srv.protocolIdentityFor(null).protocolHash());
    }

    @Test
    void identityIsNotAttributedToAProtocolTheServerDoesNotHost() {
        // A server without identity configured must not name identity: the protocol is absent,
        // not routed-and-refusing, so a record claiming it would describe a surface that is
        // not there.
        RpcServer bare = new RpcServer(Ledger.class, new LedgerImpl(), "srv-bare");
        assertEquals(bare.protocolName(), bare.protocolIdentityFor(Identity.PROTOCOL_NAME).name());
    }

    @Test
    void rewiringIdentityRestatesItsDigest() {
        // The digests are memoised -- computing one serialises every method's schemas and
        // canonicalises the JSON, and an access record needs one per dispatch. Identity's table
        // is narrowed to the hooks the deployment configured, so it can change after the first
        // record was written; a cache that outlived the change would file every later call under
        // the surface the worker used to have.
        RpcServer srv = new RpcServer(Ledger.class, new LedgerImpl(), "srv-rewire");
        srv.setIdentity(IdentityImpl.builder()
                .resolveToken(token -> null)
                .introspectPrincipals("alice")
                .build());
        String resolveOnly = srv.protocolIdentityFor(Identity.PROTOCOL_NAME).protocolHash();

        srv.setIdentity(IdentityImpl.builder()
                .resolveToken(token -> null)
                .introspectPrincipals("alice")
                .mintGrant((principal, purpose, scopes, ttl) ->
                        new farm.query.vgirpc.identity.IssuedGrant("g", 1.0, "g1"))
                .build());
        String alsoMints = srv.protocolIdentityFor(Identity.PROTOCOL_NAME).protocolHash();

        assertNotEquals(resolveOnly, alsoMints,
                "a worker that gained the ability to mint grants offers a different surface, "
                        + "and the hash is how a client discovers that without calling");
        assertEquals(Reflection.bindingHash(Identity.PROTOCOL_NAME, srv.identityMethodTable()),
                alsoMints);
    }

    @Test
    void theLoggedDigestIsTheCanonicalOne() {
        RpcServer srv = new RpcServer(Ledger.class, new LedgerImpl(), "srv-hash");
        assertEquals(LEDGER_CANONICAL_HASH, srv.protocolIdentityFor("Ledger").protocolHash(),
                () -> "preimage: " + Reflection.bindingPreimage("Ledger", srv.methods()));
    }

    // --- end to end, through real dispatch -----------------------------------

    @Test
    void anApplicationCallNamesTheApplicationProtocol() throws Exception {
        Capture cap = new Capture();
        RpcServer srv = identityServer();
        srv.setDispatchHook(cap);
        serveRaw(srv, request("Ledger", "echo", utf8("value"), Map.of("value", "hi")));

        DispatchInfo rec = cap.only();
        assertEquals("Ledger", rec.protocol);
        assertEquals(LEDGER_CANONICAL_HASH, rec.protocolHash);
    }

    @Test
    void aReflectionCallNamesReflection() throws Exception {
        Capture cap = new Capture();
        RpcServer srv = identityServer();
        srv.setDispatchHook(cap);
        serveRaw(srv, request(Reflection.PROTOCOL_NAME, "list_protocols",
                new Schema(List.of()), Map.of()));

        DispatchInfo rec = cap.only();
        assertEquals(Reflection.PROTOCOL_NAME, rec.protocol,
                "a reflection call filed under the application protocol merges two protocols' "
                        + "traffic into one bucket, and nothing about the record looks wrong");
        assertEquals(Reflection.bindingHash(Reflection.PROTOCOL_NAME, Reflection.methodTable()), rec.protocolHash);
        assertNotEquals(srv.protocolHash(), rec.protocolHash,
                "protocol_hash is the registry key for decoding archived records, so a record "
                        + "naming one protocol and carrying another's is decoded against the "
                        + "wrong description");
    }

    @Test
    void anIdentityCallNamesIdentityEvenWhenItIsRefused() throws Exception {
        // Refused, because the raw path carries no authenticated principal. A refusal is still a
        // dispatch and still logs -- and it is the record an operator reads when diagnosing who
        // is probing the credential oracle, so attributing it to the application protocol hides
        // it in the busiest bucket on the dashboard.
        Capture cap = new Capture();
        RpcServer srv = identityServer();
        srv.setDispatchHook(cap);
        serveRaw(srv, request(Identity.PROTOCOL_NAME, "resolve_token",
                utf8("token"), Map.of("token", "good")));

        DispatchInfo rec = cap.only();
        assertEquals(Identity.PROTOCOL_NAME, rec.protocol);
        assertEquals(Reflection.bindingHash(Identity.PROTOCOL_NAME, srv.identityMethodTable()),
                rec.protocolHash);
        assertNotEquals(srv.protocolHash(), rec.protocolHash);
    }

    @Test
    void aCallToEachProtocolProducesExactlyOneRecord() throws Exception {
        // The end half of the hook is the half a new dispatch path forgets, and a missing
        // onDispatchEnd is invisible: the record simply never appears, and an absent record
        // looks like an absent call.
        Capture cap = new Capture();
        RpcServer srv = identityServer();
        srv.setDispatchHook(cap);
        serveRaw(srv, request("Ledger", "echo", utf8("value"), Map.of("value", "hi")));
        serveRaw(srv, request(Reflection.PROTOCOL_NAME, "list_protocols",
                new Schema(List.of()), Map.of()));
        serveRaw(srv, request(Identity.PROTOCOL_NAME, "resolve_token",
                utf8("token"), Map.of("token", "good")));

        assertEquals(3, cap.started.size(), "one start per dispatch");
        assertEquals(3, cap.ended.size(), "one end per dispatch");
        assertEquals(List.of("Ledger", Reflection.PROTOCOL_NAME, Identity.PROTOCOL_NAME),
                cap.ended.stream().map(i -> i.protocol).toList());
    }

    // --- the structural guard ------------------------------------------------

    /**
     * Every {@link DispatchInfo} built anywhere in this library takes both identity fields from
     * {@link RpcServer#protocolIdentityFor}.
     *
     * <p>The behavioural tests above cover the sites that exist. This one covers the site somebody
     * adds next. The failure mode is precisely "a new emit site reads the server's primary
     * instead", and it is invisible to every test that does not call a secondary protocol -- so
     * without a structural check, an added site reintroduces the bug with the suite green. A
     * source scan rather than reflection because what is being asserted is about the code, not
     * about a run of it: a site that is simply never exercised still has to be right.
     */
    @Test
    void everyDispatchInfoTakesItsIdentityFromTheAccessor() throws Exception {
        Path root = mainSourceRoot();
        List<String> offenders = new ArrayList<>();
        try (Stream<Path> files = Files.walk(root)) {
            for (Path file : files.filter(p -> p.toString().endsWith(".java")).toList()) {
                List<String> lines = Files.readAllLines(file);
                for (int i = 0; i < lines.size(); i++) {
                    if (!lines.get(i).contains("new DispatchInfo()")) continue;
                    // The construction and its field assignments sit in one block; 40 lines is
                    // well past the longest of them and well short of the next method.
                    String window = String.join("\n",
                            lines.subList(i, Math.min(lines.size(), i + 40)));
                    if (!window.contains("protocolIdentityFor(")) {
                        offenders.add(file.getFileName() + ":" + (i + 1));
                    }
                }
            }
        }
        assertTrue(offenders.isEmpty(),
                () -> "these DispatchInfo constructions do not read the owning binding's identity "
                        + "via RpcServer.protocolIdentityFor, so the access records they produce "
                        + "may name one protocol and carry another's digest: " + offenders);
    }

    /** The guard is only worth having if it can see the sites it is guarding. */
    @Test
    void theStructuralGuardActuallyFindsSomethingToCheck() throws Exception {
        Path root = mainSourceRoot();
        int sites = 0;
        try (Stream<Path> files = Files.walk(root)) {
            for (Path file : files.filter(p -> p.toString().endsWith(".java")).toList()) {
                for (String line : Files.readAllLines(file)) {
                    if (line.contains("new DispatchInfo()")) sites++;
                }
            }
        }
        assertTrue(sites >= 2,
                "expected at least the raw-dispatch and HTTP-stream emit sites, found " + sites
                        + " -- a scan that matches nothing passes vacuously");
    }

    /** Locate {@code src/main/java} from wherever the test runner set the working directory. */
    private static Path mainSourceRoot() {
        Path dir = Path.of("").toAbsolutePath();
        for (int i = 0; i < 6 && dir != null; i++, dir = dir.getParent()) {
            for (Path candidate : List.of(dir.resolve("src/main/java"),
                    dir.resolve("vgirpc/src/main/java"))) {
                if (Files.isDirectory(candidate)) return candidate;
            }
        }
        throw new IllegalStateException(
                "could not locate src/main/java from " + Path.of("").toAbsolutePath());
    }

    // --- harness -------------------------------------------------------------

    /** Records every dispatch the server reports, in order. */
    private static final class Capture implements DispatchHook {
        final List<DispatchInfo> started = new ArrayList<>();
        final List<DispatchInfo> ended = new ArrayList<>();

        @Override public Object onDispatchStart(DispatchInfo info) {
            started.add(info);
            return "token";
        }

        @Override public void onDispatchEnd(Object token, DispatchInfo info,
                                            CallStatistics stats, Throwable error) {
            assertEquals("token", token, "the end callback must receive the start's token");
            ended.add(info);
        }

        DispatchInfo only() {
            assertEquals(1, ended.size(), () -> "expected exactly one record, got " + ended.size());
            assertFalse(ended.get(0).protocol.isEmpty(), "a record must name a protocol");
            return ended.get(0);
        }
    }

    private static RpcServer identityServer() {
        RpcServer srv = new RpcServer(Ledger.class, new LedgerImpl(), "srv-ident");
        srv.setIdentity(IdentityImpl.builder()
                .resolveToken(token -> "good".equals(token) ? new TokenIdentity("bob", "ci") : null)
                .introspectPrincipals("alice")
                .build());
        return srv;
    }

    private static Schema utf8(String name) {
        return new Schema(List.of(
                new Field(name, FieldType.notNullable(new ArrowType.Utf8()), null)));
    }

    private static byte[] request(String protocol, String method, Schema params,
                                  Map<String, Object> args) throws Exception {
        Map<String, String> meta = Wire.requestMetadata(method);
        meta.put(Metadata.PROTOCOL, protocol);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (IpcStreamWriter w = new IpcStreamWriter(out)) {
            w.writeSchema(params);
            if (params.getFields().isEmpty()) {
                try (VectorSchemaRoot zero = VectorSchemaRoot.create(params, Allocators.root())) {
                    zero.setRowCount(1);
                    w.writeBatch(zero, meta);
                }
            } else {
                try (Marshalling.EncodedRow enc = Marshalling.encodeRowForWire(
                        params, new LinkedHashMap<>(args), Allocators.root())) {
                    w.writeBatch(enc.root(), meta, enc.provider());
                }
            }
        }
        return out.toByteArray();
    }

    private static void serveRaw(RpcServer rpc, byte[] body) {
        ByteArrayOutputStream response = new ByteArrayOutputStream();
        RpcTransport transport = new RpcTransport() {
            private final InputStream in = new ByteArrayInputStream(body);
            @Override public InputStream reader() { return in; }
            @Override public OutputStream writer() { return response; }
            @Override public void close() { }
        };
        rpc.serveOne(transport);
    }
}
