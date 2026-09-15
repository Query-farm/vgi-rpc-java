// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

import farm.query.vgirpc.external.ExternalLocationConfig;
import farm.query.vgirpc.external.Externalizer;
import farm.query.vgirpc.external.LocationResolver;
import farm.query.vgirpc.http.SessionScope;
import farm.query.vgirpc.identity.Identity;
import farm.query.vgirpc.identity.IdentityImpl;
import farm.query.vgirpc.log.Message;
import farm.query.vgirpc.marshal.Marshalling;
import farm.query.vgirpc.marshal.ParameterBinder;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.schema.SchemaDerivation;
import farm.query.vgirpc.shm.ShmResolver;
import farm.query.vgirpc.shm.Shm;
import farm.query.vgirpc.shm.ShmSession;
import farm.query.vgirpc.transport.RpcTransport;
import farm.query.vgirpc.transport.TcpSocketTransport;
import farm.query.vgirpc.transport.UnixSocketTransport;
import farm.query.vgirpc.wire.Allocators;
import farm.query.vgirpc.wire.IpcStreamReader;
import farm.query.vgirpc.wire.IpcStreamWriter;
import farm.query.vgirpc.wire.Metadata;
import farm.query.vgirpc.wire.OversizedMessageException;
import farm.query.vgirpc.wire.Wire;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;

/** Dispatches RPC requests to a service implementation over an {@link RpcTransport}. */
public final class RpcServer {

    private static final Logger LOG = LoggerFactory.getLogger(RpcServer.class);

    private final Class<?> serviceInterface;
    private final Object impl;
    private final String serverId;
    private final Map<String, RpcMethodInfo> methods;
    private final boolean describeEnabled;
    private LocationResolver locationResolver;
    private ExternalLocationConfig externalConfig;
    private DispatchHook dispatchHook;
    private IdentityImpl identity;
    private Map<String, RpcMethodInfo> identityMethods = Map.of();
    private String protocolVersion = "";
    private String protocolHash = "";
    private final Object transportLock = new Object();
    private volatile TransportKind transportKind;
    private Consumer<TransportKind> serveStartHook;

    /**
     * Create a server with a random 12-char server id and {@code __describe__} enabled.
     *
     * @param serviceInterface the service interface introspected for method schemas
     * @param impl the implementation instance calls are dispatched to
     */
    public RpcServer(Class<?> serviceInterface, Object impl) {
        this(serviceInterface, impl, UUID.randomUUID().toString().replace("-", "").substring(0, 12), true);
    }

    /**
     * Create a server with an explicit server id.
     *
     * @param serviceInterface the service interface introspected for method schemas
     * @param impl the implementation instance calls are dispatched to
     * @param serverId stable identifier echoed in response metadata
     * @param enableDescribe whether to answer the built-in {@code __describe__} introspection call
     */
    public RpcServer(Class<?> serviceInterface, Object impl, String serverId, boolean enableDescribe) {
        this.serviceInterface = serviceInterface;
        this.impl = impl;
        this.serverId = serverId;
        this.methods = new LinkedHashMap<>(ServiceIntrospector.describe(serviceInterface));
        this.describeEnabled = enableDescribe;
    }

    /**
     * Attach an external-location resolver so streaming inputs with pointer
     * batches are fetched transparently.
     *
     * @param r resolver that turns pointer batches into the real data batches
     */
    public void setLocationResolver(LocationResolver r) { this.locationResolver = r; }

    /**
     * Return the configured inbound external-location resolver.
     *
     * <p>The HTTP stream transport dispatches outside {@link #serveOne} and
     * therefore must resolve pointer batches explicitly on its init and
     * exchange routes.</p>
     *
     * @return the resolver, or {@code null} when inbound external locations are disabled
     */
    public LocationResolver locationResolver() { return locationResolver; }

    /**
     * Configure outgoing-batch externalisation. Uploaded via
     * {@link farm.query.vgirpc.external.ExternalStorage}.
     *
     * @param cfg externalisation thresholds and storage backend; {@code null} disables
     */
    public void setExternalConfig(ExternalLocationConfig cfg) { this.externalConfig = cfg; }

    /** Read-only accessor used by the HTTP transport to decide whether the
     *  server should advertise {@code VGI-Externalization-Enabled: true}.
     *
     *  @return the configured externalisation settings, or {@code null} when disabled */
    public ExternalLocationConfig externalConfig() { return externalConfig; }

    /**
     * Install an observability hook fired around each RPC dispatch.
     *
     * @param hook the hook to invoke (e.g. {@link AccessLogHook}); {@code null} removes it
     */
    public void setDispatchHook(DispatchHook hook) { this.dispatchHook = hook; }

    /**
     * Host {@code vgi_rpc.Identity.v1} alongside the application protocol.
     *
     * <p>Absent by default, and <em>absent</em> rather than routed-and-refusing when omitted:
     * that is what keeps a dependency upgrade from growing a credential-to-identity oracle on
     * every existing worker. Registered after reflection so it appears in reflection's output,
     * and narrowed to the methods whose hooks the deployment actually configured -- a worker that
     * resolves credentials but does not mint grants hosts one method, and its protocol hash says
     * so, because a server offering half the methods is not offering the same surface.
     *
     * <p>Passing an implementation that offers no method registers nothing at all.
     *
     * @param impl the guard-applying implementation, or {@code null} to host nothing
     */
    public void setIdentity(IdentityImpl impl) {
        Map<String, RpcMethodInfo> narrowed = impl == null ? Map.of() : identityMethods(impl);
        this.identity = narrowed.isEmpty() ? null : impl;
        this.identityMethods = narrowed;
    }

    /**
     * The identity implementation this server hosts, if any.
     *
     * @return the implementation, or {@code null} when the protocol is not hosted
     */
    public IdentityImpl identity() { return identity; }

    /**
     * The {@code vgi_rpc.Identity.v1} method table this server actually hosts.
     *
     * <p>Narrowed to the methods whose hooks the deployment configured, so a transport routing on
     * the path answers "no such method" for an unconfigured one -- the same answer the raw
     * dispatch gives, from the same table.
     *
     * @return the narrowed table; empty when the protocol is not hosted
     */
    public Map<String, RpcMethodInfo> identityMethodTable() {
        return identityMethods;
    }

    /**
     * The server-level reserved method names this server answers.
     *
     * <p>Reserved names are owned by no protocol and are routed flat, so a transport routing on
     * the path needs the set to tell a reserved name this server offers from one it does not.
     * {@code __describe__} is in it only when describe is enabled: a client probing for optional
     * introspection needs the capability answer, not a routing complaint.
     *
     * @return the reserved names, which are never protocol-namespaced
     */
    public java.util.Set<String> reservedMethodNames() {
        return describeEnabled
                ? java.util.Set.of(Introspect.METHOD_NAME, TransportOptions.METHOD_NAME)
                : java.util.Set.of(TransportOptions.METHOD_NAME);
    }

    /**
     * The {@code vgi_rpc.Identity.v1} method table {@code impl} hosts.
     *
     * <p>Narrowing the method set narrows the protocol hash with it, which is the point: a client
     * discovers a worker that cannot mint grants by reflecting on it, rather than by calling and
     * reading an error.
     *
     * @param impl the implementation whose configured hooks decide the method set
     * @return the narrowed table, in the interface's declaration order
     */
    public static Map<String, RpcMethodInfo> identityMethods(IdentityImpl impl) {
        Set<String> offered = impl.offeredMethods();
        Map<String, RpcMethodInfo> out = new LinkedHashMap<>();
        for (Map.Entry<String, RpcMethodInfo> e
                : ServiceIntrospector.describe(Identity.class).entrySet()) {
            if (offered.contains(e.getKey())) out.put(e.getKey(), e.getValue());
        }
        return Collections.unmodifiableMap(out);
    }

    /**
     * The installed dispatch hook, if any.
     *
     * <p>Exposed so transports that dispatch outside {@link #serveOne} — the
     * HTTP stream handler runs its own init/continuation turns — can fire the
     * same hook and produce the same access-log records.
     *
     * @return the hook, or {@code null} when none is installed
     */
    public DispatchHook dispatchHook() { return dispatchHook; }

    /**
     * Operator-supplied free-form protocol-contract version label (optional).
     *
     * @param v the label advertised as {@code vgi_rpc.protocol_version} in
     *     {@code __describe__}; {@code null} becomes {@code ""}
     */
    public void setProtocolVersion(String v) { this.protocolVersion = v == null ? "" : v; }
    /**
     * The operator-supplied protocol-contract version label.
     *
     * @return the label set via {@link #setProtocolVersion}, or {@code ""} if unset
     */
    public String protocolVersion() { return protocolVersion; }

    /**
     * SHA-256 hex digest of the canonical {@code __describe__} payload,
     * computed lazily and cached. Stable within this port; cross-port byte
     * equality with Python is not guaranteed.
     *
     * @return the hex-encoded protocol hash advertised as
     *     {@code vgi_rpc.protocol_hash} in {@code __describe__} responses
     */
    public String protocolHash() {
        if (protocolHash.isEmpty()) {
            Introspect.Built b = Introspect.build(protocolName(), methods, serverId, protocolVersion);
            try {
                protocolHash = b.customMetadata().getOrDefault(Metadata.PROTOCOL_HASH_KEY, "");
            } finally {
                b.root().close();
            }
        }
        return protocolHash;
    }

    /**
     * The stable server identifier echoed in response metadata.
     *
     * @return the server id passed to (or generated by) the constructor
     */
    public String serverId() { return serverId; }
    /**
     * The protocol name advertised in {@code __describe__}.
     *
     * @return the service interface's simple name
     */
    public String protocolName() { return ServiceIntrospector.protocolName(serviceInterface); }
    /**
     * Introspected method table for the service interface.
     *
     * @return an unmodifiable map from method name to {@link RpcMethodInfo}
     */
    public Map<String, RpcMethodInfo> methods() { return Collections.unmodifiableMap(methods); }

    /**
     * Underlying service implementation (exposed for the HTTP streaming handler).
     *
     * @return the implementation instance calls are dispatched to
     */
    public Object implementation() { return impl; }

    /** Install an optional one-shot hook for each newly selected transport kind. */
    public void setServeStartHook(Consumer<TransportKind> hook) { this.serveStartHook = hook; }

    /** Current transport kind, or {@code null} before serving begins. */
    public TransportKind transportKind() { return transportKind; }

    /**
     * Bind this server to {@code kind} and fire the configured startup hook.
     * State is committed only after the hook succeeds, so a transient failure
     * is retried on the next request. Calls for an already-bound kind are no-ops.
     *
     * @param kind concrete transport selected by the integration
     */
    public void notifyTransport(TransportKind kind) {
        if (transportKind == kind) return;
        synchronized (transportLock) {
            if (transportKind == kind) return;
            Consumer<TransportKind> hook = serveStartHook;
            if (hook != null) hook.accept(kind);
            transportKind = kind;
        }
    }

    private void notifyRawTransport(RpcTransport transport) {
        if (transportKind != null) return;
        if (transport instanceof UnixSocketTransport) notifyTransport(TransportKind.UNIX);
        else if (transport instanceof TcpSocketTransport) notifyTransport(TransportKind.TCP);
        else notifyTransport(TransportKind.PIPE);
    }

    /**
     * Loop serving requests until the transport closes. A single shared-memory
     * session is held for the lifetime of the connection. Recoverable per-call
     * errors are reported back to the client as error streams without tearing
     * down the loop.
     *
     * @param transport the transport whose request/response streams are served
     */
    public void serve(RpcTransport transport) {
        notifyRawTransport(transport);
        // One shared-memory session per connection: lazily attaches when the
        // client advertises a segment, and is munmap'd/closed when the loop
        // exits (never unlinked — the client owns the segment).
        try (ShmSession shm = new ShmSession()) {
            while (true) {
                try {
                    serveCall(transport, shm, null);
                } catch (EndOfStream e) {
                    return;
                } catch (Throwable t) {
                    // Don't take the whole loop down on a single bad request — try to surface
                    // the error to the client and continue serving subsequent calls.
                    LOG.warn("recoverable serve error: {}", t.toString(), t);
                    t.printStackTrace(System.err);
                    try {
                        Wire.writeErrorStream(transport.writer(), RpcStream.EMPTY_SCHEMA, t, serverId);
                        transport.writer().flush();
                    } catch (Exception ignore) {
                        // If we can't even write back, the transport is dead — exit.
                        return;
                    }
                }
            }
        }
    }

    private static final class EndOfStream extends RuntimeException {}

    /**
     * Serialize a parameters {@link VectorSchemaRoot} (with its custom metadata) into a
     * self-contained Arrow IPC stream — schema message followed by one record batch
     * message — for inclusion in access-log {@code request_data}.
     *
     * <p>Best-effort: returns {@code null} on any failure so observability never fails
     * dispatch.
     */
    private static byte[] serializeRequestBatch(VectorSchemaRoot root, Map<String, String> meta,
                                                 DictionaryProvider dictionaries) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (IpcStreamWriter w = new IpcStreamWriter(baos)) {
                // Dictionary-encoded params (enums) need their dictionary batches
                // in the stream, or the writer refuses and the record loses
                // request_data entirely — which the access-log schema reads as a
                // missing required property on every enum-taking method.
                w.writeBatch(root, meta, dictionaries);
                w.writeEos();
            }
            return baos.toByteArray();
        } catch (Throwable t) {
            return null;
        }
    }

    /**
     * Handle exactly one RPC call (no shared-memory session — used by the HTTP transport).
     *
     * @param transport the transport whose next request stream is read and answered
     */
    public void serveOne(RpcTransport transport) {
        notifyRawTransport(transport);
        serveCall(transport, null, null);
    }

    /**
     * Handle exactly one RPC call whose protocol was already resolved by the transport.
     *
     * <p>HTTP routes on {@code {prefix}/{protocol}/{method}}, so by the time a request reaches
     * here the binding is settled and the path is a carrier of the routing key in its own right.
     * The metadata field stays canonical — it is the only carrier on stdio, unix and named pipes
     * — so when the request also names a protocol the two must agree; when it does not, this is
     * the single-carrier case and the path answers for it. That asymmetry is deliberate: a
     * request that named nothing <em>and</em> arrived on a transport that names nothing is the
     * one the required-key rule exists to reject.
     *
     * @param transport the transport whose next request stream is read and answered
     * @param routedProtocol the protocol the transport resolved from the request path
     */
    public void serveOne(RpcTransport transport, String routedProtocol) {
        notifyRawTransport(transport);
        serveCall(transport, null, routedProtocol);
    }

    /** Handle exactly one RPC call, attaching/using the connection's shm session if present. */
    private void serveCall(RpcTransport transport, ShmSession shmSession, String routedProtocol) {
        // Opened per call so a pipe/unix/TCP transport — which has no request
        // boundary of its own — still reports a raised method as status="error"
        // in the access log. Under HTTP the servlet already installed one
        // spanning the whole request, and this nests inertly inside it so the
        // signal outlives dispatch and reaches the response headers.
        try (CallOutcome outcome = CallOutcome.open();
             IpcStreamReader reader = new IpcStreamReader(transport.reader(), Allocators.root())) {
            Map<String, String> meta;
            try {
                meta = reader.readNextBatch();
            } catch (OversizedMessageException oversized) {
                // A body this runtime cannot hold — over INT_MAX, so no heap or
                // allocator limit rescues it. The reader has already drained the
                // bytes, so the stream is back on a frame boundary and this is an
                // error for THIS call only: answer it and let the loop serve the
                // next one. Falling through to the generic IOException branch
                // would report end-of-stream and hang up on a live connection,
                // which is precisely the wedge the conformance suite checks for.
                LOG.warn("refusing oversized request body: {}", oversized.toString());
                // The reader drained the body, but the caller's request stream is
                // schema + batch + EOS: the marker its writer.close() emitted is
                // still queued. Every other exit from this method drains to it
                // (below); skipping that here leaves the marker to be read as the
                // next call's schema, and the connection dies one call later —
                // looking exactly like the wedge this branch exists to avoid.
                try { reader.drain(); } catch (IOException ignore) { /* best-effort */ }
                Wire.writeErrorStream(transport.writer(), RpcStream.EMPTY_SCHEMA, oversized, serverId);
                transport.writer().flush();
                return;
            } catch (RuntimeException malformed) {
                // Arrow Java decodes custom_metadata while loading the record
                // batch.  Invalid UTF-8 therefore surfaces before a metadata
                // map exists, but the batch frame itself has already been
                // consumed. Drain its EOS marker, return a typed error for
                // this call, and preserve the persistent connection.
                try { reader.drain(); } catch (Exception ignore) { /* best-effort */ }
                Wire.writeErrorStream(transport.writer(), RpcStream.EMPTY_SCHEMA, malformed, serverId);
                transport.writer().flush();
                return;
            } catch (IOException ioe) {
                throw new EndOfStream();
            }
            if (meta == null) {
                throw new EndOfStream();
            }
            if (shmSession != null) shmSession.attachIfAdvertised(meta);
            Shm shm = shmSession != null ? shmSession.segment() : null;
            VectorSchemaRoot paramsRoot = reader.root();
            Schema requestSchema;
            try {
                requestSchema = reader.wireSchema();
            } catch (IOException schemaExc) {
                try { reader.drain(); } catch (IOException ignore) { /* best-effort */ }
                Wire.writeErrorStream(transport.writer(), RpcStream.EMPTY_SCHEMA, schemaExc, serverId);
                transport.writer().flush();
                return;
            }
            // If the outer batch is a shm or external-location pointer, resolve the
            // inner batch and decode kwargs from it. Dispatch metadata still comes
            // from the (merged) outer batch metadata.
            VectorSchemaRoot resolvedParams = null;
            try {
                if (shm != null && ShmResolver.isPointer(paramsRoot.getRowCount(), meta)) {
                    try {
                        shm.inShmBatches++;
                        shm.inShmBytes += Long.parseLong(meta.get(Metadata.SHM_LENGTH));
                        ShmResolver.Resolved res = ShmResolver.resolve(shm, paramsRoot, meta);
                        resolvedParams = res.root();
                        paramsRoot = resolvedParams;
                        meta = res.customMetadata();
                    } catch (Exception shmExc) {
                        Wire.writeErrorStream(transport.writer(), RpcStream.EMPTY_SCHEMA, shmExc, serverId);
                        transport.writer().flush();
                        return;
                    }
                } else if (locationResolver != null && LocationResolver.isPointer(paramsRoot.getRowCount(), meta)) {
                    try {
                        LocationResolver.Resolved res = locationResolver.resolve(meta);
                        resolvedParams = res.root();
                        paramsRoot = resolvedParams;
                    } catch (Exception fetchExc) {
                        Wire.writeErrorStream(transport.writer(), RpcStream.EMPTY_SCHEMA, fetchExc, serverId);
                        transport.writer().flush();
                        return;
                    }
                } else if (ShmResolver.isPointer(paramsRoot.getRowCount(), meta)) {
                    // An inbound shm pointer arrived but no segment is attached. This
                    // can only happen if the client used shm without honoring the
                    // __transport_options__ handshake (e.g. a worker that reported
                    // shm-unavailable, or a capable worker whose attach failed). Fail
                    // loudly rather than silently decoding the 0-row pointer as empty.
                    Wire.writeErrorStream(transport.writer(), RpcStream.EMPTY_SCHEMA,
                            new IOException("received shm pointer batch but no segment is attached "
                                    + "(transport negotiation mismatch)"), serverId);
                    transport.writer().flush();
                    return;
                }
                int parameterRows = paramsRoot.getRowCount();
                // Snapshot the kwargs immediately, before draining mutates the reader's root.
                // Decode errors must be reported as an error response so the (possibly shared)
                // transport stays framed correctly for the next call.
                Map<String, Object> kwargsSnapshot;
                try {
                    kwargsSnapshot = paramsRoot.getRowCount() == 0
                            ? new LinkedHashMap<>()
                            : (resolvedParams != null
                                ? Marshalling.decodeRow(paramsRoot, null, paramsRoot.getSchema())
                                : Marshalling.decodeRow(paramsRoot, reader.dictionaryProvider(), reader.wireSchema()));
                } catch (Exception decodeExc) {
                    try { reader.drain(); } catch (IOException ignore) {}
                    Wire.writeErrorStream(transport.writer(), RpcStream.EMPTY_SCHEMA, decodeExc, serverId);
                    transport.writer().flush();
                    return;
                }
                // Snapshot request_data here for the same reason the kwargs are
                // snapshotted above: draining mutates the reader's root, so a
                // batch serialized after it carries zero rows. Only worth the
                // re-encode when a hook will actually consume it.
                byte[] requestDataSnapshot = dispatchHook == null ? null
                        : serializeRequestBatch(paramsRoot, meta,
                                resolvedParams != null ? null : reader.dictionaryProvider());
                // Drain remaining batches in this request stream so the next call sees a fresh stream
                try { reader.drain(); } catch (IOException ignore) {}
                String method;
                try {
                    Wire.validateRequestVersion(meta);
                    method = Wire.requireMethodName(meta);
                } catch (RuntimeException pe) {
                    Wire.writeErrorStream(transport.writer(), RpcStream.EMPTY_SCHEMA, pe, serverId);
                    transport.writer().flush();
                    return;
                }
                // Reflection is a co-hosted protocol, routed by the same key as
                // everything else and appearing in its own output. Handled
                // before the version gate because it is exempt from it: this is
                // what a version-mismatched client calls to learn what
                // mismatched, and gating it would deny the diagnosis it came
                // for.
                String requestProtocol = meta.get(farm.query.vgirpc.wire.Metadata.PROTOCOL);
                if (routedProtocol != null) {
                    // HTTP resolved the binding from the path. The metadata field is still
                    // canonical when present, so a disagreement is refused rather than silently
                    // preferring one carrier: unspecified, this is the
                    // Content-Length/Transfer-Encoding shape, where the edge applies policy to
                    // one protocol and the worker dispatches another.
                    if (requestProtocol != null && !requestProtocol.equals(routedProtocol)) {
                        Wire.writeErrorStream(transport.writer(), RpcStream.EMPTY_SCHEMA,
                                new ProtocolNotSupportedError(
                                        "Protocol mismatch: the request path resolved to '"
                                                + routedProtocol + "' but the Arrow IPC "
                                                + "custom_metadata 'vgi_rpc.protocol' says '"
                                                + requestProtocol + "'. These must agree."),
                                serverId);
                        transport.writer().flush();
                        return;
                    }
                    requestProtocol = routedProtocol;
                }
                if (Reflection.PROTOCOL_NAME.equals(requestProtocol)) {
                    serveReflection(transport, method, kwargsSnapshot);
                    return;
                }
                // Identity is co-hosted the same way, and for the same reason: it is
                // framework-owned, lives under the reserved prefix, and is routed by
                // the ordinary protocol key. Handled here, before the application
                // protocol's version gate, because that gate is a statement about the
                // application protocol -- identity declares no version of its own, and
                // gating it on somebody else's would make a proxy's ability to resolve
                // a credential depend on a worker upgrade it has no part in.
                //
                // Like reflection, and DELIBERATELY like reflection, identity calls
                // are not reported to the dispatch hook: returning here is ahead of
                // the access-log block below, so nothing is logged for them. That is
                // a decision, not an oversight. This port's DispatchInfo.protocol is
                // filled from protocolName(), which is the APPLICATION protocol --
                // so logging an identity call through the hook unchanged would file
                // it under the wrong protocol, and a wrong protocol field fails
                // silently: it yields a plausible dashboard rather than an error.
                // Other ports (C#) took the other road and added per-protocol
                // overrides to the access log. Doing that here means a change to
                // DispatchInfo and to every hook that reads it, which is a
                // cross-port change of its own rather than a detail of this one.
                // Until then: no record beats a confidently mislabelled record.
                if (identity != null && Identity.PROTOCOL_NAME.equals(requestProtocol)) {
                    serveIdentity(transport, method, kwargsSnapshot, requestSchema,
                            parameterRows, shm);
                    return;
                }
                if (describeEnabled && Introspect.METHOD_NAME.equals(method)) {
                    serveDescribe(transport);
                    return;
                }
                if (TransportOptions.METHOD_NAME.equals(method)) {
                    serveTransportOptions(transport);
                    return;
                }
                // Required, with no single-protocol exemption -- and checked only now, after the
                // reserved server-level built-ins above. Those belong to no protocol, and
                // __describe__ in particular is the diagnostic path a confused client reaches
                // for, so demanding a routing key first would remove the tool exactly when it is
                // needed. An application call that named nothing is refused: an intermediary
                // that rebuilds a request and drops the field must be told, not landed silently
                // on whichever protocol happens to be first.
                //
                // The name is NOT required to equal protocolName(). This port hosts exactly one
                // application protocol, so there is nothing to mis-route to, and peers whose
                // Protocol class name differs from the Java interface's simple name (the
                // conformance suite's transport-kind probe among them) would otherwise be
                // refused over a naming difference that costs nothing here.
                if (requestProtocol == null) {
                    if (method.startsWith("__") && method.endsWith("__")) {
                        // A reserved name this server does not offer -- __describe__ on a server
                        // built with describe disabled, say. The answer is "no such method", not
                        // "you failed to name a protocol": the caller did nothing wrong with
                        // routing, and a client probing for optional introspection needs the
                        // capability answer rather than a routing complaint.
                        Wire.writeErrorStream(transport.writer(), RpcStream.EMPTY_SCHEMA,
                                new MethodNotImplementedError(
                                        "This server does not implement the reserved method '"
                                                + method + "'."),
                                serverId);
                        transport.writer().flush();
                        return;
                    }
                    Wire.writeErrorStream(transport.writer(), RpcStream.EMPTY_SCHEMA,
                            new ProtocolNotSpecifiedError(
                                    "Request carries no 'vgi_rpc.protocol' routing key. Every "
                                            + "request must name the protocol it addresses. This "
                                            + "server hosts: " + hostedProtocolNames() + "."),
                            serverId);
                    transport.writer().flush();
                    return;
                }
                RpcMethodInfo info = methods.get(method);
                if (info == null) {
                    Wire.writeErrorStream(transport.writer(), RpcStream.EMPTY_SCHEMA,
                            new IllegalArgumentException("Unknown method: '" + method + "'. Available: " + methods.keySet()),
                            serverId);
                    transport.writer().flush();
                    return;
                }
                // Application-protocol-version gate. Fires only when the
                // operator declared a version via setProtocolVersion (empty =
                // opt out). Placed after method resolution so an unknown method
                // still reports as unknown, and after the __describe__ /
                // transport-options short-circuits above — those are the
                // diagnostic paths a MISMATCHED client uses to discover what the
                // server expects, so gating them would hide the answer.
                if (!protocolVersion.isEmpty()) {
                    ProtocolVersionError pve =
                            checkProtocolVersion(meta.get(farm.query.vgirpc.wire.Metadata.PROTOCOL_VERSION_KEY));
                    if (pve != null) {
                        Schema errorSchema = info.methodType() == MethodType.UNARY
                                ? info.resultSchema() : RpcStream.EMPTY_SCHEMA;
                        Wire.writeErrorStream(transport.writer(), errorSchema, pve, serverId);
                        transport.writer().flush();
                        return;
                    }
                }
                try {
                    validateParameterContract(method, requestSchema,
                            parameterRows, info.paramsSchema());
                } catch (RuntimeException contractError) {
                    Schema errorSchema = info.methodType() == MethodType.UNARY
                            ? info.resultSchema() : RpcStream.EMPTY_SCHEMA;
                    Wire.writeErrorStream(transport.writer(), errorSchema, contractError, serverId);
                    transport.writer().flush();
                    return;
                }
                Map<String, Object> kwargs = kwargsSnapshot;

                // Build dispatch info + invoke hook
                DispatchInfo dispatchInfo = null;
                CallStatistics callStats = null;
                Object hookToken = null;
                Throwable handlerErr = null;
                if (dispatchHook != null) {
                    dispatchInfo = new DispatchInfo();
                    dispatchInfo.method = method;
                    dispatchInfo.methodType = info.methodType() == MethodType.UNARY ? "unary" : "stream";
                    dispatchInfo.serverId = serverId;
                    dispatchInfo.protocol = protocolName();
                    dispatchInfo.protocolHash = protocolHash();
                    dispatchInfo.protocolVersion = protocolVersion;
                    AuthScope.Scope scope = AuthScope.current();
                    dispatchInfo.principal = scope.auth() != null && scope.auth().principal() != null ? scope.auth().principal() : "";
                    dispatchInfo.authDomain = scope.auth() != null && scope.auth().domain() != null ? scope.auth().domain() : "";
                    dispatchInfo.authenticated = scope.auth() != null && scope.auth().authenticated();
                    dispatchInfo.claims = scope.auth() != null ? scope.auth().claims() : null;
                    dispatchInfo.transportMetadata = scope.transportMetadata();
                    dispatchInfo.requestData = requestDataSnapshot;
                    if ("stream".equals(dispatchInfo.methodType)) {
                        dispatchInfo.streamId = AccessLogHook.randomStreamId();
                    }
                    callStats = new CallStatistics();
                    try {
                        hookToken = dispatchHook.onDispatchStart(dispatchInfo);
                    } catch (Throwable t) {
                        LOG.warn("dispatch hook start error: {}", t.toString());
                    }
                }

                try {
                    if (info.methodType() == MethodType.UNARY) {
                        serveUnary(transport, info, kwargs, shm);
                    } else {
                        serveStream(transport, info, kwargs, shm);
                    }
                } catch (Throwable t) {
                    handlerErr = t;
                    throw t;
                } finally {
                    if (dispatchHook != null && dispatchInfo != null) {
                        // Snapshot sticky-session state at end-of-dispatch so the access
                        // log reflects open/resume/close that happened during the call.
                        SessionScope scope = SessionScope.current();
                        if (scope != null) {
                            dispatchInfo.sessionId = scope.sessionIdHex();
                            dispatchInfo.sessionAction = scope.action();
                        }
                        try {
                            dispatchHook.onDispatchEnd(hookToken, dispatchInfo, callStats, handlerErr);
                        } catch (Throwable t) {
                            LOG.warn("dispatch hook end error: {}", t.toString());
                        }
                    }
                }
                transport.writer().flush();
            } finally {
                if (resolvedParams != null) resolvedParams.close();
            }
        } catch (EndOfStream e) {
            throw e;
        } catch (Exception e) {
            LOG.warn("serve error: {}", e.toString());
            e.printStackTrace(System.err);
        }
    }

    /**
     * Whether an incoming parameter schema satisfies the declared one.
     *
     * <p>Not plain equality, because a DICTIONARY-ENCODED utf8 column satisfies a
     * declared plain utf8 one. This library deliberately declares enum-shaped
     * fields as plain utf8 and resolves dict-encoded values on read (see
     * {@code SchemaDerivation}) — but the contract check was exact, so a client
     * sending the dictionary encoding it was entitled to send was rejected before
     * the tolerant reader ever saw it. The reader's tolerance was unreachable.</p>
     *
     * @param actual the schema the peer sent
     * @param expected the schema derived from the service interface
     * @return whether the call may proceed
     */
    private static boolean schemasCompatible(Schema actual, Schema expected) {
        if (actual.equals(expected)) return true;
        List<Field> a = actual.getFields();
        List<Field> e = expected.getFields();
        if (a.size() != e.size()) return false;
        for (int i = 0; i < a.size(); i++) {
            Field af = a.get(i);
            Field ef = e.get(i);
            if (!fieldsCompatible(af, ef)) return false;
        }
        return true;
    }

    private static boolean fieldsCompatible(Field actual, Field expected) {
        if (actual.equals(expected)) return true;
        if (!actual.getName().equals(expected.getName())
                || actual.isNullable() != expected.isNullable()) return false;
        boolean dictionaryUtf8 =
                (actual.getDictionary() != null || expected.getDictionary() != null)
                && actual.getType() instanceof org.apache.arrow.vector.types.pojo.ArrowType.Utf8
                && expected.getType() instanceof org.apache.arrow.vector.types.pojo.ArrowType.Utf8;
        if (!dictionaryUtf8 && !actual.getType().equals(expected.getType())) return false;
        if (actual.getChildren().size() != expected.getChildren().size()) return false;
        for (int i = 0; i < actual.getChildren().size(); i++) {
            if (!fieldsCompatible(actual.getChildren().get(i), expected.getChildren().get(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Validate a client's declared application {@code protocol_version} against
     * this server's.
     *
     * <p>An exact MAJOR.MINOR match is required; PATCH is ignored, so a patch
     * release on either side stays compatible. Missing and malformed are
     * mismatches too — a peer that cannot say what it speaks is not a peer that
     * can be trusted to speak it.</p>
     *
     * <p>The message is directional on purpose: "mismatch" alone leaves an
     * operator with two versions and no idea which side to move. It mirrors
     * vgi-rpc's Python, Go, Rust and TypeScript servers byte-for-byte, because
     * the same {@code .test} file asserts against all five.</p>
     *
     * @param clientVersion the version the client declared, or {@code null} when it declared none
     * @return the error to return to the caller, or {@code null} when the versions are compatible
     */
    private ProtocolVersionError checkProtocolVersion(String clientVersion) {
        String head = "VGI client/worker protocol_version mismatch.\n"
                + "  Client: " + (clientVersion == null ? "<not declared>" : clientVersion) + "\n"
                + "  Server: " + protocolVersion + "\n"
                + "  Direction: ";
        if (clientVersion == null) {
            return new ProtocolVersionError(head
                    + "the client did not send a vgi_rpc.protocol_version metadata key. "
                    + "This is either a vgi-rpc framework bug or a non-VGI client "
                    + "connecting to a VGI worker.");
        }
        int[] client = parseSemver(clientVersion);
        int[] server = parseSemver(protocolVersion);
        if (client == null || server == null) {
            return new ProtocolVersionError(head
                    + "client sent a malformed protocol_version. "
                    + "Expected canonical semver MAJOR.MINOR.PATCH.");
        }
        if (client[0] == server[0] && client[1] == server[1]) {
            return null;
        }
        boolean clientOlder = client[0] < server[0] || (client[0] == server[0] && client[1] < server[1]);
        return new ProtocolVersionError(head + (clientOlder
                ? "client is too old; upgrade the VGI extension/client to a version "
                        + "supporting protocol_version " + protocolVersion + "."
                : "server is too old; upgrade the VGI worker to a version "
                        + "supporting protocol_version " + clientVersion + "."));
    }

    /** {@code MAJOR.MINOR.PATCH} as three ints, or {@code null} when not canonical semver. */
    private static int[] parseSemver(String v) {
        if (v == null) return null;
        String[] parts = v.split("\\.");
        if (parts.length != 3) return null;
        int[] out = new int[3];
        for (int i = 0; i < 3; i++) {
            try {
                out[i] = Integer.parseInt(parts[i]);
            } catch (NumberFormatException e) {
                return null;
            }
            if (out[i] < 0) return null;
        }
        return out;
    }

    /** Validate the exact parameter schema/cardinality before handler dispatch. */
    private static void validateParameterContract(String method, Schema actualSchema,
                                                  int rows, Schema expectedSchema) {
        if (!schemasCompatible(actualSchema, expectedSchema)) {
            throw new ClassCastException(
                    "parameter schema mismatch for '" + method + "': expected "
                            + expectedSchema + ", got " + actualSchema);
        }
        // Existing clients use both Arrow encodings of an empty argument
        // tuple: zero rows, or one row in a zero-column batch.
        boolean validRows = expectedSchema.getFields().isEmpty()
                ? rows == 0 || rows == 1
                : rows == 1;
        if (!validRows) {
            throw new IllegalArgumentException(
                    "parameter batch for '" + method + "' must contain "
                            + (expectedSchema.getFields().isEmpty() ? "zero or one" : "one")
                            + " row(s), got " + rows);
        }
    }

    private void serveUnary(RpcTransport transport, RpcMethodInfo info, Map<String, Object> kwargs,
                            Shm shm) throws Exception {
        serveUnary(transport, info, kwargs, shm, impl, protocolName());
    }

    /**
     * Serve one unary call against {@code target}, reported as belonging to {@code proto}.
     *
     * <p>Parameterised on the target because a co-hosted framework protocol -- identity -- is
     * answered by an object that is not the application implementation. Everything else is
     * deliberately shared: an identity method must marshal, report errors and carry an error kind
     * exactly the way an application method does, and a second copy of this loop is a second
     * place for that to drift.
     */
    private void serveUnary(RpcTransport transport, RpcMethodInfo info, Map<String, Object> kwargs,
                            Shm shm, Object target, String proto) throws Exception {
        Schema schema = info.resultSchema();
        ClientLogSink sink = new ClientLogSink(serverId);
        AuthScope.Scope scope = AuthScope.current();
        try (IpcStreamWriter w = new IpcStreamWriter(transport.writer())) {
            w.writeSchema(schema);
            CallContext ctx = new CallContext(scope.auth(), sink, scope.transportMetadata(),
                    serverId, info.name(), proto, "", transportKind, scope.peerEvidence());
            sink.bind(w, schema);
            try {
                Object[] callArgs = ParameterBinder.bind(info.reflectMethod(), kwargs, ctx);
                Object result = info.reflectMethod().invoke(target, callArgs);
                writeResult(w, info, result, shm);
            } catch (Throwable t) {
                Throwable inner = unwrap(t);
                Wire.writeZeroBatch(w, schema, Wire.errorMetadata(inner, serverId));
            }
        }
    }

    /**
     * Serve one call to {@code vgi_rpc.Identity.v1}.
     *
     * <p>A method the deployment did not configure is not in {@link #identityMethods} at all, so
     * it reports as unknown rather than as a refusal -- the whole point of narrowing the binding.
     * {@link IdentityImpl} still guards each method individually: a narrowed binding is what a
     * client discovers, and the per-method guard is what actually holds if a caller reaches the
     * method some other way.
     */
    private void serveIdentity(RpcTransport transport, String method, Map<String, Object> kwargs,
                               Schema requestSchema, int parameterRows, Shm shm) throws Exception {
        RpcMethodInfo info = identityMethods.get(method);
        if (info == null) {
            Wire.writeErrorStream(transport.writer(), RpcStream.EMPTY_SCHEMA,
                    new MethodNotImplementedError(
                            "Protocol '" + Identity.PROTOCOL_NAME + "' has no method '" + method
                                    + "'. Available: " + identityMethods.keySet()),
                    serverId);
            transport.writer().flush();
            return;
        }
        try {
            validateParameterContract(method, requestSchema, parameterRows, info.paramsSchema());
        } catch (RuntimeException contractError) {
            Wire.writeErrorStream(transport.writer(), info.resultSchema(), contractError, serverId);
            transport.writer().flush();
            return;
        }
        serveUnary(transport, info, kwargs, shm, identity, Identity.PROTOCOL_NAME);
        transport.writer().flush();
    }

    private void writeResult(IpcStreamWriter w, RpcMethodInfo info, Object result, Shm shm) throws Exception {
        Schema schema = info.resultSchema();
        if (schema.getFields().isEmpty()) {
            Wire.writeZeroBatch(w, schema, null);
            return;
        }
        Field resultField = schema.getFields().get(0);
        Map<String, Object> row = new LinkedHashMap<>();
        row.put(resultField.getName(), convertResult(result, info.resultType(), resultField));
        try (Marshalling.EncodedRow encoded =
                     Marshalling.encodeRowForWire(schema, row, Allocators.root())) {
            VectorSchemaRoot root = encoded.root();
            // Prefer the shared-memory side-channel; fall back to external-location,
            // then inline. The client resolves unary-response shm pointers on the
            // FunctionConnection path (ResolveUnaryShm); connections that never
            // advertise a segment (catalog, bind) get shm == null → inline.
            ShmResolver.ShmPointer sp = ShmResolver.maybeWriteToShm(shm, root, null);
            if (sp != null) {
                try (VectorSchemaRoot pr = sp.root()) {
                    w.writeBatch(pr, sp.customMetadata());
                }
                return;
            }
            Externalizer.Pointer ptr = Externalizer.maybeExternalize(
                    root, null, externalConfig, null, encoded.provider());
            if (ptr != null) {
                try (VectorSchemaRoot pr = ptr.root()) {
                    w.writeBatch(pr, ptr.customMetadata(), encoded.provider());
                }
            } else {
                w.writeBatch(root, null, encoded.provider());
            }
        }
    }

    private Object convertResult(Object value, Type resultType, Field field) {
        if (value instanceof Optional<?> opt) value = opt.orElse(null);
        if (value == null) return null;
        // Records-as-binary: serialize whole record into bytes for the binary column
        if (value instanceof ArrowSerializableRecord r && field.getType() instanceof ArrowType.Binary) {
            return RecordCodec.serializeToBytes(r);
        }
        return value;
    }

    private void serveStream(RpcTransport transport, RpcMethodInfo info, Map<String, Object> kwargs,
                             Shm shm) throws Exception {
        ClientLogSink sink = new ClientLogSink(serverId);
        AuthScope.Scope scope = AuthScope.current();
        CallContext ctx = new CallContext(scope.auth(), sink, scope.transportMetadata(),
                serverId, info.name(), protocolName(), "", transportKind, scope.peerEvidence());

        RpcStream<?> stream = runStreamInit(info, kwargs, ctx, transport);
        if (stream == null) return;  // init failed; error already reported + input drained

        if (stream.header() != null) {
            writeHeaderStream(transport.writer(), stream.header(), sink);
            transport.writer().flush();
        }

        Schema outputSchema = stream.outputSchema();
        Schema inputSchema = stream.inputSchema();
        StreamState state = stream.state();
        boolean isProducer = stream.isProducer();

        try (IpcStreamReader inputReader = new IpcStreamReader(transport.reader(), Allocators.root())) {
            IpcStreamWriter outputWriter = new IpcStreamWriter(transport.writer());
            try {
                outputWriter.writeSchema(outputSchema);
                transport.writer().flush();
                sink.bind(outputWriter, outputSchema);

                runTickLoop(inputReader, outputWriter, transport, state, ctx,
                        outputSchema, inputSchema, isProducer, shm);
            } finally {
                closeStreamCleanly(outputWriter, transport, inputReader);
            }
        }
    }

    /**
     * Invoke the service method to obtain the {@link RpcStream}. On failure
     * write an error stream + drain the client's input and return {@code null};
     * callers should early-exit in that case.
     */
    private RpcStream<?> runStreamInit(RpcMethodInfo info, Map<String, Object> kwargs, CallContext ctx,
                                        RpcTransport transport) throws IOException {
        try {
            Object[] args = ParameterBinder.bind(info.reflectMethod(), kwargs, ctx);
            return (RpcStream<?>) info.reflectMethod().invoke(impl, args);
        } catch (Throwable t) {
            Throwable initException = unwrap(t);
            Wire.writeErrorStream(transport.writer(), RpcStream.EMPTY_SCHEMA, initException, serverId);
            transport.writer().flush();
            // Drain the client's input IPC stream so subsequent requests aren't misparsed.
            try (IpcStreamReader inputReader = new IpcStreamReader(transport.reader(), Allocators.root())) {
                try { inputReader.drain(); } catch (IOException ignore) { /* best-effort */ }
            }
            return null;
        }
    }

    /** Main tick loop: read input batch → resolve/cast → state.process → flush output, until EOS or finish. */
    private void runTickLoop(IpcStreamReader inputReader, IpcStreamWriter outputWriter, RpcTransport transport,
                             StreamState state, CallContext ctx, Schema outputSchema, Schema inputSchema,
                             boolean isProducer, Shm shm) throws IOException {
        while (true) {
            Map<String, String> meta;
            // Time spent blocked here is the worker idle waiting for the client to
            // send the next input — under lockstep that equals the client's own
            // (encode/consume) work + handoff latency.
            long tIdle0 = System.nanoTime();
            try {
                meta = inputReader.readNextBatch();
            } catch (IOException e) {
                break;
            }
            if (shm != null) shm.idleNs += System.nanoTime() - tIdle0;
            if (meta == null) break;
            if (meta.containsKey(Metadata.CANCEL)) {
                try { state.onCancel(ctx); } catch (Exception ignore) { /* best-effort */ }
                break;
            }
            long tBusy0 = System.nanoTime();
            boolean cont = processOneTick(inputReader, outputWriter, transport, state, ctx,
                    outputSchema, inputSchema, isProducer, meta, shm);
            if (shm != null) shm.busyNs += System.nanoTime() - tBusy0;
            if (!cont) break;
        }
    }

    /**
     * Handle one tick: resolve pointer / cast schema / invoke state.process / flush.
     * Returns {@code true} to continue the loop, {@code false} on terminal state (error or finish).
     */
    private boolean processOneTick(IpcStreamReader inputReader, IpcStreamWriter outputWriter, RpcTransport transport,
                                    StreamState state, CallContext ctx, Schema outputSchema, Schema inputSchema,
                                    boolean isProducer, Map<String, String> meta, Shm shm) throws IOException {
        VectorSchemaRoot inputRoot = inputReader.root();
        VectorSchemaRoot resolvedRoot = null;
        Map<String, String> effectiveMeta = meta;

        boolean inboundViaShm = shm != null && ShmResolver.isPointer(inputRoot.getRowCount(), meta);
        if (inboundViaShm) {
            try {
                shm.inShmBatches++;
                shm.inShmBytes += Long.parseLong(meta.get(Metadata.SHM_LENGTH));
                long tr0 = System.nanoTime();
                ShmResolver.Resolved res = ShmResolver.resolve(shm, inputRoot, meta);
                shm.resolveNs += System.nanoTime() - tr0;
                resolvedRoot = res.root();
                inputRoot = resolvedRoot;
                effectiveMeta = res.customMetadata();
            } catch (Exception shmExc) {
                Wire.writeZeroBatch(outputWriter, outputSchema, Wire.errorMetadata(shmExc, serverId));
                transport.writer().flush();
                return false;
            }
        } else if (locationResolver != null && LocationResolver.isPointer(inputRoot.getRowCount(), meta)) {
            try {
                LocationResolver.Resolved res = locationResolver.resolve(meta);
                resolvedRoot = res.root();
                inputRoot = resolvedRoot;
                effectiveMeta = res.customMetadata();
            } catch (Exception fetchExc) {
                Wire.writeZeroBatch(outputWriter, outputSchema, Wire.errorMetadata(fetchExc, serverId));
                transport.writer().flush();
                return false;
            }
        } else if (shm != null && resolvedRoot == null && inputRoot.getRowCount() > 0) {
            // A non-empty data batch arrived inline even though shm is active —
            // the client chose not to (or couldn't) put it in the segment.
            shm.inInlineDataBatches++;
        }

        VectorSchemaRoot castRoot = null;
        if (!isProducer && !inputRoot.getSchema().equals(inputSchema)) {
            try {
                castRoot = Marshalling.castRoot(inputRoot, inputSchema, Allocators.root());
                inputRoot = castRoot;
            } catch (Exception castExc) {
                Wire.writeZeroBatch(outputWriter, outputSchema, Wire.errorMetadata(
                        new ClassCastException(castExc.getMessage()), serverId));
                transport.writer().flush();
                return false;
            }
        }

        OutputCollector out = new OutputCollector(outputSchema, serverId, isProducer,
                ctx.responseLimitBytes(), ctx.preferredResponseBytes());
        long tp0 = System.nanoTime();
        try {
            state.process(new AnnotatedBatch(inputRoot, effectiveMeta), out, ctx);
        } catch (Throwable t) {
            Wire.writeZeroBatch(outputWriter, outputSchema, Wire.errorMetadata(unwrap(t), serverId));
            transport.writer().flush();
            return false;
        }
        if (shm != null) shm.processNs += System.nanoTime() - tp0;
        long te0 = System.nanoTime();
        flushCollector(outputWriter, out, inputReader.dictionaryProvider(), shm);
        transport.writer().flush();
        if (shm != null) shm.emitNs += System.nanoTime() - te0;
        if (castRoot != null) castRoot.close();
        if (resolvedRoot != null) resolvedRoot.close();
        return !out.finished();
    }

    /**
     * Close the output FIRST (sends EOS to client), then drain the client's
     * remaining input (until its EOS) so the transport is clean for the next
     * request. Closing output first breaks the client out of its reader;
     * otherwise draining deadlocks.
     */
    private static void closeStreamCleanly(IpcStreamWriter outputWriter, RpcTransport transport,
                                            IpcStreamReader inputReader) {
        try { outputWriter.close(); } catch (Exception ignore) { /* already-closed transport is fine */ }
        try { transport.writer().flush(); } catch (Exception ignore) { /* already-closed transport is fine */ }
        try { inputReader.drain(); } catch (IOException ignore) { /* client already gone */ }
    }

    /**
     * Threads the inbound stream's {@link
     * org.apache.arrow.vector.dictionary.DictionaryProvider} through to the
     * writer, so dict-encoded columns (DuckDB ENUMs) round-trip through
     * passthrough handlers like {@code echo} with their dictionaries
     * intact. Without this the consumer sees raw index columns and renders
     * them as unbound nulls. {@code shm}, when present, offloads non-dict data
     * batches to the shared-memory segment as zero-row pointer batches.
     */
    private void flushCollector(IpcStreamWriter writer, OutputCollector out,
                                  org.apache.arrow.vector.dictionary.DictionaryProvider dictProvider,
                                  Shm shm)
            throws IOException {
        for (OutputCollector.Entry e : out.entries()) {
            // Per-entry provider (set when a producer emits dict-encoded
            // batches) wins over the stream-level dictProvider (set by the
            // TIO/echo path so input dicts round-trip). Either is sufficient
            // for IpcStreamWriter to emit the dict batches alongside the
            // record batch.
            org.apache.arrow.vector.dictionary.DictionaryProvider effective =
                    e.dictionaryProvider() != null ? e.dictionaryProvider() : dictProvider;
            try {
                if (e.isData() && shm != null) {
                    ShmResolver.ShmPointer sp =
                            ShmResolver.maybeWriteToShm(shm, e.root(), e.customMetadata());
                    if (sp != null) {
                        shm.outShmBatches++;
                        shm.outShmBytes += Long.parseLong(sp.customMetadata().get(Metadata.SHM_LENGTH));
                        try (VectorSchemaRoot pr = sp.root()) {
                            writer.writeBatch(pr, sp.customMetadata(), effective);
                        }
                        continue;   // finally still closes e.root()
                    }
                    // Eligible for shm but fell back to inline (segment full / serialize
                    // failed) — the signal that intended-shm output leaked to the pipe.
                    if (ShmResolver.shmEligible(e.root())) {
                        shm.outInlineEligibleBatches++;
                    }
                }
                if (e.isData() && externalConfig != null && externalConfig.storage() != null) {
                    try {
                        Externalizer.Pointer ptr = Externalizer.maybeExternalize(
                                e.root(), e.customMetadata(), externalConfig, null, effective);
                        if (ptr != null) {
                            try (VectorSchemaRoot pr = ptr.root()) {
                                writer.writeBatch(pr, ptr.customMetadata(), effective);
                            }
                            continue;
                        }
                    } catch (farm.query.vgirpc.external.ExternalizedResponseCapExceededException cap) {
                        // Not an upload failure — an operator refusal. Falling back to
                        // inline would answer success for the very response the cap was
                        // configured to refuse, so it propagates and fails the stream.
                        throw cap;
                    } catch (Exception up) {
                        // Upload failed — fall through and write the batch inline rather than
                        // failing the stream. The client will still receive valid data.
                    }
                }
                writer.writeBatch(e.root(), e.customMetadata(), effective);
            } finally {
                e.root().close();
                // Per-entry providers own their dictionary vectors (the
                // producer that built them transferred ownership via emit).
                // The stream-level dictProvider is *not* closed here — its
                // lifecycle is owned by the input reader on the TIO path.
                if (e.dictionaryProvider() != null) {
                    for (long id : e.dictionaryProvider().getDictionaryIds()) {
                        org.apache.arrow.vector.dictionary.Dictionary d =
                                e.dictionaryProvider().lookup(id);
                        if (d != null && d.getVector() != null) {
                            try { d.getVector().close(); } catch (Exception ignore) {}
                        }
                    }
                }
            }
        }
    }

    private void serveDescribe(RpcTransport transport) throws IOException {
        Introspect.Built built = Introspect.build(protocolName(), methods, serverId, protocolVersion);
        try (IpcStreamWriter w = new IpcStreamWriter(transport.writer());
             VectorSchemaRoot root = built.root()) {
            w.writeBatch(root, built.customMetadata());
        } finally {
            transport.writer().flush();
        }
    }

    /**
     * Serve one call to {@code vgi_rpc.Reflection.v1}.
     *
     * <p>Two methods, deliberately. {@code list_protocols} is the cheap question -- what is here,
     * and has it changed -- and the only one a client needs on a warm path, because the hash
     * answers "has it changed" without transferring any schema. {@code describe} is the expensive
     * one, asked once.
     *
     * <p>Self-description is not special-cased: reflection appears in its own output, so a client
     * discovers it the same way it discovers everything else.
     */
    private void serveReflection(
            RpcTransport transport, String method, Map<String, Object> kwargs) throws IOException {
        String appHash = Reflection.bindingHash(protocolName(), methods);
        // Reflection describes itself with no methods of its own in the table:
        // they are framework-owned rather than registered, so the honest hash is
        // over an empty method set.
        Map<String, RpcMethodInfo> reflectionMethods = Map.of();
        String reflHash = Reflection.bindingHash(Reflection.PROTOCOL_NAME, reflectionMethods);
        // Identity's hash is taken over its NARROWED method table, so a worker
        // that only resolves credentials fingerprints differently from one that
        // also mints grants. That difference is the discovery mechanism: a client
        // reads the hash and knows the surface changed without fetching it.
        String identityHash = identity == null
                ? "" : Reflection.bindingHash(Identity.PROTOCOL_NAME, identityMethods);

        byte[] payload;
        if ("list_protocols".equals(method)) {
            List<Reflection.Summary> hosted = new ArrayList<>();
            hosted.add(new Reflection.Summary(protocolName(), protocolVersion, appHash));
            hosted.add(new Reflection.Summary(Reflection.PROTOCOL_NAME, "", reflHash));
            // After reflection, so identity appears in reflection's own output
            // rather than having to be known a priori.
            if (identity != null) {
                hosted.add(new Reflection.Summary(Identity.PROTOCOL_NAME, "", identityHash));
            }
            payload = Reflection.buildProtocolList(
                    serverId == null ? "" : serverId,
                    "",
                    Metadata.REQUEST_VERSION,
                    hosted);
        } else if ("describe".equals(method)) {
            String requested = readProtocolArgument(kwargs);
            if (protocolName().equals(requested)) {
                payload = Reflection.buildServiceDescription(
                        protocolName(), protocolVersion, appHash, methods);
            } else if (Reflection.PROTOCOL_NAME.equals(requested)) {
                payload = Reflection.buildServiceDescription(
                        Reflection.PROTOCOL_NAME, "", reflHash, reflectionMethods);
            } else if (identity != null && Identity.PROTOCOL_NAME.equals(requested)) {
                payload = Reflection.buildServiceDescription(
                        Identity.PROTOCOL_NAME, "", identityHash, identityMethods);
            } else {
                // Named, not silently empty: an empty description reads as
                // "this protocol has no methods".
                Wire.writeErrorStream(transport.writer(), RpcStream.EMPTY_SCHEMA,
                        new IllegalArgumentException(
                                "This server does not host protocol '" + requested + "'. Hosted: "
                                        + hostedProtocolNames()),
                        serverId);
                transport.writer().flush();
                return;
            }
        } else {
            Wire.writeErrorStream(transport.writer(), RpcStream.EMPTY_SCHEMA,
                    new IllegalArgumentException(
                            "Protocol '" + Reflection.PROTOCOL_NAME + "' has no method '" + method
                                    + "'. Available: [describe, list_protocols]"),
                    serverId);
            transport.writer().flush();
            return;
        }

        // The framework's ordinary convention for a structured return: the
        // payload rides as serialized bytes in a single `result` binary column.
        writeReflectionResult(transport, payload);
    }

    /** The protocols this server routes, for a diagnostic that must name them. */
    private String hostedProtocolNames() {
        List<String> names = new ArrayList<>();
        names.add(protocolName());
        names.add(Reflection.PROTOCOL_NAME);
        if (identity != null) names.add(Identity.PROTOCOL_NAME);
        return names.toString();
    }

    /**
     * Read the {@code protocol} argument off a {@code describe} request.
     *
     * <p>From the decoded kwargs rather than the reader's root: draining the request stream
     * mutates that root, and the drain happens before dispatch -- which is exactly why the
     * kwargs are snapshotted beforehand.
     */
    private static String readProtocolArgument(Map<String, Object> kwargs) {
        Object v = kwargs == null ? null : kwargs.get("protocol");
        return v == null ? "" : v.toString();
    }

    private void writeReflectionResult(RpcTransport transport, byte[] payload) throws IOException {
        var schema = new org.apache.arrow.vector.types.pojo.Schema(List.of(
                new org.apache.arrow.vector.types.pojo.Field(
                        "result",
                        org.apache.arrow.vector.types.pojo.FieldType.notNullable(
                                new org.apache.arrow.vector.types.pojo.ArrowType.Binary()),
                        null)));
        Map<String, String> md = new LinkedHashMap<>();
        md.put(Metadata.REQUEST_VERSION_KEY, Metadata.REQUEST_VERSION);
        if (serverId != null) md.put(Metadata.SERVER_ID, serverId);
        try (IpcStreamWriter w = new IpcStreamWriter(transport.writer());
             VectorSchemaRoot root = VectorSchemaRoot.create(schema, Allocators.root())) {
            ((org.apache.arrow.vector.VarBinaryVector) root.getVector("result")).setSafe(0, payload);
            root.setRowCount(1);
            w.writeBatch(root, md);
        } finally {
            transport.writer().flush();
        }
    }

    private void serveTransportOptions(RpcTransport transport) throws IOException {
        // Worker capabilities ride as metadata; the response batch is empty.
        Map<String, String> md = new LinkedHashMap<>(TransportOptions.workerCapabilities());
        md.put(Metadata.REQUEST_VERSION_KEY, Metadata.REQUEST_VERSION);
        if (serverId != null) md.put(Metadata.SERVER_ID, serverId);
        try (IpcStreamWriter w = new IpcStreamWriter(transport.writer());
             VectorSchemaRoot root = VectorSchemaRoot.create(RpcStream.EMPTY_SCHEMA, Allocators.root())) {
            root.setRowCount(0);
            w.writeBatch(root, md);
        } finally {
            transport.writer().flush();
        }
    }

    private void writeHeaderStream(OutputStream os, ArrowSerializableRecord header,
                                   ClientLogSink sink) throws IOException {
        Schema schema = SchemaDerivation.schemaForRecord(header.getClass());
        Map<String, Object> row = RecordCodec.toRowMap(header);
        IpcStreamWriter w = new IpcStreamWriter(os);
        try {
            w.writeSchema(schema);
            // Flush buffered init-time log batches into the header IPC stream
            // (matches the Python reference: log batches precede the header batch).
            if (sink != null) sink.bind(w, schema);
            try (Marshalling.EncodedRow encoded =
                         Marshalling.encodeRowForWire(schema, row, Allocators.root())) {
                w.writeBatch(encoded.root(), null, encoded.provider());
            }
        } finally {
            w.close();
            if (sink != null) sink.detach();
        }
    }

    private static Throwable unwrap(Throwable t) {
        if (t instanceof InvocationTargetException && t.getCause() != null) {
            return t.getCause();
        }
        return t;
    }

    /** Buffers log messages until the IPC writer is bound, then writes them inline. */
    static final class ClientLogSink implements Consumer<Message> {
        private final String serverId;
        private final List<Message> buffer = new ArrayList<>();
        private IpcStreamWriter writer;
        private Schema schema;

        ClientLogSink(String serverId) { this.serverId = serverId; }

        void bind(IpcStreamWriter writer, Schema schema) throws IOException {
            this.writer = writer;
            this.schema = schema;
            for (Message msg : buffer) writeNow(msg);
            buffer.clear();
        }

        /** Unbind so further logs are buffered again (used between header + main streams). */
        void detach() {
            this.writer = null;
            this.schema = null;
        }

        @Override
        public void accept(Message msg) {
            if (writer != null) {
                try { writeNow(msg); } catch (IOException e) { throw new RuntimeException(e); }
            } else buffer.add(msg);
        }

        private void writeNow(Message msg) throws IOException {
            Map<String, String> md = msg.addToMetadata(null);
            if (serverId != null) md.put(Metadata.SERVER_ID, serverId);
            Wire.writeZeroBatch(writer, schema, md);
        }
    }
}
