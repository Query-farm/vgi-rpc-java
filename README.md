<p align="center">
  <img src="https://raw.githubusercontent.com/Query-farm/vgi-rpc-java/main/assets/vgi-logo.png" alt="Vector Gateway Interface logo" width="320">
</p>

<h1 align="center">vgi-rpc-java</h1>

<p align="center">
  Transport-agnostic RPC framework built on <a href="https://arrow.apache.org/">Apache Arrow</a> IPC serialization — the Java port of <a href="https://github.com/Query-farm/vgi-rpc-python">vgi-rpc</a>.<br>
  Built by <a href="https://query.farm">🚜 Query.Farm</a>
</p>

<p align="center">
  <a href="https://github.com/Query-farm/vgi-rpc-java/actions/workflows/ci.yml"><img src="https://github.com/Query-farm/vgi-rpc-java/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://central.sonatype.com/artifact/farm.query/vgirpc"><img src="https://img.shields.io/maven-central/v/farm.query/vgirpc" alt="Maven Central"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache--2.0-blue" alt="License"></a>
</p>

Define RPC interfaces as ordinary Java interfaces. The framework derives Apache Arrow schemas from your method signatures and record component types, and hands you a typed client proxy with automatic serialization/deserialization. There are no `.proto` files or codegen steps — your Java types *are* the schema. Unlike JSON-over-HTTP, structured data stays in Arrow's columnar format for efficient transfer, which pays off for large or batch-oriented workloads.

This is a port of the Python reference implementation, [`vgi-rpc`](https://github.com/Query-farm/vgi-rpc-python), and is **wire-compatible** with it: the same calls interoperate across the Python, Java, Go, and C++ peers (the conformance suite runs the Python driver against this Java worker over every transport).

## Key features

- **Interface-based services** — define a service as a typed Java interface; the client proxy preserves that interface for full IDE autocompletion.
- **Apache Arrow IPC wire format** — columnar serialization for structured data.
- **Two method types** — unary calls and streaming (producer and exchange patterns).
- **Transport-agnostic** — stdio pipe, subprocess, Unix domain socket, raw TCP socket (trusted networks — no auth/TLS), shared memory, or HTTP.
- **Automatic schema inference** — Java types and `record` components map to Arrow types; `@ArrowField` refines them.
- **Pluggable authentication** — `AuthContext` + authenticators for HTTP (bearer, mTLS/XFCC; JWT/OAuth in the optional `vgirpc-oauth` module).
- **Runtime introspection** — the `vgi_rpc.Reflection.v1` protocol (`list_protocols`, then `describe`) for dynamic service discovery — `Introspect.listProtocols` / `describeProtocol` over any held connection (see [Discovering what a server hosts](#discovering-what-a-server-hosts)) — with a canonical protocol hash every port agrees on. The old `__describe__` RPC is retired; a request for it is refused with a message naming its replacement.
- **Shared-memory transport** — zero-copy batch transfer between co-located processes (auto-negotiated on JDK 22+ via a multi-release overlay; transparent pipe fallback otherwise).
- **Large-batch externalization** — oversized batches transparently spilled to S3 (`vgirpc-s3`) or GCS (`vgirpc-gcs`).
- **Pre-published results** — a unary method can answer with an `ExternalRef` to an object published once, so a large, rarely-changing result is never re-serialized or re-uploaded per call.

## Requirements

- **Java 21+** at runtime. The shared-memory side-channel additionally requires **JDK 22+** (where `java.lang.foreign` is GA); on 21 it transparently falls back to inline transfer.

## Installation

Artifacts are published to Maven Central under the `farm.query` group.

**Gradle (Kotlin DSL):**

```kotlin
dependencies {
    implementation("farm.query:vgirpc:0.34.0")          // core: protocol, transports, HTTP, schema
    implementation("farm.query:vgirpc-iroh:0.34.0")     // optional: official native Iroh binding
    implementation("farm.query:vgirpc-oauth:0.34.0")    // optional: JWT / OAuth / PKCE auth
    implementation("farm.query:vgirpc-s3:0.34.0")       // optional: S3 external storage
    implementation("farm.query:vgirpc-gcs:0.34.0")      // optional: GCS external storage
}
```

**Maven:**

```xml
<dependency>
  <groupId>farm.query</groupId>
  <artifactId>vgirpc</artifactId>
  <version>0.34.0</version>
</dependency>
```

The core depends on Apache Arrow and SLF4J (API only — bring your own logging backend).

## Quick start

**1. Define a service as a Java interface** (shared by client and server):

```java
public interface Calculator {
    double add(double a, double b);
    String greet(String name);
}
```

**2. Implement it and serve it.** A worker typically serves over stdio so a parent process can drive it as a subprocess:

```java
import farm.query.vgirpc.RpcServer;
import farm.query.vgirpc.transport.StdioTransport;

public final class CalculatorWorker {
    public static void main(String[] args) {
        Calculator impl = new Calculator() {
            public double add(double a, double b) { return a + b; }
            public String greet(String name)      { return "Hello, " + name + "!"; }
        };
        RpcServer server = new RpcServer(Calculator.class, impl);
        try (StdioTransport transport = new StdioTransport()) {
            server.serve(transport);
        }
    }
}
```

**3. Call it through a typed proxy.** The client launches the worker and gets back something that *is* a `Calculator`:

```java
import farm.query.vgirpc.RpcConnection;
import farm.query.vgirpc.transport.SubprocessTransport;
import java.util.List;

var transport = new SubprocessTransport(List.of(
        "java", "--add-opens=java.base/java.nio=ALL-UNNAMED", "-Dio.netty.noUnsafe=false",
        "-cp", "worker.jar", "CalculatorWorker"));
try (RpcConnection conn = new RpcConnection(transport)) {
    Calculator calc = conn.proxy(Calculator.class);
    double sum    = calc.add(2.0, 3.0);   // 5.0
    String hello  = calc.greet("World");  // "Hello, World!"
}
```

> **Three things to get right:**
> - **Run with `--add-opens=java.base/java.nio=ALL-UNNAMED`** on every JVM that touches the library (both the worker and the client above) — Apache Arrow accesses `java.nio` internals and throws on startup without it. Notice it's passed both to the client JVM and, in the `SubprocessTransport` command, to the spawned worker.
> - **On Java 25+, also pass `-Dio.netty.noUnsafe=false`.** Arrow allocates through Netty 4.2, which switches `sun.misc.Unsafe` off by default on Java 25+, and Arrow's allocator cannot start without it ([apache/arrow-java#728](https://github.com/apache/arrow-java/issues/728)). The library sets this default itself when it is the first thing in the process to touch Arrow, but a JVM that allocates Arrow memory some other way first needs the flag. It is harmless on Java 21–24.
> - **Compile services with `-parameters`** — the framework binds call arguments by parameter name (matching the Python reference's keyword-argument wire semantics).

## Modules

| Module | Purpose |
|---|---|
| **`vgirpc`** | Core library — wire protocol, transports, HTTP server/client (Jetty 12), schema derivation, marshalling, external-location support, shared-memory primitive. |
| **`vgirpc-iroh`** | Optional official Kotlin/JVM Iroh provider for raw Arrow mux and HTTP semantics. |
| **`vgirpc-oauth`** | Optional OAuth/JWT support (JWKS validation, PKCE, signed cookies). Split out so core users don't pull `nimbus-jose-jwt`. |
| **`vgirpc-s3`** | Amazon S3 `ExternalStorage` backend for large-batch externalization. |
| **`vgirpc-gcs`** | Google Cloud Storage `ExternalStorage` backend. |

## Transports

| Transport | Use case |
|---|---|
| **stdio** (`StdioTransport`) | Worker process driven over stdin/stdout by a parent. |
| **subprocess** (`SubprocessTransport`) | Client spawns and talks to a worker subprocess. |
| **Unix socket** (`UnixSocketTransport`) | Co-located processes over a domain socket. |
| **shared memory** | Zero-copy batch transfer for co-located processes; auto-negotiated on JDK 22+, transparent pipe fallback otherwise. |
| **HTTP** (`HttpServer` / Jetty 12) | Networked, stateless-server streaming; auth via authenticators. |
| **Iroh Arrow mux** (`IrohTransports`) | Stateful, authenticated QUIC to an `iroh://` worker. |
| **HTTP over Iroh** (`HttpRpcConnection.irohBuilder`) | Existing HTTP state/continuation semantics carried over authenticated `iroh-http/2`. |

Both Iroh modes use the official `computer.iroh` JVM binding from the optional
`vgirpc-iroh` module. HTTP over Iroh retains the ordinary HTTP client’s OPTIONS
discovery, response budgets, headers, authentication, and continuation logic:

```java
try (var connection = HttpRpcConnection.irohBuilder(
        "httpi://<64-lowercase-hex-endpoint-id>/vgi",
        IrohTransportOptions.defaults())
        .bearerToken(accessToken)
        .buildIroh()) {
    Calculator calculator = connection.proxy(Calculator.class);
    double result = calculator.add(2.0, 3.0);
}
```

The provider owns one authenticated Iroh connection, opens one bidirectional
stream per HTTP request, and closes it with the `HttpRpcConnection`. It does not
start or download a helper executable.

## Method types

- **Unary** — request batch in, one result (or error) batch out.
- **Streaming** — a `RpcStream<S extends StreamState>` whose state's `process(input, out, ctx)` runs once per tick, in two flavours: **producer** (server emits a sequence of output batches) and **exchange** (lockstep input batch → output batch).

## Pre-published results (`ExternalRef`)

Some unary results are large and change rarely (a worker's whole catalog, say). Rather than letting
the externalizer serialize and upload them on every call, publish the result once with
`Externalizer.publishExternal` and answer later calls with the cached reference:

```java
public interface CatalogService {
    String catalog(CallContext ctx);   // CallContext is off-wire
}

final class CatalogImpl implements CatalogService {
    private volatile ExternalRef ref;

    @Override public String catalog(CallContext ctx) {
        ExternalRef r = ref;
        if (r == null) {
            Schema schema = ServiceIntrospector.describe(CatalogService.class).get("catalog").resultSchema();
            // Builds {result: [value]}, serializes it exactly like the per-call externalizer,
            // hashes the raw bytes, compresses (zstd here) and uploads once.
            r = ref = Externalizer.publishExternal(schema, buildCatalog(), storage,
                    ExternalLocationConfig.Compression.zstd(), /* includeSha256 */ true);
        }
        ctx.respondWithExternalRef(r);
        return null;   // ignored once a ref is set
    }
}
```

`ctx.respondWithExternalRef(ref)` makes the dispatcher write the ExternalLocation pointer batch
(`vgi_rpc.location`, plus `vgi_rpc.location.sha256` only when the ref has a digest) directly: the
returned value is ignored and not validated, nothing is serialized or uploaded, the inline and
shared-memory routes are never taken, and this holds whether or not the server has external
storage configured and regardless of the externalization threshold. A ref is not charged to
`max_externalized_response_bytes`. It works on every transport (pipe, unix, TCP, HTTP); clients
resolve it like any other pointer and need no change. Unary methods with a result only — a stream
method's context refuses the call.

`publishExternal` also takes a ready-built 1-row `VectorSchemaRoot`. A ref built with
`includeSha256 = false` (or `ExternalRef.of(url)` for an object published out of band) carries no
digest, so clients skip the content check — use that for an object rewritten in place. You own the
ref's cache and the object's lifecycle: a long-lived ref must not point at an object under the
short-TTL lifecycle rule used for per-call uploads, a pre-signed URL expires (re-sign or rebuild the
ref before then), and a ref must only be returned to callers who are all entitled to the same
content.

## Stream state by reference (`StateRef`, protocol 3.4)

On HTTP every stream continuation echoes the stream's state back to the server. Data that does not
change for the life of a stream belongs in a `StateRef`: create it in the stream method, keep it in a
state field, resolve it on any turn.

```java
public RpcStream<ScanState> scan(byte[] query, CallContext ctx) {
    StateRef ref = ctx.stateRef(query);          // byte[] or an ArrowSerializableRecord
    return RpcStream.producer(SCHEMA, new ScanState(ref));
}

static final class ScanState extends ProducerState {
    StateRef query;                              // serialized as its encoded bytes
    @Override public void produce(OutputCollector out, CallContext ctx) {
        byte[] q = ctx.resolve(query);           // any turn, any replica
    }
}
```

On pipe, stdio, Unix and TCP the value stays in memory. On HTTP the first cursor mint decides its form:
inline (compressed inside the sealed cursor) when it is small or no `ObjectStore` is configured, else
stored once and carried as a ~130-byte sealed tag. `ctx.stateRef` is allowed only on the stream method's
own context: continuations, `onCancel`, unary calls and the first producer tick refuse it
(`state_ref_misuse`). The rest of the contract is vgi-rpc-python's `tools/cross-port/specs/STATE_REF_V1.md`:

- **Turn budget.** `floor((max_request_bytes - 4096) / 1.34) - call_token_bytes`. A cursor over budget logs
  `cursor_large` and sends the client a WARN log under `HttpServer.Config.Builder.cursorRule(WARN)` (the
  default); `ERROR` refuses the turn with `cursor_too_large`. A request cap below 16 KiB disables the rule
  with a startup warning. The inline limit is `stateRefInlineBytes` (`VGI_RPC_STATE_REF_INLINE_BYTES`,
  default 16384), capped at a quarter of the budget.
- **Storage.** `ObjectStore` (`putObject`, `get(objectId, maxBytes)`) sits next to `ExternalStorage`;
  `VendorStorage`, `S3Storage`, `GcsStorage` and the conformance fake storage implement both. Objects carry
  no TTL and the server never deletes them: **set a bucket lifecycle rule of at least one day** on the
  `rpc` prefix.
- **Replicas.** Every replica must share one `tokenKey`. Stored tags are sealed under it, and the
  default `tokenTtlSeconds` is now 21600 (6 hours, the maximum stream duration).
- **Object cache.** One per process (`ObjectCache.defaultCache()`), keyed by `object_id`, single-flight,
  bounded by `VGI_RPC_OBJECT_CACHE_BYTES` (default `min(1 GiB, 40%` of memory`)`), entries stale after a day.
- **Argument references.** Every `__upload_url__/init` row also carries `object_id` and `ref`, a sealed
  reference bound to the caller's principal. `ctx.resolveArgumentRef(ref, size, sha256)` (or
  `ArgumentRefs.resolve`) opens, fetches through the object cache and verifies it. The advertised
  `VGI-Max-Upload-Bytes` is always sent with upload URLs: by default it is the server's fetch cap
  (`VGI_RPC_MAX_FETCH_BYTES`, default 512 MiB); a larger configured upload cap raises a default fetch cap
  to match, and is refused at startup against an explicit lower one.
- **Direct object storage (no blob vendor).** `S3Storage.fromEnv(env)` / `GcsStorage.fromEnv(env)` read
  the cross-SDK direct-storage contract: `VGI_STORAGE_S3_BUCKET`, `_PREFIX`, `_ENDPOINT`, `_PUBLIC_ENDPOINT`
  (the host of every URL handed to clients), `_REGION`, `_FORCE_PATH_STYLE`, `_CHECKSUM` (`sha256` or
  `none`: no upload grants), `_SDK_CHECKSUM` (`when_required`, the default with an endpoint, or
  `when_supported`: the AWS SDK's own CRC32), `_ACCESS_KEY_ID` / `_SECRET_ACCESS_KEY` / `_SESSION_TOKEN`
  (else the AWS default chain, which refreshes temporary credentials), `VGI_STORAGE_URL_TTL_SECONDS`
  (default 3600, capped at credential expiry minus 60 s); `VGI_STORAGE_GCS_BUCKET`, `_PREFIX`, `_ENDPOINT`,
  `_CREDENTIALS_FILE`. `S3Storage.verify()` / `GcsStorage.verify()` check the bucket and name the
  problem. Keys go under `<prefix>rpc/` (results, grants, StateRef and argument objects) and
  `<prefix>fs/` (function storage, `fromEnv(env, "fs")`); add lifecycle rules of 1 day on `rpc/` and
  2 days on `fs/`. An `http://` endpoint needs `VGI_STORAGE_ALLOW_PLAIN_HTTP=1`. An explicit session
  token's expiry goes in `VGI_STORAGE_S3_SESSION_EXPIRES` (RFC 3339 or epoch seconds) so URL lifetimes
  are capped. The worker must also reach `_PUBLIC_ENDPOINT`: request-upload pointers are fetched by URL.
- **Storage timeouts.** Storage transfers (vendor-signed PUTs and GETs, inbound pointer fetches, client
  upload PUTs) fail only when no bytes move for `VGI_RPC_STORAGE_STALL_TIMEOUT_SECONDS` (default 30 s;
  connect 30 s). There is no whole-transfer deadline, so a 100 MB result on a 2 MB/s link (50 s)
  completes in one attempt. Transport errors (connect, reset, stall), 429 and 5xx retry up to three
  attempts (0.25 s backoff, doubling, 5 s cap, jitter). Vendor API calls keep their 30 s request
  timeout. `S3Storage` uses a 30 s connect and a per-read stall timeout, plus a per-PUT backstop of
  connect + stall + size at 64 KiB/s for a server that stops reading; `GcsStorage` (default client)
  uses 30 s connect and the stall timeout per read. See `farm.query.vgirpc.external.StorageHttp`.

## Protocol names

A service's **wire name** is its routing key: it rides every request as `vgi_rpc.protocol`, and
over HTTP it is also the protocol path segment. By default it is the interface's simple name.
Declare it explicitly when the name is a cross-implementation contract:

```java
@ProtocolName("orders.v2")
@ProtocolVersion("2.0.0")
public interface OrderService { ... }
```

Put the major version in the name. An incompatible major then becomes a *different* protocol and
an unroutable request 404s — an answer every proxy and load balancer understands without an Arrow
parser — and `orders.v1` and `orders.v2` can be served side by side while clients migrate. A Java
simple name cannot express that shape at all, since no Java identifier contains a dot.

The declaration is read from the interface's **own** annotations. An interface that extends a
declared protocol and does not redeclare gets its own simple name rather than silently answering
to its parent's routing key.

A request naming a protocol this server does not host is refused with `ProtocolNotSupportedError`
(`protocol_not_supported`), distinct from `protocol_not_specified` for a request that named none
and from `method_not_implemented` for a hosted protocol missing the method. A client probing for
an optional protocol depends on telling those apart.

The same answer comes back from `vgi_rpc.Reflection.v1`'s `describe`, which asks the same question
with the name as an argument rather than as a routing key. Both check the name against the grammar
*before* the lookup, so a name that cannot be a protocol name is refused without being echoed back
in the message.

## Discovering what a server hosts

`Introspect.listProtocols(target)` and `Introspect.describeProtocol(target, name)` ask
`vgi_rpc.Reflection.v1` over a connection you already hold. `target` is an `RpcConnection` (pipe,
subprocess, Unix socket, TCP, Iroh), an `HttpRpcConnection` (HTTP, HTTP over Iroh), a typed proxy
from either one's `proxy(Class)` — bound to any protocol — or a raw `RpcTransport`. The connection is
reused and never closed; nothing new is opened.

```java
try (RpcConnection conn = new RpcConnection(transport)) {
    Calculator calc = conn.proxy(Calculator.class);
    for (Introspect.HostedProtocol p : Introspect.listProtocols(calc)) {
        // Server order: application protocols (primary first), then vgi_rpc.Reflection.v1.
        System.out.println(p.name() + " " + p.version() + " " + p.hash());
    }
    Introspect.ServiceDescription d = Introspect.describeProtocol(conn, "Calculator");
    calc.add(1, 2);   // the connection is still yours
}
```

`HostedProtocol` is an immutable record: `name`, `version`, `hash`, `deprecated` (default `false`),
`deprecationMessage` (default `""`) and `features` (default empty). `describeProtocol` lists first,
then describes, so the two failures stay apart: a server that does not host reflection throws
`ReflectionNotSupportedError` (an `RpcError` carrying the server's error fields), while an unknown
name is an ordinary `RpcError` with `errorKind()` `"protocol_not_supported"`. No listing is ever
inferred, and the connection stays usable after either.

A Java `RpcServer` always hosts reflection. The Python reference hosts it only when built with
`enable_describe=True` (the default is `false`; `vgi-rpc-conformance --describe`). Against a server
without it, or one that predates reflection (`method_not_implemented`, `UNIMPLEMENTED`, or a bare
HTTP 404), these throw `ReflectionNotSupportedError`. On a byte-stream transport, don't call them while
a stream is open on the same connection: the calls would interleave on one channel.

## Protocol versions (`PROTOCOL_VERSIONS_V1`)

A server advertises, for every protocol it gates (one declaring a version), the window of
`major.minor` versions it accepts: `VGI-Protocol-Versions: vgi.v3=3.4-3.4` on every HTTP response
(401/404/413/5xx, `HEAD`/`OPTIONS /health` and preflights included, CORS-exposed), and the same
string as `vgi_rpc.protocol_versions` metadata on reflection results, `__transport_options__`
replies and refusals on every transport. The window is exactly the declared version's
`major.minor`; `RpcServer.setProtocolVersionRanges(Map.of("vgi.v3", "3.3-3.4"))` overrides it (how a
fixture impersonates another version). A request outside it is refused with
`protocol_version_unsupported` / `FAILED_PRECONDITION` (HTTP 400), naming both windows and which side
to upgrade, with a `PreconditionFailure` and an `ErrorInfo` carrying the advertisement.

`HttpRpcConnection` negotiates per endpoint: it sends its newest version until a response teaches it
the server's window, then the highest common one (plus its own patch); a missing header falls back to
a pre-spec refusal's `Server: X.Y` or its own version; with no common version it fails before
sending. `Builder.protocolVersionRange("3.4-3.5")` widens what it speaks, `clientProtocolVersion(...)`
declares it for raw calls, and `negotiation(...)` shares the state between connections. Only
reflection calls are retried automatically after a refusal. The client's own window rides as
`VGI-Client-Protocol-Versions` / `vgi_rpc.protocol_versions_client` (informational).

## Wire compatibility

When the Python and Java implementations disagree, **Python is the reference.** Wire format, metadata keys, error semantics, and stream-state token layout match byte-for-byte so the two interoperate. See the Python project's [README](https://github.com/Query-farm/vgi-rpc-python) for the higher-level protocol design.

## Proxy proof

Proxy proof lets a worker refuse any request that did not arrive through a trusted proxy. The
proxy mints a per-request HMAC-SHA256 over a timestamp, a fresh nonce and the worker's own
identifier, keyed by a secret shared only with that worker. Unlike a forwarded assertion about
what happened at a TLS terminator, a proof cannot be produced by someone who merely reaches the
worker directly — without the secret there is nothing to replay.

```java
var secrets = ProxyProof.parseSecrets("prod-use1:" + hexSecret);
var config = ProxyProof.Config.of(ProxyProof.Mode.REQUIRE, "worker-a", secrets);

HttpServer.Config.builder()
    .authenticator(ProxyProof.require(config, existingAuthenticator)) // inner may be null
    .proxyProofRequired(true)                                         // REQUIRE mode only
    .build();
```

It composes as an **AND**, not an alternative: do not pass the gate to `Authenticator.chain`,
whose first-authenticated-wins semantics would let any later credential bypass it.

`proxyProofRequired(true)` advertises `VGI-Proxy-Proof-Required: true` on every response,
`GET /health` and `OPTIONS` included, so an operator or proxy can confirm the worker really does
reject unproofed requests — otherwise a misconfiguration turns the whole feature into a silent
no-op. Set it in `REQUIRE` mode only: `off` and `allow` never deny, so they must not claim to. It
is a separate knob because the gate arrives as an opaque `Authenticator` the server cannot
introspect, and it advertises only — enforcement is entirely the gate's.

The key id doubles as the calling proxy's label, so `AuthContext.claims().get("vgi_proxy_proof")`
records which proxy served each request — derived from the secret that verified, never from the
transmitted field. `OPTIONS`, `/.well-known/` and `{prefix}/health` stay reachable without a proof
in every mode.

Needs no dependency beyond the JDK. The normative cross-language contract is
`docs/proxy-proof-spec.md` in the vgi-rpc repository.

## License

[Apache License 2.0](LICENSE) — Copyright 2026 Query Farm LLC · https://query.farm
