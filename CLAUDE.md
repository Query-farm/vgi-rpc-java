# CLAUDE.md

Guidance for Claude Code when working in this repository.

## Project overview

**vgi-rpc-java** is a Java 21 port of **vgi-rpc** (the Python reference lives at `~/Development/vgi-rpc`). vgi-rpc is a transport-agnostic RPC framework built on Apache Arrow IPC: services are defined as Java interfaces, Arrow schemas are derived from method signatures / record component types, and calls flow over pipe, unix-socket, raw TCP, or HTTP transports as sequential Arrow IPC streams.

When the Python and Java implementations disagree, **Python is the reference**. Wire format, metadata keys, error semantics, and stream-state token layout must match byte-for-byte so the two can interoperate (the conformance suite runs a Python driver against the Java worker).

## Commands

```bash
# Build everything (uses Gradle wrapper, toolchain pins to JDK 21)
./gradlew build

# Compile only
./gradlew compileJava

# JUnit tests (Arrow memory needs --add-opens java.base/java.nio — already set in root build.gradle.kts)
./gradlew test

# Single module
./gradlew :vgirpc:test

# Assemble runnable distributions for workers
./gradlew installDist

# Python-driven conformance suite (builds first, then runs pytest against all transports)
./run_tests.sh
./run_tests.sh pipe             # single transport
./run_tests.sh "echo_point"     # pytest -k filter
./run_tests.sh --no-build …     # skip gradle rebuild

# Inspect a single failing conformance test
./inspect.sh <test_id>

# Conformance suite under JaCoCo (one .exec per spawned worker, merged report)
./run_tests.sh --coverage              # → vgirpc/build/reports/jacoco/jacocoConformanceReport/

# Combined coverage: JUnit lane + conformance lane (the honest "adequacy" number)
./gradlew :vgirpc:test :vgirpc:java22Test      # JUnit + FFM exec data
./run_tests.sh --coverage                      # conformance exec data
./gradlew :vgirpc:jacocoMergedReport           # → .../jacocoMergedReport/
```

`run_tests.sh` requires `JAVA_HOME=/opt/homebrew/opt/openjdk@21` (set inside the script) and the Python venv at `~/Development/vgi-rpc/.venv`. Full pytest output is written to `/tmp/pytest_java.txt`.

Before pushing: `./gradlew build` must pass, and `./run_tests.sh` must pass for the transports that apply to the change.

## Module layout (`settings.gradle.kts`)

- **`vgirpc`** — core library. Wire protocol, transports, HTTP server/client (Jetty 12), schema derivation, marshalling, external-location support, shared-memory segment primitive.
- **`vgirpc-oauth`** — optional OAuth/JWT bits (JWKS validation, PKCE, signed cookies). Split out so core users don't pull `nimbus-jose-jwt` (~500 KB).
- **`vgirpc-s3`** — S3 `ExternalStorage` backend.
- **`vgirpc-gcs`** — Google Cloud Storage `ExternalStorage` backend.
- **`conformance`** — the conformance service definition (`ConformanceService`, `AllTypes`, `Point`, `BoundingBox`, `RichHeader`, etc.) shared between the Java worker and the Python driver.
- **`conformance-worker`** — runnable entry point (`Main`) that serves `ConformanceService` over pipe / unix / tcp / HTTP based on CLI args (`--unix <path>`, `--tcp [HOST:]PORT`, `--http`). Packaged via `installDist`.
- **`benchmark`** + **`benchmark-worker`** — equivalent pair for the benchmark service.

## Core modules inside `vgirpc`

Package root: `farm.query.vgirpc`

- **`RpcServer`** — dispatches unary + streaming calls, owns server identity, handles `__describe__`. Call sites use `Wire.writeZeroBatch(writer, schema, meta)` for log/error/tick batches — don't inline the `VectorSchemaRoot.create + allocateNew + setRowCount(0) + writeBatch` sequence again.
- **`RpcConnection`** — client-side `java.lang.reflect.Proxy` factory. Turns a typed interface into an RPC proxy over an `RpcTransport`.
- **`ClientStreamSession`** — client side of a streaming exchange; buffers params, writes ticks / input batches, reads output batches.
- **`CallContext`** + **`AuthContext`** + **`AuthScope`** — request-scoped context injected into method implementations via an optional `CallContext ctx` parameter (the parameter is NOT declared on the service interface, it's detected reflectively at dispatch time). `AuthScope` is the thread-local bridge for HTTP auth.
- **`RpcMethodInfo`** / **`MethodType`** / **`ServiceIntrospector`** — reflective introspection of a service interface. Pulls method type (UNARY/STREAM), params schema, result schema, auth requirements.
- **`Stream<S>`** / **`StreamState`** / **`ProducerState`** / **`ExchangeState`** — streaming primitives. A streaming method returns `Stream<S extends StreamState>`; the state's `process(input, out, ctx)` is called once per tick.
- **`OutputCollector`** — per-tick output buffer. Collects zero or one data batch plus any log/error zero-row batches.
- **`Introspect`** / **`AnnotatedBatch`** / **`RpcError`** / **`VersionError`** — protocol types.

### Subpackages

- **`wire/`** — `IpcStreamReader`, `IpcStreamWriter`, `Metadata` (all `vgi_rpc.*` metadata key constants), `Allocators` (shared `BufferAllocator` root), `Wire` (higher-level helpers: `requestMetadata`, `validateRequestVersion`, `requireMethodName`, `writeErrorStream`, `writeZeroBatch`, `errorMetadata`, `classify`, `errorFromMetadata`, `messageFromMetadata`), `MapToList` (Arrow map↔list-of-struct coercion).
- **`transport/`** — `RpcTransport` interface, `StdioTransport`, `SubprocessTransport`, `UnixSocketTransport`, `TcpSocketTransport` (raw Arrow-IPC framing over a bare TCP socket — the network analog of `UnixSocketTransport`; no auth/TLS, loopback-default, trusted networks only).
- **`http/`** — Jetty-based HTTP transport. `HttpServer`, `HttpPreHandler`, `HttpStreamHandler` (stateless streaming: state travels in a signed `StateToken` in custom metadata), `StateSerializer`, `StateToken`, `Authenticator`, `AuthException`, `TokenExpiredException`.
- **`http/auth/`** — shared authenticator implementations (bearer, mTLS/XFCC). JWT/OAuth lives in the `vgirpc-oauth` module to keep core deps lean.
- **`marshal/`** — `Marshalling` (row↔VectorSchemaRoot, type casting, parameter adaptation), `RecordCodec` (Java record ↔ row map).
- **`schema/`** — `SchemaDerivation` (Java type → Arrow schema), `ArrowSerializableRecord`, `ArrowField`, `ArrowFieldType`, `Nullable`, `EnumDictionaryRegistry`, `StreamHeader`.
- **`external/`** — `ExternalStorage`, `ExternalLocationConfig`, `Externalizer` (large batch → pointer batch), `LocationResolver`, `ExternalFetcher`.
- **`shm/`** — `ShmSegment` for zero-copy batch transfer between co-located processes.
- **`log/`** — `Level`, `Message`. Log messages are serialized as zero-row batches with `vgi_rpc.log_level` / `vgi_rpc.log_message` / `vgi_rpc.log_extra` metadata.

## Wire protocol

- Multiple IPC streams sequential on the same pipe; one request stream and one response stream per call.
- Every request batch carries `vgi_rpc.request_version` in custom metadata (`Wire.requestMetadata`) — server validates via `Wire.validateRequestVersion` and rejects mismatches with `VersionError`.
- Unary: client sends params batch → server replies with zero or more log batches + one result/error batch.
- Stream: initial params exchange, then lockstep ticks (producer) or input batches (exchange) → server replies with log batches + one output batch per tick, until EOS.
- HTTP mapping: `POST /vgi/{method}` (unary), `POST /vgi/{method}/init` (stream init), `POST /vgi/{method}/exchange` (stream exchange). Streaming state is stateless server-side: `StateToken` (HMAC-signed) rides in Arrow custom metadata between calls.
- Errors become zero-row batches with `Level.EXCEPTION` log metadata; the transport stays clean for the next call. `Wire.errorFromMetadata` / `Wire.messageFromMetadata` reconstruct on the client side.

## Conventions

- **Java 21**, `--release 21`, `-Xlint:all,-serial,-processing`, `-parameters` (parameter names matter — the framework uses them to bind kwargs).
- Prefer **records** for data classes (`AllTypes`, `Point`, `RichHeader` are records).
- Prefer **sealed types** and pattern matching where they simplify dispatch.
- **Try-with-resources** for every `VectorSchemaRoot`, `IpcStreamWriter/Reader`, and socket.
- All `VectorSchemaRoot`s allocate from `Allocators.root()` unless a sub-allocator is explicitly needed; closing them returns memory.
- Metadata keys live in `wire/Metadata.java` — never hard-code the string `"vgi_rpc.*"` elsewhere.
- Zero-row control batches (log, error, tick, pointer) go through `Wire.writeZeroBatch` — don't re-inline the allocate/setRowCount/writeBatch sequence.
- Keep the wire path byte-compatible with Python. Before changing metadata keys, stream-state layout, or batch framing, check the Python implementation at `~/Development/vgi-rpc/vgi_rpc/`.

## Testing

- **JUnit 5** for Java-side unit tests (`*Test.java` under `src/test/java`). Arrow memory needs `--add-opens=java.base/java.nio=ALL-UNNAMED` — already wired in the root `build.gradle.kts`.
- **Conformance** is driven from Python via `tests/test_java_conformance.py` and the other `tests/test_java_*.py` files. These spawn the Java worker (built via `./gradlew installDist`) over the transport under test. The `./run_tests.sh` / `./inspect.sh` entry points stay at the repo root.
- The conformance driver expects `conformance-worker` to print `PORT:<port>` on stdout when launched with `--http` (auto-port selection, matches the Python reference).

## Cross-language wire alignment

This port tracks `vgi-rpc-python` for wire compatibility. Two surfaces matter:

- **`__describe__`** — `Introspect.DESCRIBE_VERSION = "4"`. `DESCRIBE_SCHEMA` is the slim 8-column form: `name`, `method_type`, `has_return`, `params_schema_ipc`, `result_schema_ipc`, `has_header`, `header_schema_ipc`, `is_exchange`. Python-flavoured columns (`doc`, `param_types_json`, `param_defaults_json`, `param_docs_json`) are off the wire — the Protocol interface is the source of truth for human-readable type info. The response's custom metadata carries `vgi_rpc.protocol_hash` via `Introspect.computeProtocolHash`, byte-identical to the Python algorithm. `RpcServer.protocolHash()` exposes it; `RpcServer.setProtocolVersion(...)` sets the optional human label. Within-port stable; cross-port byte equality is *not* guaranteed (Arrow IPC schema bytes differ across libraries).
- **Access log** — `AccessLogHook` (`AccessLogHook.java`) implements `DispatchHook` and writes one JSONL record per dispatch. The record conforms to `vgi_rpc/access_log.schema.json` in the Python repo and validates under `vgi-rpc-test --access-log <path>`. `DispatchInfo` carries `protocol`, `protocolHash`, `protocolVersion`, `remoteAddr`, `requestData`, `streamId`, `cancelled`, `httpStatus`. Install via `RpcServer.setDispatchHook(new AccessLogHook(out, serverVersion))`.

The conformance worker accepts `--access-log <path>` (`Main.java` parses it).

## When in doubt

1. Check the Python reference at `~/Development/vgi-rpc/vgi_rpc/` — behavior there is authoritative.
2. Check `~/Development/vgi-rpc/CLAUDE.md` for the higher-level architectural summary.
3. Run `./run_tests.sh <keyword>` to see whether the conformance suite already exercises the behavior you're changing.
