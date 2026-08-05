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

  **Unauthorized responses.** Every 401 follows `docs/unauthorized-spec.md` in the Python repo. `AuthReason` is the closed set of codes; the reason is read off the `AuthException` subtype (`MissingCredentials` → `missing_credential`, `InvalidCredentials` → `invalid_credential`, `AuthFailure` → whatever it declares, defaulting to `unauthorized`) — never guessed from message text. `HttpServer.writeUnauthorized` renders `VGI-Auth-Reason`, `Cache-Control: no-store`, and the JSON envelope `{error, reason, detail, proxy_hint?}`; this port always answers JSON, which §4.2 permits. The **proxy note** (`VGI-Auth-Proxy-Required: true` + `proxy_hint`) comes from server configuration only — `Config.proxyProofRequired` contributes `VGI-Proxy-Proof` in require mode, `Config.proxyAuthHeaders` states headers for a custom authenticator — so it is identical on every 401 and discloses nothing. Cross-language conformance group: `TestUnauthorized`.
  **CORS.** `CorsPolicy` (package-private, applied from `RouterServlet.service` so the grant rides *every* answer, not just the preflight). Strictly opt-in: `Config.corsOrigins` empty ⇒ not one `Access-Control-*` header, which is itself a conformance contract (`TestCorsOffMode`). A single `"*"` allows all — safe only because credentials here are header-borne and the server never sets `Access-Control-Allow-Credentials`; anything else is matched case-insensitively against `Origin`, echoed back, and paired with `Vary: Origin`. `Access-Control-Allow-Headers` echoes the preflight's `Access-Control-Request-Headers` (same answer Go/Rust/Python give), falling back to the request-side surface. `Access-Control-Expose-Headers` is built by `HttpServer.corsExposeHeaders()` from the *same conditions* as `applyCapabilityHeaders` — whatever this server advertises, it exposes. **Adding a `VGI-*` / `X-VGI-*` response header means adding it to both**: an advertised-but-unexposed header is invisible to a browser and to nothing else, so every non-CORS test passes right through the omission. Cross-language conformance group: `TestCors`.
  **Token introspection.** `TokenIntrospection` + `TokenResolver` + `TokenIdentity` back `POST {prefix}/__introspect_token__`, which resolves an opaque bearer credential to a principal for a fronting proxy. Off unless `Config.tokenIntrospection(resolver, principals)` is called, and a disabled worker still answers `404 {"error":"not_enabled"}` — a caller classifies `401/403/404` as definitive and everything else as transient, so an unrouted path (which would dispatch a JSON body into the Arrow reader and 500) means retrying forever against a worker that will never support the feature. The response is a **closed set** of `principal` / `token_name` / `ttl_seconds`; a `claims` field would let a worker choose its caller's tenant routing, row scope and policy branch. The introspector allowlist has **no permissive default** (authentication and introspection are different capabilities), JWS-shaped subjects are refused without reaching the resolver, unknown/expired/malformed are byte-identical rejections, and the credential is SHA-256 digested rather than logged. It is deliberately *not* implemented by replaying the credential through the server's own `Authenticator` — see `TokenResolver` for the four ways that breaks. Advertised via `VGI-Token-Introspection: true`. Conformance groups: `TestTokenIntrospection` (needs the `--introspect` worker) and `TestTokenIntrospectionOffMode` (ungated).
  **Definitive vs transient.** `AuthUnavailableException` means "I could not find out whether the credential is bad" and sits *outside* the `AuthException` hierarchy on purpose: every `AuthException` subtype renders as a 401 and `Authenticator.chain` catches it to mean "not my credential, try the next", so an outage raised as one emerges as a 401 from the end of the chain — turning a sidecar restart into a fleet-wide re-login storm and poisoning callers' negative caches. Unchecked, so it propagates to `RouterServlet.service`, which renders `503` + `Retry-After`.
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
- **Access log** — `AccessLogHook` (`AccessLogHook.java`) implements `DispatchHook` and writes one JSONL record per dispatch. The record conforms to `vgi_rpc/access_log.schema.json` in the Python repo and validates under `vgi-rpc-test --access-log <path>`. `DispatchInfo` carries `protocol`, `protocolHash`, `protocolVersion`, `remoteAddr`, `requestData`, `streamId`, `cancelled`, `httpStatus`, `claims`. Install via `RpcServer.setDispatchHook(new AccessLogHook(out, serverVersion))`, or `AccessLogHook.builder(out)` for the spec's optional behaviours: `sampleRate` (deterministic per call, keyed on `stream_id` then `request_id`, errors never sampled, out-of-range rejected at construction), `asyncQueueSize` (bounded, non-blocking, drops reported as `dropped_records`), `logPayloads(false)` (⇒ `truncated: "payload_omitted"`, which is *not* the size-driven `true`), `claimRedactor` (`ClaimRedactor.byKeyName()` by default, `ClaimRedactor.none()` to opt out; a redactor that throws fails **closed**), and `traceCorrelator` (`TraceCorrelator.openTelemetry()` reads `Span.current()` reflectively so OTel stays off the core classpath — `trace_id`/`span_id` are emitted both or neither, and only when they are well-formed W3C hex).

  **Egress accounting** (§4.8) can't all be measured in the hook: response compression runs after the handler returns. `AccessLogScope` is the per-request thread-local that closes that gap — `RouterServlet.service` opens one, `readBody` stamps `request_bytes` (pre-decompression), `writeArrowResponse` stamps `response_bytes` (post-compression), `Externalizer.maybeExternalize` counts `externalized_bytes` at the single upload choke point, and the scope emits the parked records on close. Transports that install no scope (pipe / unix / TCP) keep logging inline. These are distinct from §4.6's `input_bytes`/`output_bytes` (logical Arrow buffers), which this port does not yet populate at all.

  **HTTP stream turns** (§1: "one record per `init` and one per `exchange`/`produce` continuation") are emitted by `HttpStreamHandler`, not `RpcServer` — HTTP streams never reach `serveOne`, so for a long time they produced no records at all while unary calls logged fine, which is backwards: streams are where the bytes are. `beginTurn`/`StreamTurn` fire the same `DispatchHook` per HTTP request, after the point the turn is a genuine dispatch (a malformed body or an unopenable cursor is refused earlier and logs nothing, matching the reference). The `stream_id` is minted at `/init` — before `mintInitTokens`, so a producer that finishes in one turn still gets one — and travels in the `CallToken`, which is how every continuation's record joins the init's without any server-side state. `DispatchInfo.requestState`/`responseState` carry the **decrypted** cursor (§4.4): the wire token is opaque AEAD, and a log a reader cannot decode without the server's token key is not an audit trail. Java's state blob is CBOR (`StateSerializer`), not the Arrow IPC the spec names — the schema only constrains it to base64, and plaintext-not-ciphertext is the property that matters.

  **`X-VGI-RPC-Error` and the `status` field** both need to know a call failed, and neither can learn it from control flow: every error path serializes the exception into the response body and then returns *normally*. `CallOutcome` (a thread-local opened by `RouterServlet.service` for HTTP and by `serveOne` for pipe/unix/TCP, nesting inertly when both apply) is set at the one choke point every error passes through — `Wire.errorMetadata` — so a new error path cannot forget to raise it. `writeArrowResponse` reads it to set the header (never unconditionally: a flag on every response is the same outage as no flag), and `AccessLogHook` reads it when the dispatcher reported no exception.

  It is read a *second* time, at `AccessLogScope.close`, because dispatch returning is not the moment the outcome is settled. `max_response_bytes` is enforced after the body exists (`HttpServer.writeResponseCapError`, unary and `/exchange`; producer `/init` is soft-capped and must stay `ok`), so an overshoot discards the body, answers an EXCEPTION batch, and lands *after* `onDispatchEnd` computed `status: "ok"`. `AccessLogHook.restate` promotes the parked record — `ok` → `error` only, since a record that already named a failure named the cause and the overshoot it tripped on the way out is a consequence. This works because `RouterServlet.service` opens `CallOutcome` *outside* `AccessLogScope`, so the error is still readable when the scope emits; keep that nesting order. `max_externalized_response_bytes` is **advertised but never enforced** here (the Python reference pre-flights it in `_app_unary`/`_app_stream`), so it has no overshoot path to log. Covered by `TestHttpResponseCapAccessLog`.

  What the schema cannot check — sampling determinism, drop reporting, fail-closed redaction, `payload_omitted` vs `true`, `response_bytes` being the compressed size — is covered by `AccessLogHookTest` and `http/AccessLogEgressTest`.

The conformance worker accepts `--access-log <path>` (`Main.java` parses it) plus `--access-log-sample <rate>`, `--access-log-async`, `--access-log-queue-size <n>` and `--access-log-no-payloads` (so `vgi-rpc-test --access-log` can validate the optional record shapes, not just the default one), `--access-log-debug` (accepted and ignored — see below), `--http-auth` (reject-all authenticator that honours the `X-Conformance-Auth-Reason` fixture header, backing `TestHealth` + `TestUnauthorized`), `--no-call-state-cache` (disables the per-process call-state cache so every stream continuation takes the miss path, backing `TestColdCallStateCache`), `--cors-origin <origin>` (repeatable; implies `--http` and grants that origin browser access, backing `TestCors` — the default worker stays CORS-free for `TestCorsOffMode`), and `--introspect` (implies `--http` plus principal-header auth, and enables token introspection with the fixed conformance introspector/subject/JWS-trap constants, backing `TestTokenIntrospection` — the default worker stays introspection-free for `TestTokenIntrospectionOffMode`).

**Verifying the access log.** `vgi-rpc-test --access-log <path> --require-request-data` is run by the `launcher` conformance lane in CI (`.github/workflows/ci.yml`), unfiltered so the zero-parameter methods — which send an empty schema and no row — stay in the sample. `--require-request-data` is the part that matters: without it `request_data` is only checked when present, so a worker that never emits it passes vacuously. This port logs payloads by **default**, which is why it caught a `request_data` bug the DEBUG-gated ports logged past; `--access-log-debug` exists only so the porting guide's canonical command line runs here unmodified, and must stay a no-op rather than becoming the inverse of `--access-log-no-payloads`.

That lane is a *pipe* run, where a whole stream call is one dispatch and one record — it says nothing about HTTP, where a stream is a chain of requests. The `http` lane therefore also runs `TestHttpStreamAccessLog` (`tests/test_java_conformance.py`), which drives producer / exchange / failing streams against a worker started with `--access-log` and asserts the records **exist** and have the right shape before validating them. Presence is the assertion that matters: the schema validator reported PASS over a log with zero stream records for as long as the bug existed. The correlation half — `X-Request-ID` on the response equalling `request_id` in the log — is the shared suite's `TestRequestId`, gated on the `conformance_http_access_log` fixture in the same file.

## When in doubt

1. Check the Python reference at `~/Development/vgi-rpc/vgi_rpc/` — behavior there is authoritative.
2. Check `~/Development/vgi-rpc/CLAUDE.md` for the higher-level architectural summary.
3. Run `./run_tests.sh <keyword>` to see whether the conformance suite already exercises the behavior you're changing.
