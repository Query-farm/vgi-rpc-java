<p align="center">
  <img src="assets/vgi-logo.png" alt="Vector Gateway Interface logo" width="320">
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
- **Transport-agnostic** — stdio pipe, subprocess, Unix domain socket, shared memory, or HTTP.
- **Automatic schema inference** — Java types and `record` components map to Arrow types; `@ArrowField` refines them.
- **Pluggable authentication** — `AuthContext` + authenticators for HTTP (bearer, mTLS/XFCC; JWT/OAuth in the optional `vgirpc-oauth` module).
- **Runtime introspection** — opt-in `__describe__` RPC for dynamic service discovery, with a protocol hash that matches the Python reference byte-for-byte.
- **Shared-memory transport** — zero-copy batch transfer between co-located processes (auto-negotiated on JDK 22+ via a multi-release overlay; transparent pipe fallback otherwise).
- **Large-batch externalization** — oversized batches transparently spilled to S3 (`vgirpc-s3`) or GCS (`vgirpc-gcs`).

## Requirements

- **Java 21+** at runtime. The shared-memory side-channel additionally requires **JDK 22+** (where `java.lang.foreign` is GA); on 21 it transparently falls back to inline transfer.

## Installation

Artifacts are published to Maven Central under the `farm.query` group.

**Gradle (Kotlin DSL):**

```kotlin
dependencies {
    implementation("farm.query:vgirpc:0.8.0")          // core: protocol, transports, HTTP, schema
    implementation("farm.query:vgirpc-oauth:0.8.0")    // optional: JWT / OAuth / PKCE auth
    implementation("farm.query:vgirpc-s3:0.8.0")       // optional: S3 external storage
    implementation("farm.query:vgirpc-gcs:0.8.0")      // optional: GCS external storage
}
```

**Maven:**

```xml
<dependency>
  <groupId>farm.query</groupId>
  <artifactId>vgirpc</artifactId>
  <version>0.8.0</version>
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

var transport = new SubprocessTransport(List.of("java", "-cp", "worker.jar", "CalculatorWorker"));
try (RpcConnection conn = new RpcConnection(transport)) {
    Calculator calc = conn.proxy(Calculator.class);
    double sum    = calc.add(2.0, 3.0);   // 5.0
    String hello  = calc.greet("World");  // "Hello, World!"
}
```

> Compile services with `-parameters` (this project already does): the framework binds call arguments by parameter name, matching the Python reference's keyword-argument wire semantics.

## Modules

| Module | Purpose |
|---|---|
| **`vgirpc`** | Core library — wire protocol, transports, HTTP server/client (Jetty 12), schema derivation, marshalling, external-location support, shared-memory primitive. |
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

## Method types

- **Unary** — request batch in, one result (or error) batch out.
- **Streaming** — a `RpcStream<S extends StreamState>` whose state's `process(input, out, ctx)` runs once per tick, in two flavours: **producer** (server emits a sequence of output batches) and **exchange** (lockstep input batch → output batch).

## Wire compatibility

When the Python and Java implementations disagree, **Python is the reference.** Wire format, metadata keys, error semantics, and stream-state token layout match byte-for-byte so the two interoperate. See the Python project's [README](https://github.com/Query-farm/vgi-rpc-python) for the higher-level protocol design.

## License

[Apache License 2.0](LICENSE) — Copyright 2026 Query Farm LLC · https://query.farm
