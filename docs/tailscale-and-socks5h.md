# Tailscale evidence and SOCKS5h dialing

`TailscalePeerIdentityProviders.serve(...)` accepts Serve headers only from an
exact configured immediate-peer IP literal. Configuration performs no DNS:
hostnames, CIDRs, endpoints with ports, bracketed/zone-qualified IPv6, invalid
literals, and duplicates after IPv6 or IPv4-mapped normalization are rejected.
The physical peer is normalized the same way before comparison. The backend
must be unreachable except through that proxy, and Serve must replace the headers. Funnel requests are
`not_applicable` only when the structured header is exactly `?1`; malformed
Funnel values are invalid. A Serve login is verified evidence within that proxy boundary,
but is deliberately a login-stability subject; capability-only requests remain
subjectless. Capabilities are bounded opaque JSON arrays and are never logged or
treated as a VGI role language.

`TailscaleLocalApiProvider` snapshots a raw TCP peer and performs one LocalAPI
WhoIs request per connection. It does not cache and never invokes the Tailscale
CLI. Untagged nodes use `user:<numeric-id>`; tagged nodes use
`node:<stable-node-id>` and do not use their `UserProfile` as the caller.
Destination- and service-scoped capability targets are retained in the evidence.
Supported clients are:

- `UnixLocalApiClient` for a configured Unix-domain socket;
- `HttpLocalApiClient` for an explicit local HTTP/token endpoint, including the
  macOS userspace endpoint when supplied by the operator.

The baseline JDK has no native Windows named-pipe HTTP transport. Java therefore
requires an explicitly exposed local HTTP/token endpoint on Windows; this is a
documented platform gap, not automatic discovery.

`TcpSocketTransport.connect(host, port, proxy, timeout)` adds an explicit
credential-free `socks5h://host:port` raw TCP path. Target hostnames are converted
to IDNA ASCII and sent to the proxy without local target DNS. IPv4 and IPv6
literals use their native SOCKS address types. A single monotonic setup budget
covers proxy resolution elapsed time, connection, negotiation, and target
connection. Failure never falls back to direct TCP, process proxy environment is
ignored, and successful sockets use `TCP_NODELAY`.

With the built-in `anyOf(...)` policy, valid application authentication wins and
peer evidence is observation-only. Applications that need authorization or
state binding to require peer evidence must use `require(...)`, `allOf(...)`, or
a custom composition policy that performs that binding explicitly.

JDK `HttpClient` does not expose a supported per-request socket/connect callback,
and its SOCKS `ProxySelector` path does not provide the required guaranteed
proxy-side DNS semantics. Consequently the built-in Java HTTP RPC client does not
claim SOCKS5h support. Applications may supply a separately implemented HTTP
client, but VGI does not silently weaken the transport contract.

The adversarial tests mirror the canonical transport identity vectors maintained
by the Python conformance repository; this module has no runtime dependency on a
sibling checkout.
