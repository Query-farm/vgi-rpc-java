// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.schema;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * The wire name of the protocol a service interface defines — its routing key.
 *
 * <pre>{@code
 * @ProtocolName("vgi.v2")
 * @ProtocolVersion("2.0.0")
 * public interface VgiService { ... }
 * }</pre>
 *
 * <p>The name rides every request as {@code vgi_rpc.protocol}, and over HTTP it is also the
 * protocol path segment. Absent, the name falls back to the interface's simple name, which is
 * what every interface that predates this annotation keeps.
 *
 * <h2>Why declare rather than derive</h2>
 *
 * <p>A derived name is an accident of the local type system, and the local type system differs
 * per language. Six implementations of one protocol derived four different names — {@code
 * VgiProtocol}, {@code VgiService}, {@code Service}, {@code vgi} — and no client could address
 * them all, which stayed invisible until the routing key became required. A wire name is a
 * cross-port contract and has to be written down somewhere a reader can see it.
 *
 * <p>A Java simple name additionally cannot express the convention the contract uses: {@code
 * vgi.v2} and {@code vgi_rpc.Reflection.v1} carry a major version in a dot-qualified name, and
 * no Java identifier contains a dot. Renaming the interface is not an available workaround.
 *
 * <h2>Put the major version in the name</h2>
 *
 * <p>Following gRPC's AIP-185, Kubernetes API groups and D-Bus: an incompatible major becomes a
 * <em>different</em> protocol and therefore a 404 — an answer every proxy, WAF and load balancer
 * understands without an Arrow parser — and {@code foo.v1} and {@code foo.v2} can be served side
 * by side while clients migrate.
 *
 * <h2>Not inherited</h2>
 *
 * <p>Read from the interface's <em>own</em> declaration. An interface that extends another and
 * does not redeclare gets its own simple name, not its parent's wire name: silently sharing a
 * routing key with a parent is how a fixture that subclasses a protocol to vary one thing ends
 * up impersonating it. Declare it again on the subinterface when that is what you mean.
 *
 * <p>The name must match the protocol-name grammar ({@code [A-Za-z_][A-Za-z0-9_.]*}, at most 255
 * UTF-8 bytes) and may not claim the {@code vgi_rpc.} prefix, which is reserved for protocols the
 * framework itself defines. A violation is an {@link IllegalArgumentException} at introspection
 * time — at worker construction, not on the first request.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface ProtocolName {

    /**
     * The wire name, conventionally dot-qualified with a major version (e.g. {@code vgi.v2}).
     *
     * @return the declared protocol name
     */
    String value();
}
