// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.schema;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * The application protocol version a service interface speaks, stamped on every
 * request a client of that interface sends as
 * {@code vgi_rpc.protocol_version}.
 *
 * <pre>{@code
 * @ProtocolVersion("1.3.0")
 * public interface VgiService { ... }
 * }</pre>
 *
 * <h2>Why it lives on the interface</h2>
 *
 * <p>The version describes the <em>wire contract</em>, which is exactly what
 * the interface is; putting it anywhere else lets a client and a server that
 * share the interface disagree about which revision of it they are speaking.
 * Declaring it here means {@code connection.proxy(VgiService.class)} sends the
 * right version with no call-site ceremony — and a caller who forgets does not
 * silently produce a client that a versioned worker rejects on every call.
 *
 * <p>Absent (or blank), no version key is sent. That is the correct behaviour
 * for a protocol that has no versioning: a peer that does not check cannot be
 * upset by the key's absence, and a peer that does check is telling you the
 * interface needs this annotation.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface ProtocolVersion {

    /**
     * The version string, conventionally {@code major.minor.patch}. Peers that
     * enforce it typically require an exact major+minor match.
     *
     * @return the declared protocol version
     */
    String value();
}
