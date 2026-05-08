// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

/**
 * Optional interface for {@link StreamState} subclasses that want to control
 * their own wire encoding for HTTP state-token round-trips.
 *
 * <p>The default {@link farm.query.vgirpc.http.StateSerializer} reflects over
 * fields and serialises them as JSON via Jackson, which only works when every
 * field is JSON-friendly (primitives, strings, JSON-deserialisable records).
 * State classes that hold richer data (Arrow schemas, batches, byte arrays
 * with binary semantics) implement this interface to take over encoding —
 * the framework calls {@link #encode()} on the way out and a no-arg
 * constructor + {@link #decode(byte[])} on the way back in.</p>
 *
 * <p>Implementing classes MUST expose a public no-arg constructor; the
 * deserialiser instantiates a blank state and then hands it the bytes.</p>
 */
public interface PortableStreamState {

    /** Serialise this state to a self-contained byte array. */
    byte[] encode();

    /** Restore state from bytes produced by {@link #encode()}. Mutates {@code this}. */
    void decode(byte[] data);
}
