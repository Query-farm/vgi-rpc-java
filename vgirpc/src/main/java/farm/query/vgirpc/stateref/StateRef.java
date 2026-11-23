// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.stateref;

import java.util.Arrays;

/**
 * An immutable value carried in stream state by reference (STATE_REF_V1.md).
 *
 * <p>On HTTP every continuation echoes the stream's state back to the server. A
 * {@code StateRef} is how a server says "this part of my stream state does not change":
 * store it once, carry a small sealed tag in the state, read it back on any later turn on
 * any replica.</p>
 *
 * <pre>{@code
 * public RpcStream<ScanState> scan(byte[] query, CallContext ctx) {
 *     StateRef ref = ctx.stateRef(query);              // only while handling init
 *     return RpcStream.producer(SCHEMA, new ScanState(ref));
 * }
 *
 * final class ScanState extends ProducerState {
 *     StateRef query;                                  // a field of type StateRef
 *     public void produce(OutputCollector out, CallContext ctx) {
 *         byte[] q = ctx.resolve(query);              // any turn, any replica
 *     }
 * }
 * }</pre>
 *
 * <p>The value is held in memory until the state is first serialized into an HTTP state
 * token. That serialization decides the form — inline (a compressed payload inside the
 * token) or stored (uploaded once, a sealed tag in the token) — and caches the encoded
 * bytes, so later serializations re-emit them exactly: no re-compression, no re-seal, no
 * second upload. A ref read back from a token holds only those bytes. On pipe, stdio,
 * Unix and TCP nothing is ever encoded.</p>
 *
 * <p>A field of type {@code StateRef} in a reflectively serialized state is handled by the
 * HTTP state serializer; a {@link farm.query.vgirpc.PortableStreamState} writes
 * {@link #toBytes()} and reads {@link #fromBytes(byte[])} itself. State transitions copy
 * the ref; they never call {@code stateRef()} again. A ref belongs to its stream.</p>
 *
 * <pre>
 * StateRef (inline) = 0x01 || codec || body
 * StateRef (stored) = 0x02 || 0x01 || nonce || AEAD(tag_plaintext, STATEREF_TAG_AAD)
 * </pre>
 */
public final class StateRef {

    static final byte FORM_INLINE = 0x01;
    static final byte FORM_STORED = 0x02;

    /** Installed by the HTTP server for the duration of one cursor mint. */
    private static final ThreadLocal<StateRefs.Encoder> ENCODER = new ThreadLocal<>();

    private final byte[] value;
    private final Object record;
    private volatile byte[] encoded;

    private StateRef(byte[] value, Object record, byte[] encoded) {
        this.value = value;
        this.record = record;
        this.encoded = encoded;
    }

    static StateRef ofValue(byte[] value, Object record) {
        return new StateRef(value, record, null);
    }

    /**
     * Rebuild a ref from its encoded bytes (validated only when resolved).
     *
     * @param encoded the bytes {@link #toBytes()} produced
     * @return a ref holding exactly those bytes
     */
    public static StateRef fromBytes(byte[] encoded) {
        if (encoded == null) throw new NullPointerException("encoded");
        return new StateRef(null, null, encoded.clone());
    }

    /**
     * Where the value lives.
     *
     * @return {@code "memory"} (not yet encoded), {@code "inline"}, {@code "stored"}, or
     *     {@code "invalid"} for bytes with an unknown form
     */
    public String form() {
        byte[] e = encoded;
        if (e == null) return "memory";
        if (e.length > 0 && e[0] == FORM_INLINE) return "inline";
        if (e.length > 0 && e[0] == FORM_STORED) return "stored";
        return "invalid";
    }

    /** @return the encoded bytes once the form is decided, else {@code null} */
    public byte[] encoded() {
        byte[] e = encoded;
        return e == null ? null : e.clone();
    }

    /**
     * The field's wire bytes, deciding the form on first serialization into a state token.
     *
     * <p>Inside a state-token mint (the HTTP server installs an encoder) the form is
     * decided, any store performed, and the result cached. Outside one — a caller
     * serializing state for its own reasons — an un-encoded ref renders inline without
     * caching, so the next token mint still decides properly.</p>
     *
     * @return the encoded bytes
     */
    public byte[] toBytes() {
        byte[] e = encoded;
        if (e != null) return e.clone();
        StateRefs.Encoder encoder = ENCODER.get();
        if (encoder == null) return Sealing.concat(new byte[] {FORM_INLINE}, TokenCodec.pack(value));
        // No lock around the encode: it may upload, and a ref is only ever un-encoded during
        // its own stream's init turn, which one thread runs.
        byte[] fresh = StateRefs.encodeValue(value, encoder);
        synchronized (this) {
            if (encoded == null) encoded = fresh;
            return encoded.clone();
        }
    }

    /** The value while still in memory, else {@code null}. */
    byte[] memoryValue() {
        return value;
    }

    /** The original record while still in memory, else {@code null}. */
    Object memoryRecord() {
        return record;
    }

    /** The encoded bytes without copying, or {@code null}. */
    byte[] rawEncoded() {
        return encoded;
    }

    static AutoCloseable installEncoder(StateRefs.Encoder encoder) {
        StateRefs.Encoder previous = ENCODER.get();
        ENCODER.set(encoder);
        return () -> {
            if (previous == null) ENCODER.remove();
            else ENCODER.set(previous);
        };
    }

    private byte[] identity() {
        byte[] e = encoded;
        if (e != null) return e;
        return Sealing.concat(new byte[] {(byte) 0xff}, value);
    }

    @Override
    public boolean equals(Object o) {
        return o == this || (o instanceof StateRef other && Arrays.equals(identity(), other.identity()));
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(identity());
    }

    /** Describe the form and size, never the value. */
    @Override
    public String toString() {
        byte[] e = encoded;
        if (e == null) return "StateRef[form=memory, bytes=" + value.length + "]";
        return "StateRef[form=" + form() + ", encodedBytes=" + e.length + "]";
    }
}
