// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.stateref;

import farm.query.vgirpc.AuthContext;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgirpc.schema.ArrowSerializableRecord;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;

/**
 * The {@link StateRef} view of one request: what {@link farm.query.vgirpc.CallContext}
 * delegates to.
 *
 * <p>The framework attaches one to every context it builds. On HTTP it carries the server
 * runtime and the stream's {@code call_id}; on the byte-stream transports there is no
 * runtime (values never leave memory). {@link #isInit()} is true only for the context
 * handed to the stream method itself, while it runs — the one place {@code stateRef()} is
 * allowed (STATE_REF_V1.md §2.1). Every other context — {@code process}, {@code onCancel},
 * unary calls, and the first producer tick that HTTP folds into {@code /init} — refuses it,
 * so worker code that works on one transport works on all.</p>
 */
public final class StateRefScope {

    private final StateRefRuntime runtime;
    private final AuthContext auth;
    private final String protocol;
    private final String method;
    private final byte[] callId;
    private volatile boolean init;
    private final Map<ByteBuffer, StateRef> memoBytes = new HashMap<>();
    private final Map<Object, StateRef> memoIds = new IdentityHashMap<>();

    /**
     * @param runtime the server's runtime (HTTP), or {@code null}
     * @param auth the request's authentication context
     * @param protocol owning protocol's wire name
     * @param method method name
     * @param callId the stream's call id, known on continuations; {@code null} otherwise
     * @param init whether this is the stream method's own context
     */
    public StateRefScope(StateRefRuntime runtime, AuthContext auth, String protocol, String method,
                         byte[] callId, boolean init) {
        this.runtime = runtime;
        this.auth = auth;
        this.protocol = protocol != null ? protocol : "";
        this.method = method != null ? method : "";
        this.callId = callId != null ? callId.clone() : null;
        this.init = init;
    }

    /** @return whether {@code stateRef()} is currently allowed */
    public boolean isInit() {
        return init;
    }

    /** Close the init window: called by the framework once the stream method has returned. */
    public void endInit() {
        init = false;
    }

    /** @return the server runtime, or {@code null} off HTTP */
    public StateRefRuntime runtime() {
        return runtime;
    }

    /**
     * Wrap {@code value} in a {@link StateRef}.
     *
     * @param value {@code byte[]} or an {@link ArrowSerializableRecord}; treat it as immutable
     *     afterwards
     * @return the ref; identical or byte-equal values within one init share one ref (and so
     *     one stored object)
     * @throws StateRefMisuseError when not handling a stream's init request
     * @throws IllegalArgumentException when {@code value} is neither bytes nor a record
     */
    public synchronized StateRef stateRef(Object value) {
        if (!init) {
            throw new StateRefMisuseError("state_ref() may only be called while handling a stream's init request ("
                    + protocol + "." + method + ")");
        }
        if (value == null) throw new IllegalArgumentException("state_ref() takes bytes or a record, got null");
        StateRef remembered = memoIds.get(value);
        if (remembered != null) return remembered;
        byte[] data;
        Object record = null;
        if (value instanceof byte[] bytes) {
            data = bytes.clone();
        } else if (value instanceof ArrowSerializableRecord r) {
            data = RecordCodec.serializeToBytes(r);
            record = r;
        } else {
            throw new IllegalArgumentException("state_ref() takes byte[] or an ArrowSerializableRecord, got "
                    + value.getClass().getName());
        }
        ByteBuffer key = ByteBuffer.wrap(data);
        StateRef ref = memoBytes.get(key);
        if (ref == null) {
            ref = StateRef.ofValue(data, record);
            memoBytes.put(key, ref);
        }
        memoIds.put(value, ref);
        return ref;
    }

    /**
     * The value bytes {@code ref} names. Valid on every turn and every transport.
     *
     * @param ref the ref, as read from the stream state
     * @return the value's serialized bytes (shared: treat as immutable)
     * @throws InvalidStateRefError unknown form, a tag that does not open for this stream and
     *     principal, an expired tag, or a digest mismatch
     * @throws StateRefMissingError storage reports the object gone
     * @throws StateRefTooLargeError the object exceeds {@code max_fetch_bytes}
     */
    public byte[] resolve(StateRef ref) {
        if (ref == null) throw new IllegalArgumentException("resolve() takes a StateRef, got null");
        byte[] value = ref.memoryValue();
        if (value != null) return value;
        byte[] base = runtime != null ? StateRefs.baseAad(auth, protocol) : null;
        return StateRefs.resolveEncoded(ref.rawEncoded(), runtime, base, callId);
    }

    /**
     * Resolve {@code ref} into a record.
     *
     * @param ref the ref
     * @param type the record type
     * @param <R> the record type
     * @return the record (the original instance while the ref still holds it in memory —
     *     treat it as immutable)
     */
    public <R extends ArrowSerializableRecord> R resolve(StateRef ref, Class<R> type) {
        if (ref != null && ref.memoryValue() != null && type.isInstance(ref.memoryRecord())) {
            return type.cast(ref.memoryRecord());
        }
        return RecordCodec.deserializeFromBytes(resolve(ref), type);
    }

    /**
     * Resolve an argument reference minted by this server's {@code __upload_url__} grant.
     *
     * @param ref the grant's {@code ref} column value
     * @param size the byte length claimed alongside the ref
     * @param sha256 the SHA-256 hex claimed alongside the ref
     * @return the verified object bytes
     * @throws InvalidArgumentsRefError off HTTP, or any failure {@link ArgumentRefs#resolve}
     *     maps to it
     */
    public byte[] resolveArgumentRef(byte[] ref, long size, String sha256) {
        if (runtime == null) {
            throw new InvalidArgumentsRefError("argument references can only be resolved by an HTTP server");
        }
        return ArgumentRefs.resolve(ref, size, sha256, runtime.key(), auth, runtime.storage(),
                runtime.maxFetchBytes(), runtime.cache());
    }
}
