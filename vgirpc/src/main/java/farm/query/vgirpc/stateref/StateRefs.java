// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.stateref;

import farm.query.vgirpc.AuthContext;
import farm.query.vgirpc.external.ObjectMissingException;
import farm.query.vgirpc.external.ObjectStore;
import farm.query.vgirpc.http.StateToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.function.Supplier;

/**
 * Framework helpers for {@link StateRef}: encoding at a cursor mint, resolving in a
 * stream's scope, and the sealing scopes (STATE_REF_V1.md §3-§5).
 *
 * <pre>
 * STATEREF_TAG_AAD    = "vgi_rpc.stateref.v1\0"        || BASE_AAD(auth, protocol) || call_id
 * STATEREF_OBJECT_AAD = "vgi_rpc.stateref.object.v1\0" || BASE_AAD(auth, protocol) || call_id
 * ARGREF_AAD          = "vgi_rpc.argref.v1\0"          || BASE_AAD(auth, SERVER_SCOPE)
 * </pre>
 *
 * <p>{@code BASE_AAD} is exactly this server's cursor-token AAD
 * ({@link StateToken#baseAad(AuthContext, String)}).</p>
 */
public final class StateRefs {

    private static final Logger LOG = LoggerFactory.getLogger("farm.query.vgirpc.state_ref");

    /** Scope for tokens that belong to the server rather than to any one protocol. */
    public static final String SERVER_SCOPE = "\u0000server";

    static final byte[] TAG_AAD_PREFIX = "vgi_rpc.stateref.v1\0".getBytes(StandardCharsets.UTF_8);
    static final byte[] OBJECT_AAD_PREFIX = "vgi_rpc.stateref.object.v1\0".getBytes(StandardCharsets.UTF_8);
    static final byte[] ARGREF_AAD_PREFIX = "vgi_rpc.argref.v1\0".getBytes(StandardCharsets.UTF_8);

    static final byte TAG_VERSION = 0x01;
    static final int SHA256_LEN = 32;
    static final int MAX_OBJECT_ID_BYTES = 1024;
    static final int TAG_FIXED_LEN = 1 + 8 + 8 + SHA256_LEN + 2;

    private StateRefs() {}

    /** Everything needed to decide a ref's form and seal it, for one cursor mint. */
    static final class Encoder {
        final StateRefRuntime runtime;
        final byte[] baseAad;
        final byte[] callId;
        final long inlineBytes;
        final String protocol;
        final String method;

        Encoder(StateRefRuntime runtime, byte[] baseAad, byte[] callId, long inlineBytes,
                String protocol, String method) {
            this.runtime = runtime;
            this.baseAad = baseAad;
            this.callId = callId;
            this.inlineBytes = inlineBytes;
            this.protocol = protocol;
            this.method = method;
        }
    }

    /**
     * {@code BASE_AAD}: the cursor-token AAD for {@code auth} and {@code protocol}.
     *
     * @param auth the request's authentication context
     * @param protocol the owning protocol's wire name, or {@link #SERVER_SCOPE}
     * @return the AAD bytes
     */
    public static byte[] baseAad(AuthContext auth, String protocol) {
        return StateToken.baseAad(auth, protocol);
    }

    static byte[] tagAad(byte[] base, byte[] callId) {
        return Sealing.concat(TAG_AAD_PREFIX, base, callId);
    }

    static byte[] objectAad(byte[] base, byte[] callId) {
        return Sealing.concat(OBJECT_AAD_PREFIX, base, callId);
    }

    /**
     * {@code ARGREF_AAD}: bound to the principal, not to a protocol.
     *
     * @param auth the request's authentication context
     * @return the AAD bytes
     */
    public static byte[] argumentRefAad(AuthContext auth) {
        return Sealing.concat(ARGREF_AAD_PREFIX, baseAad(auth, SERVER_SCOPE));
    }

    /**
     * Run {@code body} with the encoder that decides un-encoded refs' forms installed, as
     * the HTTP server does around each cursor mint.
     *
     * @param runtime the server runtime
     * @param auth the request's authentication context
     * @param protocol owning protocol
     * @param method method name
     * @param callId the stream's call id
     * @param callTokenBytes sealed call-token length (enters the inline limit)
     * @param body serializes the state
     * @param <T> the result type
     * @return {@code body}'s result
     */
    public static <T> T encodingForStream(StateRefRuntime runtime, AuthContext auth, String protocol,
                                          String method, byte[] callId, long callTokenBytes, Supplier<T> body) {
        Encoder encoder = encoder(runtime, auth, protocol, method, callId, callTokenBytes);
        AutoCloseable restore = StateRef.installEncoder(encoder);
        try {
            return body.get();
        } finally {
            try {
                restore.close();
            } catch (Exception ignored) {
                // restoring a ThreadLocal cannot fail
            }
        }
    }

    private static Encoder encoder(StateRefRuntime runtime, AuthContext auth, String protocol, String method,
                                   byte[] callId, long callTokenBytes) {
        return new Encoder(runtime, baseAad(auth, protocol), callId.clone(), runtime.inlineBytes(callTokenBytes),
                protocol, method);
    }

    /**
     * Encode {@code value} as a {@link StateRef} directly (the call token's schemas).
     *
     * @param runtime the server runtime
     * @param value the value bytes
     * @param auth the request's authentication context
     * @param protocol owning protocol
     * @param method method name
     * @param callId the stream's call id
     * @param callTokenBytes sealed call-token length for the inline limit ({@code 0} for the
     *     call token's own schemas)
     * @return the encoded ref bytes
     */
    public static byte[] encodeValue(StateRefRuntime runtime, byte[] value, AuthContext auth, String protocol,
                                     String method, byte[] callId, long callTokenBytes) {
        return encodeValue(value, encoder(runtime, auth, protocol, method, callId, callTokenBytes));
    }

    /**
     * Resolve encoded {@link StateRef} bytes for the stream {@code callId} (the call token's
     * schemas).
     *
     * @param runtime the server runtime
     * @param encoded the encoded ref
     * @param auth the request's authentication context
     * @param protocol owning protocol
     * @param callId the stream's call id
     * @return the value bytes
     */
    public static byte[] resolveEncoded(StateRefRuntime runtime, byte[] encoded, AuthContext auth,
                                        String protocol, byte[] callId) {
        return resolveEncoded(encoded, runtime, baseAad(auth, protocol), callId);
    }

    /** Decide a value's form and encode it (§5.2), storing it when it is stored. */
    static byte[] encodeValue(byte[] value, Encoder encoder) {
        byte[] payload = TokenCodec.pack(value);
        StateRefRuntime runtime = encoder.runtime;
        ObjectStore storage = runtime.storage();
        if (storage == null || payload.length <= encoder.inlineBytes) {
            LOG.debug("state_ref.store protocol={} method={} payload_bytes={} object_bytes=0 form=inline",
                    encoder.protocol, encoder.method, payload.length);
            return Sealing.concat(new byte[] {StateRef.FORM_INLINE}, payload);
        }
        byte[] objectBytes = Sealing.seal(payload, runtime.key(), objectAad(encoder.baseAad, encoder.callId));
        if (objectBytes.length > runtime.maxFetchBytes()) {
            throw new StateRefTooLargeError("StateRef object for " + encoder.protocol + "." + encoder.method
                    + " is " + objectBytes.length + " bytes, over this server's max_fetch_bytes ("
                    + runtime.maxFetchBytes() + "); no replica could fetch it back");
        }
        String objectId;
        try {
            objectId = storage.putObject(objectBytes);
        } catch (StateRefError e) {
            throw e;
        } catch (Exception e) {
            throw new StorageUnavailableError("object storage write failed (" + e.getClass().getSimpleName()
                    + "): " + e.getMessage());
        }
        byte[] idBytes = objectId == null ? new byte[0] : objectId.getBytes(StandardCharsets.UTF_8);
        if (idBytes.length < 1 || idBytes.length > MAX_OBJECT_ID_BYTES) {
            throw new IllegalStateException("storage returned an object_id of " + idBytes.length
                    + " bytes (must be 1-1024)");
        }
        ByteBuffer tag = ByteBuffer.allocate(TAG_FIXED_LEN + idBytes.length).order(ByteOrder.LITTLE_ENDIAN);
        tag.put(TAG_VERSION);
        tag.putLong(runtime.nowSeconds());
        tag.putLong(objectBytes.length);
        tag.put(sha256(objectBytes));
        tag.putShort((short) idBytes.length);
        tag.put(idBytes);
        byte[] sealedTag = Sealing.seal(tag.array(), runtime.key(), tagAad(encoder.baseAad, encoder.callId));
        LOG.debug("state_ref.store protocol={} method={} payload_bytes={} object_bytes={} form=stored",
                encoder.protocol, encoder.method, payload.length, objectBytes.length);
        return Sealing.concat(new byte[] {StateRef.FORM_STORED}, sealedTag);
    }

    static byte[] unpackValue(byte[] payload, long cap) {
        try {
            return TokenCodec.unpack(payload, cap);
        } catch (TokenCodec.TokenCodecException e) {
            throw new InvalidStateRefError("StateRef payload is malformed: " + e.getMessage());
        }
    }

    /** Resolve encoded bytes in a stream scope; {@code runtime} null off HTTP. */
    static byte[] resolveEncoded(byte[] encoded, StateRefRuntime runtime, byte[] base, byte[] callId) {
        if (encoded == null || encoded.length == 0) throw new InvalidStateRefError("StateRef is empty");
        byte form = encoded[0];
        byte[] rest = new byte[encoded.length - 1];
        System.arraycopy(encoded, 1, rest, 0, rest.length);
        if (form == StateRef.FORM_INLINE) {
            long cap = runtime != null ? runtime.decompressCap() : TokenCodec.MAX_TOKEN_PLAINTEXT_BYTES;
            byte[] value = unpackValue(rest, cap);
            LOG.debug("state_ref.resolve form=inline cache=miss bytes={}", value.length);
            return value;
        }
        if (form == StateRef.FORM_STORED) {
            if (runtime == null || base == null || callId == null) {
                throw new InvalidStateRefError("a stored StateRef cannot be resolved outside an HTTP stream turn");
            }
            return resolveStored(rest, runtime, base, callId);
        }
        throw new InvalidStateRefError(String.format("StateRef has unknown form 0x%02x", form & 0xff));
    }

    private static byte[] resolveStored(byte[] tagEnvelope, StateRefRuntime runtime, byte[] base, byte[] callId) {
        byte[] tag;
        try {
            tag = Sealing.open(tagEnvelope, runtime.key(), tagAad(base, callId));
        } catch (Sealing.SealException e) {
            throw new InvalidStateRefError("StateRef tag does not open (tampered, or minted for another stream)");
        }
        if (tag.length < TAG_FIXED_LEN || tag[0] != TAG_VERSION) {
            throw new InvalidStateRefError("StateRef tag has an unknown version or a malformed layout");
        }
        ByteBuffer bb = ByteBuffer.wrap(tag).order(ByteOrder.LITTLE_ENDIAN);
        bb.get();
        long createdAt = bb.getLong();
        long size = bb.getLong();
        byte[] digest = new byte[SHA256_LEN];
        bb.get(digest);
        int idLen = Short.toUnsignedInt(bb.getShort());
        if (idLen < 1 || idLen > MAX_OBJECT_ID_BYTES || TAG_FIXED_LEN + idLen != tag.length) {
            throw new InvalidStateRefError("StateRef tag has a malformed layout");
        }
        String objectId = utf8(tag, TAG_FIXED_LEN, idLen, "StateRef tag has a malformed layout", true);
        long ttl = runtime.tokenTtlSeconds();
        if (ttl > 0 && runtime.nowSeconds() - createdAt > ttl) {
            throw new InvalidStateRefError("StateRef expired");
        }
        if (size < 0 || size > runtime.maxFetchBytes()) {
            throw new StateRefTooLargeError("StateRef object is " + Long.toUnsignedString(size)
                    + " bytes, over this server's max_fetch_bytes (" + runtime.maxFetchBytes() + ")");
        }
        ObjectStore storage = runtime.storage();
        if (storage == null) {
            throw new InvalidStateRefError("a stored StateRef cannot be resolved: this server has no object storage");
        }
        ObjectCache.Lookup lookup = runtime.cache().getOrFetch(objectId, () -> {
            byte[] objectBytes = fetchObject(storage, objectId, runtime.maxFetchBytes(), true);
            if (objectBytes.length != size) throw new InvalidStateRefError("StateRef object size mismatch");
            if (!MessageDigest.isEqual(sha256(objectBytes), digest)) {
                throw new InvalidStateRefError("StateRef object digest mismatch");
            }
            byte[] payload;
            try {
                payload = Sealing.open(objectBytes, runtime.key(), objectAad(base, callId));
            } catch (Sealing.SealException e) {
                throw new InvalidStateRefError("StateRef object does not open");
            }
            return unpackValue(payload, runtime.decompressCap());
        });
        LOG.debug("state_ref.resolve form=stored cache={} bytes={}", lookup.hit() ? "hit" : "miss",
                lookup.value().length);
        return lookup.value();
    }

    /** {@code storage.get} with the error mapping both resolvers share. */
    static byte[] fetchObject(ObjectStore storage, String objectId, long maxBytes, boolean stateRef) {
        try {
            return storage.get(objectId, maxBytes);
        } catch (ObjectMissingException e) {
            if (stateRef) throw new StateRefMissingError();
            throw new ArgumentsRefMissingError();
        } catch (StateRefError e) {
            throw e;
        } catch (Exception e) {
            throw new StorageUnavailableError("object storage fetch failed (" + e.getClass().getSimpleName()
                    + "): " + e.getMessage());
        }
    }

    static String utf8(byte[] data, int off, int len, String error, boolean stateRef) {
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(data, off, len)).toString();
        } catch (CharacterCodingException e) {
            if (stateRef) throw new InvalidStateRefError(error);
            throw new InvalidArgumentsRefError(error);
        }
    }

    static byte[] sha256(byte[] data) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(data);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }
}
