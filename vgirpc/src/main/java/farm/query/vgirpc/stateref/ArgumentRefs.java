// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.stateref;

import farm.query.vgirpc.AuthContext;
import farm.query.vgirpc.external.ObjectStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.regex.Pattern;

/**
 * Argument references (STATE_REF_V1.md §10.2): the sealed {@code ref} column of an
 * {@code __upload_url__/init} grant, and the resolution primitive the vgi layer calls.
 *
 * <pre>
 * argument ref = 0x01 || nonce || AEAD(0x01 || size u64le || sha256[32] || id_len u16le || object_id,
 *                                      ARGREF_AAD)
 * </pre>
 *
 * <p>Minted from the <em>claimed</em> {@code (size, sha256)} without seeing the bytes;
 * storage enforces them on the PUT where it can, and the resolver verifies them on the GET.
 * There is no expiry: on an unauthenticated server a reference is a bearer capability for as
 * long as the object lives (bucket lifecycle, about 1-2 days).</p>
 */
public final class ArgumentRefs {

    private static final Logger LOG = LoggerFactory.getLogger("farm.query.vgirpc.state_ref");
    private static final byte VERSION = 0x01;
    private static final int FIXED_LEN = 1 + 8 + StateRefs.SHA256_LEN + 2;
    private static final Pattern SHA256_HEX = Pattern.compile("[0-9a-f]{64}");

    private ArgumentRefs() {}

    /**
     * The opened contents of an argument reference.
     *
     * @param size the sealed byte length
     * @param sha256 the sealed SHA-256, 64 lowercase hex
     * @param objectId the storage id
     */
    public record ArgumentRef(long size, String sha256, String objectId) {}

    /**
     * Seal an argument reference for one granted upload.
     *
     * @param tokenKey the server's AEAD key
     * @param auth the {@code __upload_url__/init} request's authentication context
     * @param size the entry's declared size ({@code >= 1})
     * @param sha256 the entry's declared SHA-256, 64 lowercase hex
     * @param objectId the storage id the provider returned
     * @return the sealed reference
     * @throws IllegalArgumentException on a bad size, digest or object id
     */
    public static byte[] mint(byte[] tokenKey, AuthContext auth, long size, String sha256, String objectId) {
        if (size < 1) throw new IllegalArgumentException("argument ref size must be an integer >= 1, got " + size);
        if (sha256 == null || !SHA256_HEX.matcher(sha256).matches()) {
            throw new IllegalArgumentException("argument ref sha256 must be 64 lowercase hex characters");
        }
        byte[] id = objectId == null ? new byte[0] : objectId.getBytes(StandardCharsets.UTF_8);
        if (id.length < 1 || id.length > StateRefs.MAX_OBJECT_ID_BYTES) {
            throw new IllegalArgumentException("object_id must be 1-" + StateRefs.MAX_OBJECT_ID_BYTES
                    + " bytes of UTF-8, got " + id.length);
        }
        ByteBuffer plain = ByteBuffer.allocate(FIXED_LEN + id.length).order(ByteOrder.LITTLE_ENDIAN);
        plain.put(VERSION);
        plain.putLong(size);
        plain.put(HexFormat.of().parseHex(sha256));
        plain.putShort((short) id.length);
        plain.put(id);
        byte[] sealed = Sealing.seal(plain.array(), tokenKey, StateRefs.argumentRefAad(auth));
        LOG.debug("arguments_ref.mint size={}", size);
        return sealed;
    }

    /**
     * Open an argument reference sealed for {@code auth}.
     *
     * @param tokenKey the server's AEAD key
     * @param auth the presenting request's authentication context
     * @param ref the sealed reference
     * @return the opened reference
     * @throws InvalidArgumentsRefError wrong key, principal or scope, tampering, unknown
     *     version, or a malformed layout
     */
    public static ArgumentRef open(byte[] tokenKey, AuthContext auth, byte[] ref) {
        byte[] plain;
        try {
            plain = Sealing.open(ref, tokenKey, StateRefs.argumentRefAad(auth));
        } catch (Sealing.SealException e) {
            throw new InvalidArgumentsRefError(
                    "argument reference does not open (tampered, or granted to another principal or server)");
        }
        if (plain.length < FIXED_LEN || plain[0] != VERSION) {
            throw new InvalidArgumentsRefError("argument reference has an unknown version or a malformed layout");
        }
        ByteBuffer bb = ByteBuffer.wrap(plain).order(ByteOrder.LITTLE_ENDIAN);
        bb.get();
        long size = bb.getLong();
        byte[] digest = new byte[StateRefs.SHA256_LEN];
        bb.get(digest);
        int idLen = Short.toUnsignedInt(bb.getShort());
        if (idLen < 1 || idLen > StateRefs.MAX_OBJECT_ID_BYTES || FIXED_LEN + idLen != plain.length) {
            throw new InvalidArgumentsRefError("argument reference has a malformed layout");
        }
        String objectId = StateRefs.utf8(plain, FIXED_LEN, idLen, "argument reference has a malformed layout", false);
        return new ArgumentRef(size, HexFormat.of().formatHex(digest), objectId);
    }

    /**
     * Open, fetch (through the object cache) and verify an argument reference.
     *
     * <p>Order: validate the claimed location; open the ref with {@code ARGREF_AAD}; check
     * that the sealed size and SHA-256 equal the claimed ones; look the {@code object_id} up
     * in the object cache; on a miss, single-flight {@code storage.get}; verify length and
     * SHA-256; return.</p>
     *
     * @param ref the sealed reference
     * @param size the claimed byte length ({@code ArgumentsLocation.size})
     * @param sha256 the claimed SHA-256, 64 lowercase hex
     * @param tokenKey the server's AEAD key
     * @param auth the presenting request's authentication context
     * @param storage the server's object store, or {@code null}
     * @param maxFetchBytes the server's inbound fetch cap
     * @param cache the object cache, or {@code null} for the process-wide one
     * @return the verified object bytes (shared with the object cache: do not modify)
     * @throws InvalidArgumentsRefError malformed claim, no storage, a ref that does not open
     *     for this caller, or sealed values that differ from the claim
     * @throws ArgumentsRefIntegrityError the fetched bytes' length or digest differ
     * @throws ArgumentsRefMissingError storage reports the object absent
     * @throws StorageUnavailableError a transient storage failure
     */
    public static byte[] resolve(byte[] ref, long size, String sha256, byte[] tokenKey, AuthContext auth,
                                 ObjectStore storage, long maxFetchBytes, ObjectCache cache) {
        if (size < 1) {
            throw new InvalidArgumentsRefError("argument location size must be an integer >= 1, got " + size);
        }
        if (sha256 == null || !SHA256_HEX.matcher(sha256).matches()) {
            throw new InvalidArgumentsRefError("argument location sha256 must be 64 lowercase hex characters");
        }
        if (storage == null) {
            throw new InvalidArgumentsRefError(
                    "this server has no object storage configured to resolve argument references");
        }
        ArgumentRef opened = open(tokenKey, auth, ref);
        if (opened.size() != size || !opened.sha256().equals(sha256)) {
            throw new InvalidArgumentsRefError("argument reference does not match the size and sha256 it was sent with");
        }
        if (size > maxFetchBytes) {
            throw new InvalidArgumentsRefError("argument object is " + size
                    + " bytes, over this server's max_fetch_bytes (" + maxFetchBytes + ")");
        }
        ObjectCache theCache = cache != null ? cache : ObjectCache.defaultCache();
        byte[] expected = HexFormat.of().parseHex(sha256);
        ObjectCache.Lookup lookup = theCache.getOrFetch(opened.objectId(), () -> {
            byte[] data = StateRefs.fetchObject(storage, opened.objectId(), maxFetchBytes, false);
            if (data.length != size) {
                throw new ArgumentsRefIntegrityError("argument object size mismatch: expected " + size
                        + ", got " + data.length);
            }
            if (!MessageDigest.isEqual(StateRefs.sha256(data), expected)) {
                throw new ArgumentsRefIntegrityError("argument object sha256 mismatch");
            }
            return data;
        });
        byte[] value = lookup.value();
        if (value.length != size) {
            // A cached value passed the checks above for this id; a length change means the id
            // was reused, which storage never does.
            throw new ArgumentsRefIntegrityError("argument object size mismatch: expected " + size
                    + ", got " + value.length);
        }
        LOG.debug("arguments_ref.resolve cache={} bytes={}", lookup.hit() ? "hit" : "miss", value.length);
        return value;
    }
}
