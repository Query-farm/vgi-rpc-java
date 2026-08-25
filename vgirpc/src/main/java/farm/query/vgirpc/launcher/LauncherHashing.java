// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.launcher;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Computes the launcher's per-tuple hash: a 16-hex-char SHA-256 prefix of the
 * canonical-JSON {@code (cmd, cwd, VGI_RPC_*-env)} tuple. Byte-identical to Python's
 * {@code vgi_rpc.launcher.compute_hash} — see {@code docs/launcher-protocol.md}'s
 * <i>Hashing</i> section and {@code LauncherHashingParityTest}'s golden vectors
 * (generated from the Python reference, not hand-written).
 */
public final class LauncherHashing {

    private LauncherHashing() {}

    private static final int HASH_HEX_LEN = 16;
    private static final String VGI_RPC_ENV_PREFIX = "VGI_RPC_";

    /**
     * @param argv the worker command and arguments
     * @param cwd the working directory the tuple is scoped to
     * @param env the environment to filter to {@code VGI_RPC_*} keys (typically {@code System.getenv()});
     *        a {@code Map} rather than a multimap since Java's environment view can't carry duplicate
     *        keys in the first place — unlike a raw {@code environ} array, there is no "last wins" case
     *        to resolve here
     * @return the 16-hex-char hash identifying this tuple
     */
    public static String computeHash(List<String> argv, String cwd, Map<String, String> env) {
        SortedMap<String, String> vgiRpcEnv = new TreeMap<>();
        for (Map.Entry<String, String> e : env.entrySet()) {
            if (e.getKey().startsWith(VGI_RPC_ENV_PREFIX)) {
                vgiRpcEnv.put(e.getKey(), e.getValue());
            }
        }
        String canonical = CanonicalJson.encodeHashPayload(argv, cwd, vgiRpcEnv);
        byte[] digest;
        try {
            digest = MessageDigest.getInstance("SHA-256").digest(canonical.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            // Every JDK implementation is required to provide SHA-256 (JCA standard algorithm).
            throw new AssertionError("SHA-256 MessageDigest unavailable", e);
        }
        StringBuilder hex = new StringBuilder(HASH_HEX_LEN);
        for (int i = 0; i < HASH_HEX_LEN / 2; i++) {
            hex.append(Character.forDigit((digest[i] >> 4) & 0xF, 16));
            hex.append(Character.forDigit(digest[i] & 0xF, 16));
        }
        return hex.toString();
    }

    /** Convenience overload hashing against the current process's own {@code cwd} and environment. */
    public static String computeHash(List<String> argv) {
        return computeHash(argv, System.getProperty("user.dir"), System.getenv());
    }
}
