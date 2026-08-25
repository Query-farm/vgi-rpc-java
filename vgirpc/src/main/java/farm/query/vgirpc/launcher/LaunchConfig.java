// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.launcher;

import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

/**
 * Inputs to {@link LauncherClient#launch}. Mirrors the Python reference's own
 * {@code LaunchConfig} dataclass field-for-field (defaults included) — see
 * {@code vgi_rpc.launcher.LaunchConfig} in {@code vgi-rpc-python}.
 *
 * @param workerArgv the worker command and arguments; must be non-empty
 * @param explicitSocketPath an explicit socket path, bypassing hash-based derivation
 *        (gets a sibling {@code .lock}, no {@code .meta}, invisible to the launcher's
 *        own GC/status tooling) — {@code null} to derive the path from the tuple hash
 * @param idleTimeoutSeconds worker self-shutdown after this many idle seconds,
 *        forwarded as {@code --idle-timeout}; {@code 0} means no timeout
 * @param connectTimeoutSeconds maximum time to wait for the per-hash {@code flock}
 * @param workerStartupTimeoutSeconds maximum time to wait for the worker to print
 *        {@code UNIX:<path>}
 * @param stateDir override for the default per-user state directory, or {@code null}
 * @param cwd the {@code cwd} the hash is computed against, or {@code null} to use the
 *        current process's working directory ({@code user.dir})
 */
public record LaunchConfig(
        List<String> workerArgv,
        String explicitSocketPath,
        double idleTimeoutSeconds,
        double connectTimeoutSeconds,
        double workerStartupTimeoutSeconds,
        Path stateDir,
        String cwd) {

    /** Matches the Python reference's {@code idle_timeout: float = 300.0}. */
    public static final double DEFAULT_IDLE_TIMEOUT_SECONDS = 300.0;
    /** Matches the Python reference's {@code connect_timeout: float = 30.0}. */
    public static final double DEFAULT_CONNECT_TIMEOUT_SECONDS = 30.0;
    /** Matches the Python reference's {@code worker_startup_timeout: float = 60.0}. */
    public static final double DEFAULT_WORKER_STARTUP_TIMEOUT_SECONDS = 60.0;

    public LaunchConfig {
        Objects.requireNonNull(workerArgv, "workerArgv");
        if (workerArgv.isEmpty()) throw new IllegalArgumentException("workerArgv must be non-empty");
        workerArgv = List.copyOf(workerArgv);
    }

    /** A config with every default from the Python reference, for {@code workerArgv}. */
    public static LaunchConfig of(List<String> workerArgv) {
        return new LaunchConfig(workerArgv, null, DEFAULT_IDLE_TIMEOUT_SECONDS,
                DEFAULT_CONNECT_TIMEOUT_SECONDS, DEFAULT_WORKER_STARTUP_TIMEOUT_SECONDS, null, null);
    }
}
