// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

/**
 * Client for the {@code launch:} shared-warm-worker transport: the first caller for a
 * given {@code (argv, cwd, VGI_RPC_*-env)} tuple spawns an AF_UNIX-serving worker
 * process and every later caller for the same tuple — in this JVM or any other
 * process, any language — reuses it, coordinated via a per-tuple {@code flock(2)}
 * lock and a deterministic socket path in a per-user state directory.
 *
 * <p>Conforms to {@code docs/launcher-protocol.md} in the {@code vgi} repo, the
 * single source of truth this package is tested against (see {@code
 * LauncherHashingParityTest}'s golden vectors, generated from the Python reference
 * implementation, {@code vgi_rpc.launcher} in {@code vgi-rpc-python}).
 *
 * <p>{@link farm.query.vgirpc.launcher.LauncherClient#launch} resolves (spawning if
 * needed) and returns the worker's socket path — callers then connect to it exactly
 * like any other {@code unix://} location (e.g. via {@code UnixDomainSocketAddress}
 * + {@code SocketChannel}, as {@code UnixSocketTransport}'s own client side does).
 * This package does not implement a byte-stream transport itself.
 *
 * <p>The {@code flock(2)} lock (and the process's effective UID, needed for
 * state-directory naming parity with the Python/C++ launchers) require the
 * {@code java.lang.foreign} Foreign Function &amp; Memory API (GA in JDK&nbsp;22) —
 * {@code java.nio.channels.FileChannel.lock()} uses {@code fcntl(F_SETLK)} on POSIX,
 * which does <b>not</b> interlock with {@code flock(2)} (see the protocol doc's Lock
 * semantics section). {@link farm.query.vgirpc.launcher.PosixLauncherSupport} is
 * therefore Java&nbsp;21-baseline-unsupported ({@code launch:} throws {@link
 * java.lang.UnsupportedOperationException}) with a real implementation in the
 * Java&nbsp;22 multi-release overlay — the same split {@code
 * farm.query.vgirpc.shm.ShmFactory}/{@code FfmShm} use.
 *
 * <p>Out of scope for v1 (see the package's own design notes, not repeated in every
 * class): Windows named pipes (the protocol doc itself marks Windows unsupported),
 * the launcher's opportunistic stale-entry GC and {@code --status}/{@code --gc}
 * introspection (correctness-neutral niceties, not required for a client to work
 * correctly), and an in-process resolved-path cache (a pool already amortizes
 * repeat-connect cost within one catalog's lifetime; only cross-catalog reuse would
 * benefit, and the flock+probe path already provides that at acceptable cost).
 */
package farm.query.vgirpc.launcher;
