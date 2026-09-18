// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.launcher;

import farm.query.vgirpc.transport.UnixSocketTransport;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.InterruptedIOException;
import java.net.ConnectException;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Ensures a warm worker is running for a given {@link LaunchConfig} and returns its
 * AF_UNIX socket path — spawning one under a per-tuple {@code flock} if no live
 * worker already answers, reusing an existing one otherwise. Mirrors the Python
 * reference's {@code vgi_rpc.launcher.launch} step for step; see {@code
 * docs/launcher-protocol.md}'s <i>Lifecycle in one paragraph</i>.
 *
 * <p>Returns a plain socket path — this class does not open a connection itself.
 * Callers connect exactly as they would to any other {@code unix://} location, ideally
 * with {@link UnixSocketTransport#connect(Path, java.time.Duration)}, which waits out a
 * busy worker's full accept queue instead of failing (see this package's own javadoc).
 */
public final class LauncherClient {

    private LauncherClient() {}

    private static final long DISCOVERY_NOISE_CAP_BYTES = 1_048_576; // 1 MiB, matching the protocol doc

    /**
     * Pauses between re-probes of a refused socket, in milliseconds — see {@link #probe}. A socket
     * left by a dead worker costs this once (350 ms), before it is replaced. The same values as
     * the Python ({@code _PROBE_REFUSED_BACKOFF_S}) and C++ ({@code ProbeAlive}) launchers.
     */
    private static final long[] PROBE_REFUSED_BACKOFF_MS = {50, 100, 200};

    /**
     * Ensure a worker is running and return its absolute socket path.
     *
     * @throws IOException on any failure to bring up (or reuse) a worker
     * @throws UnsupportedOperationException on the Java 21 baseline — see {@link PosixLauncherSupport}
     */
    public static String launch(LaunchConfig config) throws IOException {
        Path stateDir = config.stateDir() != null ? config.stateDir() : LauncherPaths.defaultStateDir();
        Files.createDirectories(stateDir);

        Path sockPath;
        Path lockPath;
        Path metaPath; // null for an explicit socket path — invisible to GC/status tooling
        String resolvedCwd = config.cwd() != null ? config.cwd() : System.getProperty("user.dir");

        if (config.explicitSocketPath() != null) {
            sockPath = Path.of(config.explicitSocketPath()).toAbsolutePath();
            lockPath = Path.of(sockPath + ".lock");
            metaPath = null;
        } else {
            String hashId = LauncherHashing.computeHash(config.workerArgv(), resolvedCwd, System.getenv());
            lockPath = LauncherPaths.lockPath(stateDir, hashId);
            sockPath = LauncherPaths.sockPath(stateDir, hashId);
            metaPath = LauncherPaths.metaPath(stateDir, hashId);
        }

        try (FlockHandle lock = PosixLauncherSupport.tryLock(lockPath, config.connectTimeoutSeconds())) {
            requireSocketOrAbsent(sockPath);
            if (probe(sockPath)) {
                return sockPath.toString();
            }
            unlinkStaleSocket(sockPath);
            if (metaPath != null) {
                writeMetaBestEffort(metaPath, config.workerArgv(), resolvedCwd, sockPath.toString());
            }
            spawnAndAwaitDiscovery(config, sockPath);
            return sockPath.toString();
        }
        // Opportunistic stale-entry GC is intentionally not implemented here — see
        // this package's own javadoc ("Out of scope for v1").
    }

    private static boolean isUnixSocket(Path path) throws IOException {
        Object rawMode = Files.getAttribute(path, "unix:mode", LinkOption.NOFOLLOW_LINKS);
        return rawMode instanceof Integer mode && (mode & 0170000) == 0140000;
    }

    /** Refuses (without touching) an occupied launcher path unless it's a real socket inode — a
     *  regular file or symlink there may hold user data or point outside the state dir. */
    private static void requireSocketOrAbsent(Path path) throws IOException {
        if (!Files.exists(path, LinkOption.NOFOLLOW_LINKS)) return;
        if (!isUnixSocket(path)) {
            throw new IOException("refusing to replace pre-existing non-socket path: " + path);
        }
    }

    private static void unlinkStaleSocket(Path path) throws IOException {
        if (!Files.exists(path, LinkOption.NOFOLLOW_LINKS)) return;
        if (!isUnixSocket(path)) {
            throw new IOException("refusing to replace pre-existing non-socket path: " + path);
        }
        Files.deleteIfExists(path);
    }

    /**
     * True iff a worker is listening at {@code path}. Connects then immediately closes — a
     * liveness probe only, never the connection the caller actually uses.
     *
     * <p><strong>A listener whose accept queue is full is alive, only busy</strong> — and a false
     * "dead" here is destructive, because {@link #launch} then unlinks the socket out from under
     * the live worker and spawns a duplicate, orphaning the original and racing clients onto a
     * vanished path (a 32-process Python test run produced 64 workers for 2 commands). So:
     *
     * <ul>
     *   <li>The connect is <em>non-blocking</em>. A blocking one behaves differently by thread
     *   (measured on Linux, JDK 21 and 25): on a platform thread it waits for a queue slot with no
     *   bound — while this process holds the launcher lock — and on a virtual thread the JDK makes
     *   the socket non-blocking and it fails at once. One behaviour, on every thread.</li>
     *   <li>Connected (or still connecting) is alive.</li>
     *   <li>Linux reports a full queue as {@code EAGAIN}, which is alive — see
     *   {@link UnixSocketTransport#isAcceptQueueFull} for how that is told apart without reading a
     *   localised error message.</li>
     *   <li>macOS reports a full queue as {@code ECONNREFUSED}, the same as an unbound socket, so a
     *   refusal is re-probed after 50, 100 and 200 ms before it is believed.</li>
     *   <li>Anything else (absent, not reachable) is dead.</li>
     * </ul>
     *
     * <p>This asks "is anything listening", not "is it responsive": a connect lands in the queue
     * of a worker that never accepts, so a successful one never proved that either. Mirrors
     * {@code vgi_rpc.launcher._probe} and the vgi extension's {@code ProbeAlive}.
     *
     * @throws InterruptedIOException if interrupted between re-probes — never read as "dead",
     *     which would unlink a socket that may be live
     */
    static boolean probe(Path path) throws InterruptedIOException {
        UnixDomainSocketAddress address = UnixDomainSocketAddress.of(path);
        for (int attempt = 0; ; attempt++) {
            if (attempt > 0) {
                try {
                    Thread.sleep(PROBE_REFUSED_BACKOFF_MS[attempt - 1]);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new InterruptedIOException("interrupted probing " + path);
                }
            }
            try (SocketChannel channel = SocketChannel.open(StandardProtocolFamily.UNIX)) {
                channel.configureBlocking(false);
                channel.connect(address); // true: connected; false: in progress — a listener either way
                return true;
            } catch (ConnectException refused) {
                if (attempt == PROBE_REFUSED_BACKOFF_MS.length) return false;
            } catch (IOException e) {
                return UnixSocketTransport.isAcceptQueueFull(e, path);
            }
        }
    }

    private static void writeMetaBestEffort(Path metaPath, List<String> argv, String cwd, String sockPath) {
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("{\n  \"cmd\": [");
            for (int i = 0; i < argv.size(); i++) {
                if (i > 0) sb.append(", ");
                CanonicalJson.appendString(sb, argv.get(i));
            }
            sb.append("],\n  \"cwd\": ");
            CanonicalJson.appendString(sb, cwd);
            sb.append(",\n  \"socket\": ");
            CanonicalJson.appendString(sb, sockPath);
            sb.append(",\n  \"started_at\": ").append(System.currentTimeMillis() / 1000.0);
            sb.append(",\n  \"launcher_pid\": ").append(ProcessHandle.current().pid());
            sb.append("\n}");
            Files.writeString(metaPath, sb.toString(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            // Best-effort, matching the Python reference's own catch-and-log-at-debug.
        }
    }

    /** Plain-decimal seconds, matching the wire contract's C++ conversion ({@code "%.3f"}, trailing
     *  zeros stripped) — never scientific notation, so every parser on every platform agrees. */
    private static String formatIdleTimeoutSeconds(double seconds) {
        String formatted = String.format(Locale.ROOT, "%.3f", seconds);
        if (formatted.indexOf('.') >= 0) {
            int end = formatted.length();
            while (end > 0 && formatted.charAt(end - 1) == '0') end--;
            if (end > 0 && formatted.charAt(end - 1) == '.') end--;
            formatted = formatted.substring(0, end);
        }
        return formatted.isEmpty() ? "0" : formatted;
    }

    private static void spawnAndAwaitDiscovery(LaunchConfig config, Path sockPath) throws IOException {
        java.util.ArrayList<String> fullArgv = new java.util.ArrayList<>(config.workerArgv());
        fullArgv.add("--unix");
        fullArgv.add(sockPath.toString());
        fullArgv.add("--idle-timeout");
        fullArgv.add(formatIdleTimeoutSeconds(config.idleTimeoutSeconds()));

        // Redirect.DISCARD is output-only (there's no INPUT analogue) — leave stdin as the
        // default PIPE and close our write end immediately so the child sees EOF right away,
        // the same "no stdin" contract DISCARD gives for output.
        ProcessBuilder pb = new ProcessBuilder(fullArgv)
                .redirectError(ProcessBuilder.Redirect.DISCARD);
        Process worker = pb.start();
        try { worker.getOutputStream().close(); } catch (IOException ignore) {}

        String expectedLine = "UNIX:" + sockPath;
        BlockingQueue<Object> lines = new ArrayBlockingQueue<>(64); // String, or a Throwable on failure
        Thread reader = new Thread(() -> {
            long bytesRead = 0;
            try (BufferedReader in = new BufferedReader(
                    new InputStreamReader(worker.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = in.readLine()) != null) {
                    bytesRead += line.length() + 1;
                    if (bytesRead > DISCOVERY_NOISE_CAP_BYTES) {
                        lines.offer(new IOException("exceeded 1 MiB of stdout without a UNIX: line"));
                        return;
                    }
                    lines.offer(line);
                    if (line.startsWith("UNIX:")) {
                        while (in.readLine() != null) { /* drain — see the protocol doc's stdout-noise rule */ }
                        return;
                    }
                }
                lines.offer(new IOException("worker's stdout closed before a UNIX: line appeared"));
            } catch (IOException e) {
                lines.offer(e);
            }
        }, "vgi-launcher-discovery");
        reader.setDaemon(true);
        reader.start();

        long deadline = System.currentTimeMillis()
                + (long) (config.workerStartupTimeoutSeconds() * 1000);
        while (System.currentTimeMillis() < deadline) {
            if (!worker.isAlive()) {
                throw new IOException("worker exited before readiness (exit code " + worker.exitValue() + ")");
            }
            Object next;
            try {
                next = lines.poll(200, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted waiting for the worker's UNIX: line", e);
            }
            if (next instanceof IOException e) throw e;
            if (next instanceof String line && line.startsWith("UNIX:")) {
                if (!line.equals(expectedLine)) {
                    worker.destroy();
                    throw new IOException("worker bound to unexpected path: " + line
                            + " (expected " + expectedLine + ")");
                }
                return;
            }
            // Non-matching prefix line (third-party stdout noise) — keep reading.
        }
        worker.destroy();
        throw new IOException("worker did not emit UNIX:<path> within "
                + config.workerStartupTimeoutSeconds() + "s");
    }
}
