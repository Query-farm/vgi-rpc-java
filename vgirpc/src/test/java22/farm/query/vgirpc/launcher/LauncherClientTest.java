// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.launcher;

import farm.query.vgirpc.RpcConnection;
import farm.query.vgirpc.transport.UnixSocketTransport;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end {@link LauncherClient} tests against a real launcher-protocol-compliant
 * worker process — {@link LauncherFixtureWorkerMain}, spawned via {@code java -cp
 * <this test's own classpath> ...}, not a fake/mocked spawn. Covers the properties
 * {@code docs/launcher-protocol.md} actually promises: spawn-once, reuse-many
 * (including under concurrent first-callers, via the real {@code flock}), and
 * idle-timeout self-shutdown followed by clean re-spawn.
 */
@DisabledOnOs(OS.WINDOWS)
final class LauncherClientTest {

    private static final String JAVA_BIN =
            java.nio.file.Path.of(System.getProperty("java.home"), "bin", "java").toString();

    /** A fresh unique directory per test, used only as {@code cwd} to give each test its own tuple
     *  hash (the hash domain includes cwd) — isolates tests from each other and from any real
     *  launcher a developer might already have running for this same fixture. */
    private static String freshCwd() throws Exception {
        return Files.createTempDirectory("vgi-launcher-test-cwd-").toString();
    }

    private static LaunchConfig configFor(String cwd, double idleTimeoutSeconds) {
        // Arrow's memory access needs --add-opens at JVM startup; the parent test JVM gets this
        // from :vgirpc:java22Test's own jvmArgs, but a freshly-spawned `java` subprocess starts
        // clean and needs it passed explicitly — it is not inherited from the parent process.
        List<String> argv = List.of(JAVA_BIN, "--add-opens=java.base/java.nio=ALL-UNNAMED",
                "-cp", System.getProperty("java.class.path"),
                LauncherFixtureWorkerMain.class.getName());
        return new LaunchConfig(argv, null, idleTimeoutSeconds,
                LaunchConfig.DEFAULT_CONNECT_TIMEOUT_SECONDS,
                LaunchConfig.DEFAULT_WORKER_STARTUP_TIMEOUT_SECONDS, null, cwd);
    }

    @Test
    @Timeout(60)
    void launchedWorkerIsGenuinelyConnectable() throws Exception {
        LaunchConfig config = configFor(freshCwd(), 300);
        String socketPath = LauncherClient.launch(config);
        assertEcho(socketPath, "hello from the real launcher");
    }

    @Test
    @Timeout(60)
    void secondLaunchReusesTheSameWorkerNoNewProcess() throws Exception {
        LaunchConfig config = configFor(freshCwd(), 300);
        String first = LauncherClient.launch(config);
        long descendantsAfterFirst = ProcessHandle.current().descendants().count();

        String second = LauncherClient.launch(config);
        long descendantsAfterSecond = ProcessHandle.current().descendants().count();

        assertEquals(first, second, "second launch() must resolve to the same worker");
        assertEquals(descendantsAfterFirst, descendantsAfterSecond,
                "reusing an existing worker must not spawn a new process");
        assertEcho(second, "still alive after reuse");
    }

    @Test
    @Timeout(60)
    void concurrentFirstCallersShareExactlyOneWorker() throws Exception {
        LaunchConfig config = configFor(freshCwd(), 300);
        long descendantsBefore = ProcessHandle.current().descendants().count();

        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            List<Future<String>> futures = IntStream.range(0, 8)
                    .mapToObj(i -> pool.<String>submit(() -> LauncherClient.launch(config)))
                    .collect(Collectors.toList());
            List<String> results = new ArrayList<>();
            for (Future<String> f : futures) results.add(f.get(30, TimeUnit.SECONDS));

            assertEquals(1, results.stream().distinct().count(),
                    "every concurrent first-caller must resolve to the SAME socket path, got: " + results);
            long descendantsAfter = ProcessHandle.current().descendants().count();
            assertEquals(descendantsBefore + 1, descendantsAfter,
                    "8 concurrent first-callers for the same tuple must spawn exactly ONE worker");
            assertEcho(results.get(0), "shared by 8 concurrent callers");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    @Timeout(60)
    void idleWorkerSelfShutsDownAndALaterLaunchSpawnsAFreshOne() throws Exception {
        // Short idle timeout: no client ever connects to the launched worker (the probe
        // connect-then-immediately-close doesn't count as "an active client"), so it should
        // self-shutdown well within this test's patience.
        LaunchConfig config = configFor(freshCwd(), 2.0);
        String socketPath = LauncherClient.launch(config);
        assertEcho(socketPath, "before idle shutdown");

        // No probing at all while waiting: a probe is itself a real accepted connection, and the
        // watchdog resets its idle clock to "now" whenever the active-client count returns to
        // zero (see UnixSocketTransport.serveForever) — checking on any cadence anywhere near the
        // timeout itself would perpetually feed the very idle timer this test is waiting to
        // elapse (confirmed: a poll loop at the timeout's own cadence never converged). Sleep
        // once, well past the budget with no interim connections, then probe exactly once.
        Thread.sleep((long) (config.idleTimeoutSeconds() * 1000) + 5_000);
        assertTrue(!probeAlive(socketPath),
                "expected the idle-timed-out worker to stop accepting connections");

        // A later launch() must detect the stale socket file (worker exited but the inode may
        // still exist briefly) and spawn a fresh worker rather than hanging or erroring.
        String secondSocketPath = LauncherClient.launch(config);
        assertEcho(secondSocketPath, "after respawn");
    }

    private static boolean probeAlive(String socketPath) {
        try (SocketChannel channel = SocketChannel.open(StandardProtocolFamily.UNIX)) {
            channel.connect(UnixDomainSocketAddress.of(socketPath));
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static void assertEcho(String socketPath, String value) throws Exception {
        try (SocketChannel channel = SocketChannel.open(StandardProtocolFamily.UNIX)) {
            channel.connect(UnixDomainSocketAddress.of(socketPath));
            try (UnixSocketTransport transport = new UnixSocketTransport(channel)) {
                LauncherFixtureWorkerMain.Echo service =
                        new RpcConnection(transport).proxy(LauncherFixtureWorkerMain.Echo.class);
                assertEquals(value, service.echo(value));
            }
        }
    }
}
