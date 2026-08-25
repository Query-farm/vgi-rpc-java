// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.launcher;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.VarHandle;
import java.nio.file.Path;
import java.util.OptionalLong;

/**
 * Java&nbsp;22+ overlay of {@link PosixLauncherSupport} (packaged under {@code
 * META-INF/versions/22}), binding {@code open}/{@code flock}/{@code close}/{@code
 * geteuid} via the {@code java.lang.foreign} FFM API (GA in JDK&nbsp;22) — same
 * {@code Linker}/{@code SymbolLookup}/{@code captureCallState("errno")} pattern as
 * {@code farm.query.vgirpc.shm.FfmShm}.
 *
 * <p>The lock file's native fd deliberately comes from a raw {@code open(2)}
 * downcall, never {@code java.nio.channels.FileChannel} — there is no stable public
 * API to extract a native fd from a {@code FileChannel} before JDK 22, and even if
 * there were, {@code FileChannel.lock()} itself binds {@code fcntl(F_SETLK)}, the
 * syscall this class exists specifically to avoid (see the package's own javadoc).
 * {@code flock(2)} has no timeout parameter, so a bounded wait is a poll loop
 * retrying {@code LOCK_EX|LOCK_NB} rather than a blocking call with no way out —
 * acceptable because launcher lock contention is expected to be rare and brief
 * (the whole point of the launcher is a low-contention happy path).
 */
public final class PosixLauncherSupport {

    private PosixLauncherSupport() {}

    private static final boolean IS_MAC =
            System.getProperty("os.name", "").toLowerCase().contains("mac");

    // open(2) flags diverge between Linux and Darwin; O_RDWR is identical.
    private static final int O_RDWR = 0x0002;
    private static final int O_CREAT = IS_MAC ? 0x0200 : 0x40;

    // flock(2) operations — identical on Linux and Darwin.
    private static final int LOCK_EX = 2;
    private static final int LOCK_UN = 8;
    private static final int LOCK_NB = 4;

    // Poll interval while waiting for a contended lock (flock(2) has no timeout param).
    private static final long POLL_MILLIS = 20;

    private static final Linker LINKER = Linker.nativeLinker();
    private static final SymbolLookup LIBC = LINKER.defaultLookup();
    private static final MemoryLayout CAPTURE = Linker.Option.captureStateLayout();
    private static final VarHandle ERRNO =
            CAPTURE.varHandle(MemoryLayout.PathElement.groupElement("errno"));

    // int open(const char *path, int oflag, mode_t mode)  [mode is variadic when O_CREAT is set]
    private static final MethodHandle OPEN = LINKER.downcallHandle(
            LIBC.find("open").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_INT,
                    ValueLayout.ADDRESS, ValueLayout.JAVA_INT, ValueLayout.JAVA_INT),
            Linker.Option.captureCallState("errno"),
            Linker.Option.firstVariadicArg(2));
    // int flock(int fd, int operation)
    private static final MethodHandle FLOCK = LINKER.downcallHandle(
            LIBC.find("flock").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_INT, ValueLayout.JAVA_INT),
            Linker.Option.captureCallState("errno"));
    // int close(int fd)
    private static final MethodHandle CLOSE = LINKER.downcallHandle(
            LIBC.find("close").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_INT));
    // uid_t geteuid(void) -- uid_t is a 32-bit unsigned int on both Linux and Darwin
    private static final MethodHandle GETEUID = LINKER.downcallHandle(
            LIBC.find("geteuid").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_INT));

    public static boolean available() {
        return true;
    }

    public static OptionalLong euid() {
        try {
            return OptionalLong.of(Integer.toUnsignedLong((int) GETEUID.invoke()));
        } catch (Throwable t) {
            return OptionalLong.empty();
        }
    }

    /**
     * Acquire an exclusive {@code flock(2)} lock on {@code lockFile} (created if absent, mode 0600),
     * blocking (via a poll loop) up to {@code timeoutSeconds}.
     */
    public static FlockHandle tryLock(Path lockFile, double timeoutSeconds) throws IOException {
        Arena arena = Arena.ofShared();
        boolean ok = false;
        try {
            MemorySegment cPath = arena.allocateFrom(lockFile.toString());
            MemorySegment cap = arena.allocate(CAPTURE);
            int fd = (int) OPEN.invoke(cap, cPath, O_RDWR | O_CREAT, 0600);
            if (fd < 0) throw new IOException("open(" + lockFile + ") errno=" + errno(cap));
            try {
                long deadlineNanos = System.nanoTime() + (long) (timeoutSeconds * 1_000_000_000L);
                while (true) {
                    int rc = (int) FLOCK.invoke(cap, fd, LOCK_EX | LOCK_NB);
                    if (rc == 0) break;
                    if (System.nanoTime() >= deadlineNanos) {
                        throw new IOException("timed out acquiring flock on " + lockFile
                                + " within " + timeoutSeconds + "s");
                    }
                    try {
                        Thread.sleep(POLL_MILLIS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException("interrupted acquiring flock on " + lockFile, e);
                    }
                }
                ok = true;
                int lockedFd = fd;
                MemorySegment capForRelease = cap;
                Arena heldArena = arena;
                return () -> {
                    try { FLOCK.invoke(capForRelease, lockedFd, LOCK_UN); } catch (Throwable ignore) {}
                    try { CLOSE.invoke(lockedFd); } catch (Throwable ignore) {}
                    heldArena.close();
                };
            } finally {
                if (!ok) {
                    try { CLOSE.invoke(fd); } catch (Throwable ignore) {}
                }
            }
        } catch (IOException e) {
            arena.close();
            throw e;
        } catch (Throwable t) {
            arena.close();
            throw new IOException("flock(" + lockFile + ") failed: " + t, t);
        }
    }

    private static int errno(MemorySegment cap) {
        return (int) ERRNO.get(cap, 0L);
    }
}
