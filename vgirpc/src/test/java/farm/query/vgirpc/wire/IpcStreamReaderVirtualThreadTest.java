// Copyright 2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0
package farm.query.vgirpc.wire;

import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

class IpcStreamReaderVirtualThreadTest {
    @Test
    void idleReadersDoNotStarveOtherVirtualThreads() throws Exception {
        // Default scheduler parallelism equals the processor count. More idle
        // Arrow readers than carriers must still leave capacity for a new RPC.
        int count = Runtime.getRuntime().availableProcessors() + 2;
        CountDownLatch entered = new CountDownLatch(count);
        List<Socket> sockets = new ArrayList<>();
        List<Thread> readers = new ArrayList<>();
        try (RootAllocator allocator = new RootAllocator();
             ServerSocket listener = new ServerSocket(0)) {
            try {
                for (int i = 0; i < count; i++) {
                    sockets.add(new Socket("127.0.0.1", listener.getLocalPort()));
                    Socket server = listener.accept();
                    sockets.add(server);
                    readers.add(Thread.ofVirtual().start(() -> {
                        try (IpcStreamReader reader = new IpcStreamReader(server.getInputStream(), allocator)) {
                            entered.countDown();
                            reader.readNextBatch();
                        } catch (IOException expectedOnShutdown) {
                            // The peer stays idle until the test closes it.
                        }
                    }));
                }
                assertTrue(entered.await(5, TimeUnit.SECONDS),
                        "idle Arrow readers pinned every virtual-thread carrier");
                CountDownLatch heartbeat = new CountDownLatch(1);
                Thread.ofVirtual().start(heartbeat::countDown);
                assertTrue(heartbeat.await(5, TimeUnit.SECONDS), "new RPC work could not be scheduled");
            } finally {
                for (Socket socket : sockets) socket.close();
                for (Thread reader : readers) reader.join(5000);
            }
        }
    }
}
