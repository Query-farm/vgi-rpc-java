// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

/**
 * Low-level Arrow IPC wire plumbing: stream readers/writers, the {@code vgi_rpc.*}
 * metadata key constants, the shared {@link org.apache.arrow.memory.BufferAllocator}
 * root, and higher-level helpers ({@link farm.query.vgirpc.wire.Wire}) for request
 * metadata, version validation, error streams, and zero-row control batches.
 */
package farm.query.vgirpc.wire;
