// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

/**
 * POSIX shared-memory side-channel for zero-copy batch transfer between co-located
 * processes. The {@code java.lang.foreign} implementation ({@code FfmShm}) ships in
 * the Java 22 multi-release overlay and loads only on JDK&nbsp;&ge;&nbsp;22; on Java 21
 * {@link farm.query.vgirpc.shm.ShmFactory} reports shm unavailable and callers fall
 * back to inline transfer. shm is an optimisation, never a requirement, and can be
 * forced off with {@code VGI_RPC_SHM_DISABLE=1}.
 */
package farm.query.vgirpc.shm;
