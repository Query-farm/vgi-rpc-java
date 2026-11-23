// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

/**
 * Stream state by reference (protocol 3.4, {@code STATE_REF_V1.md}): {@link
 * farm.query.vgirpc.stateref.StateRef}, the per-process {@link
 * farm.query.vgirpc.stateref.ObjectCache}, argument references ({@link
 * farm.query.vgirpc.stateref.ArgumentRefs}), the turn budget and cursor rule ({@link
 * farm.query.vgirpc.stateref.StateRefRuntime}) and the standard token codec ({@link
 * farm.query.vgirpc.stateref.TokenCodec}).
 *
 * <p>Deployments with more than one replica <strong>must</strong> set a shared token key:
 * every replica that can open a cursor can then open a stored tag, because both are sealed
 * under the same key. Operators must also set a bucket lifecycle rule of at least one day on
 * the storage's {@code rpc} prefix: stored objects carry no TTL and the server never deletes
 * them.</p>
 */
package farm.query.vgirpc.stateref;
