// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.transport;

/** Whether application request bytes may have reached the worker. */
public enum IrohDispatchCertainty { NOT_SENT, UNKNOWN, SENT }
