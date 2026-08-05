// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.external;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The parts of {@code max_externalized_response_bytes} enforcement the
 * cross-language conformance suite cannot see: it observes only that an
 * overshooting call fails, not that the refused bytes were never charged nor
 * that the refusal is remembered for the transport to read back.
 */
class ExternalResponseBudgetTest {

    @Test
    void reserve_is_a_noop_without_a_scope() {
        assertNull(ExternalResponseBudget.current());
        ExternalResponseBudget.reserve(Long.MAX_VALUE);   // must not throw
    }

    @Test
    void under_cap_payloads_accumulate() {
        try (ExternalResponseBudget budget = ExternalResponseBudget.open(100, "m")) {
            ExternalResponseBudget.reserve(40);
            ExternalResponseBudget.reserve(60);
            assertEquals(100, budget.usedBytes());
            assertFalse(budget.violated());
        }
    }

    @Test
    void overshoot_refuses_and_does_not_charge_the_refused_bytes() {
        try (ExternalResponseBudget budget = ExternalResponseBudget.open(100, "echo_large_string")) {
            ExternalResponseBudget.reserve(60);
            ExternalizedResponseCapExceededException e = assertThrows(
                    ExternalizedResponseCapExceededException.class,
                    () -> ExternalResponseBudget.reserve(50));
            // The conformance suite matches on this token; a client that read
            // VGI-Max-Externalized-Response-Bytes has to recognise the limit.
            assertTrue(e.getMessage().contains("max_externalized_response_bytes"), e.getMessage());
            assertTrue(e.getMessage().contains("110 > 100"), e.getMessage());
            assertTrue(e.getMessage().contains("echo_large_string"), e.getMessage());
            // Refused bytes are never uploaded, so they are never charged --
            // a later, smaller payload must still be allowed through.
            assertEquals(60, budget.usedBytes());
            ExternalResponseBudget.reserve(40);
            assertEquals(100, budget.usedBytes());
            // ...but the trip is remembered, so a transport can still fail the
            // response even if some catch-all swallowed the exception.
            assertTrue(budget.violated());
            assertNull(budget.violation().getCause());
        }
    }

    @Test
    void a_non_positive_cap_is_unbounded() {
        try (ExternalResponseBudget budget = ExternalResponseBudget.open(0, "m")) {
            ExternalResponseBudget.reserve(1L << 40);
            assertFalse(budget.violated());
            assertEquals(1L << 40, budget.usedBytes());
        }
    }

    @Test
    void close_uninstalls_the_scope() {
        try (ExternalResponseBudget budget = ExternalResponseBudget.open(10, "m")) {
            assertEquals(budget, ExternalResponseBudget.current());
        }
        assertNull(ExternalResponseBudget.current());
    }
}
