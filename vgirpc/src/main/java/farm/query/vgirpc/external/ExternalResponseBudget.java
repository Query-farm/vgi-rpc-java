// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.external;

/**
 * Per-request ceiling on the bytes one response may push to external storage —
 * the enforcement half of {@code max_externalized_response_bytes}.
 *
 * <p>This is the cap with no escape valve. {@code max_response_bytes} governs
 * what lands on the wire and is <em>soft</em> for producer streams, because a
 * continuation token carries the overshoot to the next turn. Bytes already
 * uploaded to external storage cannot be un-uploaded, so this cap is hard on
 * every method type, and the check has to happen <em>before</em> the upload:
 * refusing after the fact still spent the egress.
 *
 * <p>{@link #reserve} is therefore called from {@link Externalizer}, at the one
 * point every externalised payload passes through and immediately before the
 * bytes are handed to {@code ExternalStorage.upload} — the same argument that
 * puts {@code AccessLogScope.countExternalized} there. A new upload path cannot
 * drift from the total, and cannot be added without being counted.
 *
 * <p>The scope also remembers that it tripped ({@link #violated}), so a
 * transport can fail the response even if the exception were swallowed on the
 * way out. The upload paths deliberately fall back to inline delivery when an
 * upload <em>fails</em>; that catch-all is one edit away from also absorbing a
 * cap refusal, and the flag is what keeps the contract observable if it does.
 *
 * <p>Bound to the dispatching thread. Transports that install no scope — pipe,
 * unix socket, raw TCP — are unbounded, which is what they were before: the cap
 * is an HTTP-server configuration knob.
 */
public final class ExternalResponseBudget implements AutoCloseable {

    private static final ThreadLocal<ExternalResponseBudget> CURRENT = new ThreadLocal<>();

    /** Configured ceiling in bytes; {@code <= 0} means unbounded. */
    private final long capBytes;
    private final String methodName;
    private long usedBytes;
    private ExternalizedResponseCapExceededException violation;

    private ExternalResponseBudget(long capBytes, String methodName) {
        this.capBytes = capBytes;
        this.methodName = methodName;
    }

    /**
     * Open a budget on the current thread, replacing any budget already installed.
     *
     * @param capBytes the configured {@code max_externalized_response_bytes};
     *     {@code 0} or negative installs an unbounded scope, which still tracks
     *     usage but never refuses
     * @param methodName the RPC method being dispatched, named in the refusal message
     * @return the scope; close it (ideally via try-with-resources) to uninstall it
     */
    public static ExternalResponseBudget open(long capBytes, String methodName) {
        ExternalResponseBudget budget = new ExternalResponseBudget(capBytes, methodName);
        CURRENT.set(budget);
        return budget;
    }

    /**
     * The budget installed on the current thread.
     *
     * @return the current budget, or {@code null} when the transport installs none
     */
    public static ExternalResponseBudget current() { return CURRENT.get(); }

    /**
     * Charge {@code bytes} against the current budget <em>before</em> they are
     * uploaded. No-op when no scope is installed or the scope is unbounded.
     *
     * @param bytes size of the payload about to be handed to {@code ExternalStorage.upload}
     * @throws ExternalizedResponseCapExceededException when the upload would take
     *     this response past {@code max_externalized_response_bytes}; the bytes are
     *     <em>not</em> charged in that case, because they are never sent
     */
    public static void reserve(long bytes) {
        ExternalResponseBudget budget = CURRENT.get();
        if (budget == null) return;
        if (budget.capBytes > 0 && budget.usedBytes + bytes > budget.capBytes) {
            ExternalizedResponseCapExceededException e = new ExternalizedResponseCapExceededException(
                    budget.usedBytes + bytes, budget.capBytes, budget.methodName);
            // Remembered, not just thrown: see the class docs.
            if (budget.violation == null) budget.violation = e;
            throw e;
        }
        budget.usedBytes += bytes;
    }

    /** @return bytes charged to this budget so far — uploads that actually happened */
    public long usedBytes() { return usedBytes; }

    /** @return {@code true} when an upload was refused during this request */
    public boolean violated() { return violation != null; }

    /** @return the refusal that tripped this budget, or {@code null} if it never tripped */
    public ExternalizedResponseCapExceededException violation() { return violation; }

    /** Uninstall the scope from the current thread. */
    @Override
    public void close() { CURRENT.remove(); }
}
