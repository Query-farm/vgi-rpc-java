// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0
package farm.query.vgirpc.identity;

/** A peer identity authority could not give a definitive answer. */
public final class PeerIdentityUnavailableException extends RuntimeException {
    private final int retryAfterSeconds;
    public PeerIdentityUnavailableException(String message) { this(message, 5); }
    public PeerIdentityUnavailableException(String message, int retryAfterSeconds) {
        super(message);
        this.retryAfterSeconds = retryAfterSeconds;
    }
    public int retryAfterSeconds() { return retryAfterSeconds; }
}
