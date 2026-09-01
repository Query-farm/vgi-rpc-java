// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.transport;

import farm.query.vgirpc.identity.PeerAuthenticationPolicy;
import farm.query.vgirpc.identity.PeerIdentityProvider;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;

/** Optional connection-snapshot identity configuration for the raw TCP listener. */
public final class TcpServerOptions {
    private final List<PeerIdentityProvider> peerIdentityProviders;
    private final PeerAuthenticationPolicy peerAuthenticationPolicy;
    private final String peerServiceName;
    private final Duration identityResolutionTimeout;
    private final int peerProviderConcurrency;

    private TcpServerOptions(Builder builder) {
        peerIdentityProviders = List.copyOf(builder.peerIdentityProviders);
        peerAuthenticationPolicy = builder.peerAuthenticationPolicy;
        peerServiceName = builder.peerServiceName;
        identityResolutionTimeout = builder.identityResolutionTimeout;
        peerProviderConcurrency = builder.peerProviderConcurrency;
        if (identityResolutionTimeout == null || identityResolutionTimeout.isZero()
                || identityResolutionTimeout.isNegative()) {
            throw new IllegalArgumentException("identity resolution timeout must be positive");
        }
        HashSet<String> names = new HashSet<>();
        for (PeerIdentityProvider provider : peerIdentityProviders) {
            if (provider == null || provider.provider() == null || provider.provider().isBlank()
                    || !names.add(provider.provider())) {
                throw new IllegalArgumentException("peer identity providers must have unique non-empty names");
            }
        }
        if (peerAuthenticationPolicy != null && peerIdentityProviders.isEmpty()) {
            throw new IllegalArgumentException("peer authentication policy requires an identity provider");
        }
        if (peerProviderConcurrency <= 0 || peerProviderConcurrency < peerIdentityProviders.size()) {
            throw new IllegalArgumentException("provider concurrency must accommodate one complete resolution fanout");
        }
    }

    public static Builder builder() { return new Builder(); }
    public static TcpServerOptions defaults() { return builder().build(); }
    public List<PeerIdentityProvider> peerIdentityProviders() { return peerIdentityProviders; }
    public PeerAuthenticationPolicy peerAuthenticationPolicy() { return peerAuthenticationPolicy; }
    public String peerServiceName() { return peerServiceName; }
    public Duration identityResolutionTimeout() { return identityResolutionTimeout; }
    public int peerProviderConcurrency() { return peerProviderConcurrency; }

    /** Builder retaining anonymous behavior unless providers are explicitly configured. */
    public static final class Builder {
        private List<PeerIdentityProvider> peerIdentityProviders = List.of();
        private PeerAuthenticationPolicy peerAuthenticationPolicy;
        private String peerServiceName;
        private Duration identityResolutionTimeout = Duration.ofSeconds(1);
        private int peerProviderConcurrency = 64;

        public Builder peerIdentityProviders(List<PeerIdentityProvider> providers) {
            peerIdentityProviders = providers != null ? providers : List.of();
            return this;
        }
        public Builder peerAuthenticationPolicy(PeerAuthenticationPolicy policy) {
            peerAuthenticationPolicy = policy;
            return this;
        }
        public Builder peerServiceName(String serviceName) {
            peerServiceName = serviceName;
            return this;
        }
        public Builder identityResolutionTimeout(Duration timeout) {
            identityResolutionTimeout = timeout;
            return this;
        }
        public Builder peerProviderConcurrency(int limit) {
            peerProviderConcurrency = limit;
            return this;
        }
        public TcpServerOptions build() { return new TcpServerOptions(this); }
    }
}
