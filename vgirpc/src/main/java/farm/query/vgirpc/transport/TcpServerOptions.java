// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.transport;

import farm.query.vgirpc.identity.PeerAuthenticationPolicy;
import farm.query.vgirpc.identity.PeerIdentityProvider;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Optional connection-snapshot identity configuration for the raw TCP listener. */
public final class TcpServerOptions {
    private final List<PeerIdentityProvider> peerIdentityProviders;
    private final PeerAuthenticationPolicy peerAuthenticationPolicy;
    private final String peerServiceName;
    private final Duration identityResolutionTimeout;
    private final int peerProviderConcurrency;
    private final boolean proxyProtocolV2Required;
    private final Set<String> trustedProxyAddresses;
    private final Duration proxyPreambleTimeout;
    private final int maximumProxyPreambleBytes;

    private TcpServerOptions(Builder builder) {
        peerIdentityProviders = List.copyOf(builder.peerIdentityProviders);
        peerAuthenticationPolicy = builder.peerAuthenticationPolicy;
        peerServiceName = builder.peerServiceName;
        identityResolutionTimeout = builder.identityResolutionTimeout;
        peerProviderConcurrency = builder.peerProviderConcurrency;
        proxyProtocolV2Required = builder.proxyProtocolV2Required;
        trustedProxyAddresses = ProxyProtocolV2.normalizeTrustedAddresses(builder.trustedProxyAddresses);
        proxyPreambleTimeout = builder.proxyPreambleTimeout;
        maximumProxyPreambleBytes = builder.maximumProxyPreambleBytes;
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
        if (proxyPreambleTimeout == null || proxyPreambleTimeout.isZero()
                || proxyPreambleTimeout.isNegative()) {
            throw new IllegalArgumentException("PROXY v2 preamble timeout must be positive");
        }
        if (maximumProxyPreambleBytes < 16) {
            throw new IllegalArgumentException("maximum PROXY v2 bytes must be at least 16");
        }
        if (proxyProtocolV2Required && trustedProxyAddresses.isEmpty()) {
            throw new IllegalArgumentException(
                    "PROXY v2 requires at least one exact trusted proxy address");
        }
    }

    public static Builder builder() { return new Builder(); }
    public static TcpServerOptions defaults() { return builder().build(); }
    public List<PeerIdentityProvider> peerIdentityProviders() { return peerIdentityProviders; }
    public PeerAuthenticationPolicy peerAuthenticationPolicy() { return peerAuthenticationPolicy; }
    public String peerServiceName() { return peerServiceName; }
    public Duration identityResolutionTimeout() { return identityResolutionTimeout; }
    public int peerProviderConcurrency() { return peerProviderConcurrency; }
    public boolean proxyProtocolV2Required() { return proxyProtocolV2Required; }
    public Set<String> trustedProxyAddresses() { return trustedProxyAddresses; }
    public Duration proxyPreambleTimeout() { return proxyPreambleTimeout; }
    public int maximumProxyPreambleBytes() { return maximumProxyPreambleBytes; }

    /** Builder retaining anonymous behavior unless providers are explicitly configured. */
    public static final class Builder {
        private List<PeerIdentityProvider> peerIdentityProviders = List.of();
        private PeerAuthenticationPolicy peerAuthenticationPolicy;
        private String peerServiceName;
        private Duration identityResolutionTimeout = Duration.ofSeconds(1);
        private int peerProviderConcurrency = 64;
        private boolean proxyProtocolV2Required;
        private Set<String> trustedProxyAddresses = Set.of();
        private Duration proxyPreambleTimeout = Duration.ofSeconds(1);
        private int maximumProxyPreambleBytes = ProxyProtocolV2.DEFAULT_MAXIMUM_BYTES;

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
        /** Require a trusted PROXY protocol v2 preamble on every accepted connection. */
        public Builder proxyProtocolV2Required(boolean required) {
            proxyProtocolV2Required = required;
            return this;
        }
        /** Configure exact immediate proxy IP literals. Hostnames and CIDRs are rejected. */
        public Builder trustedProxyAddresses(Set<String> addresses) {
            trustedProxyAddresses = addresses != null ? addresses : Set.of();
            return this;
        }
        /** Set the short, independent absolute deadline for reading the preamble. */
        public Builder proxyPreambleTimeout(Duration timeout) {
            proxyPreambleTimeout = timeout;
            return this;
        }
        /** Bound the complete fixed preamble, address block, and unknown TLVs before allocation. */
        public Builder maximumProxyPreambleBytes(int maximumBytes) {
            maximumProxyPreambleBytes = maximumBytes;
            return this;
        }
        public TcpServerOptions build() { return new TcpServerOptions(this); }
    }
}
