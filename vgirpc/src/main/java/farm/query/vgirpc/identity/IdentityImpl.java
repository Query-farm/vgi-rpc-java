// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.identity;

import farm.query.vgirpc.AuthContext;
import farm.query.vgirpc.CallContext;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HexFormat;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Applies this protocol's guards, then delegates to worker-supplied hooks.
 *
 * <p>The framework owns the guards and owns none of the policy. It decides who may ask and what
 * shape of credential is refused outright; the worker decides what a credential resolves to and
 * whether a grant is minted. That split is deliberate -- the guards are the part
 * that is identical in every deployment and catastrophic to get wrong, and the policy is the
 * part that is different in every deployment and cannot be guessed.
 *
 * <p><strong>A method whose hook is absent is not registered at all</strong>, so the protocol a
 * server hosts describes what it actually does. A worker that resolves credentials but does not
 * mint grants hosts {@code introspect_token} and not {@code issue_grant}, and a client discovers
 * that through ordinary reflection rather than by calling and reading an error. Absent beats
 * routed-and-refusing: it is what keeps a dependency upgrade from growing a
 * credential-to-identity oracle on every existing worker. The per-method guard below is the belt
 * to that braces -- both exist, because a caller can still reach a method a narrowed binding
 * never advertised.
 */
public final class IdentityImpl implements Identity {

    /**
     * Three dot-separated base64url segments -- a JWS.
     *
     * <p>Such a credential is validated locally against a key set and MUST NOT be routed to a
     * resolver: doing so sends a bearer token the asker may itself have rejected (expired, wrong
     * audience) to a third party that might accept it.
     */
    private static final Pattern JWS_SHAPED =
            Pattern.compile("^[A-Za-z0-9_-]+\\.[A-Za-z0-9_-]+\\.[A-Za-z0-9_-]*$");

    /** How recently a caller must have authenticated to mint a grant, in seconds. */
    public static final double DEFAULT_MAX_AUTH_AGE_SECONDS = 900.0;

    private final TokenResolveHook resolveToken;
    private final GrantMintHook mintGrant;
    private final Set<String> principals;
    private final double maxAuthAge;

    private IdentityImpl(Builder b) {
        this.resolveToken = b.resolveToken;
        this.mintGrant = b.mintGrant;
        this.maxAuthAge = b.maxAuthAge;
        // Validated at construction, not at first call: a worker that would
        // refuse every introspection should fail to start rather than serve
        // traffic until someone tries.
        this.principals = b.resolveToken != null
                ? normalisePrincipals(b.introspectPrincipals)
                : Set.of();
    }

    /**
     * Start configuring an implementation.
     *
     * @return a fresh builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /** Assembles an {@link IdentityImpl}, validating the allowlist before one exists. */
    public static final class Builder {
        private TokenResolveHook resolveToken;
        private GrantMintHook mintGrant;
        private Collection<String> introspectPrincipals;
        private double maxAuthAge = DEFAULT_MAX_AUTH_AGE_SECONDS;

        private Builder() {}

        /**
         * Host {@code introspect_token}, backed by {@code hook}.
         *
         * <p>Supplying this makes {@link #introspectPrincipals} mandatory.
         *
         * @param hook resolves a credential; {@code null} return means "known store, unknown
         *     credential", {@link IdentityUnavailableError} means "not knowable"
         * @return this builder
         */
        public Builder resolveToken(TokenResolveHook hook) {
            this.resolveToken = hook;
            return this;
        }

        /**
         * Host {@code issue_grant}, backed by {@code hook}.
         *
         * @param hook mints a grant for the calling principal
         * @return this builder
         */
        public Builder mintGrant(GrantMintHook hook) {
            this.mintGrant = hook;
            return this;
        }

        /**
         * Who may call {@code introspect_token}.
         *
         * <p>Required whenever a resolver is supplied; there is no permissive default.
         *
         * @param principals the allowlisted caller principals
         * @return this builder
         */
        public Builder introspectPrincipals(Collection<String> principals) {
            this.introspectPrincipals = principals;
            return this;
        }

        /**
         * Who may call {@code introspect_token}.
         *
         * @param principals the allowlisted caller principals
         * @return this builder
         */
        public Builder introspectPrincipals(String... principals) {
            // Arrays.asList, not List.of: a null or blank entry must reach
            // normalisePrincipals and be dropped there, so an allowlist that
            // ends up empty fails as "names no principal" rather than as an
            // unrelated NullPointerException from the collection factory.
            this.introspectPrincipals = principals == null ? null : Arrays.asList(principals);
            return this;
        }

        /**
         * How recently a caller must have authenticated to mint a grant.
         *
         * @param seconds the ceiling on {@code now - auth_time}
         * @return this builder
         */
        public Builder maxAuthAge(double seconds) {
            this.maxAuthAge = seconds;
            return this;
        }

        /**
         * Build the implementation.
         *
         * @return the configured implementation
         * @throws IllegalArgumentException when a resolver was supplied without an allowlist
         */
        public IdentityImpl build() {
            return new IdentityImpl(this);
        }
    }

    /**
     * The methods this deployment can actually answer.
     *
     * <p>A method whose hook is absent is not registered, so the protocol a server hosts
     * describes what it does. A worker that resolves credentials but does not mint grants offers
     * {@code introspect_token} and not {@code issue_grant}, and a client learns that from
     * reflection rather than by calling and reading an error.
     *
     * @return the offered method names; empty when the protocol should not be registered at all
     */
    public Set<String> offeredMethods() {
        Set<String> offered = new LinkedHashSet<>();
        if (resolveToken != null) offered.add("introspect_token");
        if (mintGrant != null) offered.add("issue_grant");
        return Set.copyOf(offered);
    }

    @Override
    public TokenIdentity introspect_token(String token, CallContext ctx) {
        if (resolveToken == null) {
            throw new IntrospectionRefusedError("this worker does not resolve credentials");
        }

        // ORDER IS LOAD-BEARING, and the order is: authorization, then anything
        // that touches the subject credential. An unauthorized caller must learn
        // nothing about that credential -- including how long looking at it
        // took, which is what a length check or a regex match ahead of this
        // would leak. Do not reorder for tidiness.
        //
        // There is deliberately no rate limit here: see Identity's class doc.
        // The allowlist is the control.
        checkIntrospector(ctx == null ? null : ctx.auth(), principals);
        rejectJwsShaped(token);

        TokenIdentity identity = resolveToken.resolve(token);
        if (identity == null) {
            // Uniform with malformed and expired: reporting which would confirm
            // that a guessed credential exists.
            throw new TokenUnresolvedError("unresolved");
        }
        return identity;
    }

    @Override
    public IssuedGrant issue_grant(String purpose, List<String> scopes, long ttl_seconds,
                                   CallContext ctx) {
        if (mintGrant == null) {
            throw new GrantRefusedError("this worker does not mint grants");
        }
        AuthContext auth = ctx == null ? null : ctx.auth();
        checkFreshness(auth, maxAuthAge);
        // The subject is the caller, never a parameter: cross-subject minting is
        // closed by construction rather than by a check that could be forgotten
        // in one of seven ports.
        String principal = auth == null || auth.principal() == null ? "" : auth.principal();
        return mintGrant.mint(principal, purpose, scopes == null ? List.of() : scopes, ttl_seconds);
    }

    // -----------------------------------------------------------------------
    // Guards, exposed individually so a deployment embedding them elsewhere
    // gets the same behaviour rather than a second, drifting copy.
    // -----------------------------------------------------------------------

    /**
     * Validate the introspector allowlist.
     *
     * <p>There is no permissive default: "any authenticated caller" is precisely the
     * configuration that turns introspection into an open oracle, so it must not be reachable by
     * omission.
     *
     * @param principals the configured allowlist
     * @return the allowlist, blanks dropped
     * @throws IllegalArgumentException when it names no principal
     */
    public static Set<String> normalisePrincipals(Collection<String> principals) {
        Set<String> allowed = new LinkedHashSet<>();
        if (principals != null) {
            for (String p : principals) {
                if (p != null && !p.isEmpty()) allowed.add(p);
            }
        }
        if (allowed.isEmpty()) {
            throw new IllegalArgumentException(
                    "introspectPrincipals must name at least one principal. Introspection is a "
                            + "distinct capability from authentication: allowing any authenticated "
                            + "caller lets any user resolve any other user's credential to its owner.");
        }
        return Set.copyOf(allowed);
    }

    /**
     * Return the caller principal, or refuse.
     *
     * <p>Checked before anything touches the subject credential: an unauthorized caller must not
     * learn anything about it, including how long it took.
     *
     * @param auth the caller's authenticated context
     * @param principals the introspector allowlist
     * @return the caller principal
     * @throws IntrospectionRefusedError when the caller is unauthenticated or off the allowlist
     */
    public static String checkIntrospector(AuthContext auth, Set<String> principals) {
        String caller = auth == null || auth.principal() == null ? "" : auth.principal();
        if (auth == null || !auth.authenticated() || !principals.contains(caller)) {
            throw new IntrospectionRefusedError("caller is not an introspector");
        }
        return caller;
    }

    /**
     * Refuse a blank, over-long, or JWS-shaped subject before it reaches a resolver.
     *
     * <p>All of them are one answer. A JWS arriving here is either a caller bug or an attempt to
     * have this worker vouch for a token its asker already rejected -- forwarding one would turn
     * this method into a laundering step, because an expired access token is still live at its
     * issuer for other resources.
     *
     * <p><strong>The shape test runs against the whitespace-trimmed credential, while the
     * resolver still receives what the caller actually sent.</strong> Trimming can only add
     * refusals, never remove one, and it closes a padding bypass that this port had: with a
     * strict full-region match, {@code "a.b.c\n"} is not JWS-shaped, so it was routed onward --
     * precisely what this guard exists to stop. Anchor semantics are the least portable corner of
     * seven regex dialects (the reference happened to refuse one trailing newline and admit two,
     * as an artifact of Python's {@code $}; Go's {@code \A..\z} and this port's
     * {@link java.util.regex.Matcher#matches()} refused neither), so the rule is to stop
     * depending on them. Trimming first means the same thing everywhere.
     *
     * <p>Trimming is for the shape test <em>only</em>. Rewriting a credential before resolving it
     * would make the worker answer about a string the caller never sent.
     *
     * <p>The whitespace set is {@link #stripWhitespace} rather than {@link String#strip()}, for
     * the same reason the anchors went: "whitespace" means different things in different
     * languages, so {@code IDENTITY_V1_SPEC.md} §4 enumerates the floor every port must trim
     * ({@code U+0009}-{@code U+000D}, {@code U+0020}, {@code U+0085}, {@code U+00A0}) instead of
     * delegating it. No single Java predicate spans that set: {@code strip()} uses
     * {@link Character#isWhitespace(char)}, which excludes the non-breaking spaces (U+00A0,
     * U+2007, U+202F), and {@link Character#isSpaceChar(char)} excludes U+0085 NEL. Left alone, a
     * JWS padded with either would be refused by Python and routed onward here -- a smaller copy
     * of the bug being fixed.
     *
     * <p>A whitespace-only credential is refused for the same reason an empty one is: it is not a
     * credential. The length check stays against the <em>original</em>, because the megabytes a
     * caller sent are the megabytes the framework had to hold -- and it is measured in UTF-8
     * bytes rather than {@link String#length()}'s UTF-16 code units, for the same reason: what is
     * being bounded is what a resolver would have to handle. See {@link Identity#MAX_TOKEN_BYTES}.
     *
     * @param token the subject credential, exactly as the caller sent it
     * @throws TokenUnresolvedError when the credential is refused on shape alone
     */
    public static void rejectJwsShaped(String token) {
        if (token == null) throw new TokenUnresolvedError("unresolved");
        String candidate = stripWhitespace(token);
        if (candidate.isEmpty() || utf8Length(token) > MAX_TOKEN_BYTES
                || JWS_SHAPED.matcher(candidate).matches()) {
            throw new TokenUnresolvedError("unresolved");
        }
    }

    /**
     * Return the caller's {@code auth_time}, or refuse if it is missing or stale.
     *
     * <p>A credential with no verifiable {@code auth_time} cannot mint. That single rule is what
     * stops a grant being used to mint another grant: a grant is not an IdP-issued token, so it
     * carries no {@code auth_time}, so the lineage cannot escape the identity provider. It also
     * makes subprocess and unix transports fail closed for free -- there is no authenticated
     * principal there at all. A static bearer proves a machine holds a secret, never that a human
     * just authenticated, so it is refused here too.
     *
     * <p><strong>Warning.</strong> {@code auth_time} is an OIDC claim meaning <em>when this
     * session began</em>, which can be arbitrarily old while still present and cryptographically
     * valid. Requiring it is not the same as requiring a recent login: the deployment must send
     * {@code max_age} (or an appropriate {@code acr}) at the authorize endpoint for this guard to
     * mean what it says.
     *
     * @param auth the caller's authenticated context
     * @param maxAuthAge the ceiling on {@code now - auth_time}, in seconds
     * @return the caller's {@code auth_time}
     * @throws StaleAuthError when the caller is unauthenticated, carries no usable
     *     {@code auth_time}, or authenticated too long ago
     */
    public static double checkFreshness(AuthContext auth, double maxAuthAge) {
        return checkFreshness(auth, maxAuthAge, System.currentTimeMillis() / 1000.0);
    }

    /**
     * Return the caller's {@code auth_time} as of {@code now}, or refuse.
     *
     * @param auth the caller's authenticated context
     * @param maxAuthAge the ceiling on {@code now - auth_time}, in seconds
     * @param now the current unix time, in seconds
     * @return the caller's {@code auth_time}
     * @throws StaleAuthError when the caller is unauthenticated, carries no usable
     *     {@code auth_time}, or authenticated too long ago
     */
    public static double checkFreshness(AuthContext auth, double maxAuthAge, double now) {
        if (auth == null || !auth.authenticated()
                || auth.principal() == null || auth.principal().isEmpty()) {
            throw new StaleAuthError("caller is not authenticated");
        }
        Object raw = auth.claims().get("auth_time");
        if (raw == null) {
            throw new StaleAuthError(
                    "credential carries no auth_time; only a recently authenticated user may mint a grant");
        }
        double authTime;
        if (raw instanceof Number n) {
            authTime = n.doubleValue();
        } else {
            try {
                authTime = Double.parseDouble(raw.toString().trim());
            } catch (NumberFormatException e) {
                throw new StaleAuthError("credential carries an unusable auth_time");
            }
        }
        if (Double.isNaN(authTime) || Double.isInfinite(authTime)) {
            throw new StaleAuthError("credential carries an unusable auth_time");
        }
        double age = now - authTime;
        if (age > maxAuthAge) {
            throw new StaleAuthError(String.format(Locale.ROOT,
                    "last authentication was %.0fs ago, which exceeds the %.0fs ceiling for "
                            + "minting a grant; re-authenticate", age, maxAuthAge));
        }
        return authTime;
    }

    /**
     * Strip leading and trailing whitespace, over at least the set every port must trim.
     *
     * <p>The floor is <em>enumerated</em> by {@code IDENTITY_V1_SPEC.md} §4 --
     * {@code U+0009}-{@code U+000D}, {@code U+0020}, {@code U+0085}, {@code U+00A0} -- rather
     * than delegated to any language's own notion of whitespace, because "whitespace" is itself
     * a divergence one layer down. No single Java predicate spans it:
     * {@link Character#isWhitespace(char)} excludes {@code U+00A0} (and the other non-breaking
     * spaces {@code U+2007}, {@code U+202F}) by design, and {@link Character#isSpaceChar(char)}
     * excludes {@code U+0085} NEL. Either alone leaves a credential padded that Python and Go
     * refuse -- a padded JWS routed onward here is the exact hole this guard exists to close.
     *
     * <p>So the two predicates are combined and {@code U+0085} is named explicitly. The result
     * is a strict superset of the mandated floor (it also covers {@code Zs}/{@code Zl}/
     * {@code Zp}), which is the safe direction: stripping <em>more</em> can only turn a padded
     * JWS into a recognised one, or a blank-ish credential into an empty one -- both refusals.
     * Stripping <em>less</em> is the leak.
     *
     * @param token the credential as sent
     * @return the credential with surrounding whitespace removed, for shape-testing only
     */
    static String stripWhitespace(String token) {
        int start = 0;
        int end = token.length();
        while (start < end && isPad(token.charAt(start))) start++;
        while (end > start && isPad(token.charAt(end - 1))) end--;
        return token.substring(start, end);
    }

    /**
     * {@code U+0085} NEL: in the mandated trim set, and in neither Java predicate.
     *
     * <p>Named as a constant rather than inlined so the one codepoint that has to be spelled out
     * by hand is greppable from the spec table.
     */
    private static final char NEL = '\u0085';

    private static boolean isPad(char c) {
        return Character.isWhitespace(c) || Character.isSpaceChar(c) || c == NEL;
    }

    /**
     * The credential's length in UTF-8 bytes -- the unit {@link Identity#MAX_TOKEN_BYTES} is in.
     *
     * <p>Computed without materialising the encoded array: the point of the cap is to refuse a
     * credential before anything expensive happens to it, and allocating a copy of a megabyte to
     * discover it is a megabyte undoes part of that.
     *
     * @param token the credential as sent
     * @return its length when encoded as UTF-8
     */
    private static int utf8Length(String token) {
        int bytes = 0;
        for (int i = 0; i < token.length(); i++) {
            char c = token.charAt(i);
            if (c < 0x80) {
                bytes += 1;
            } else if (c < 0x800) {
                bytes += 2;
            } else if (Character.isHighSurrogate(c) && i + 1 < token.length()
                    && Character.isLowSurrogate(token.charAt(i + 1))) {
                // One supplementary codepoint: four bytes for the pair, and the low surrogate
                // must not then be counted again on its own.
                bytes += 4;
                i++;
            } else {
                // Includes an unpaired surrogate, which String.getBytes(UTF_8) replaces with the
                // three-byte U+FFFD. Counting three keeps the two measures identical.
                bytes += 3;
            }
        }
        return bytes;
    }

    /**
     * A SHA-256 hex digest of {@code token}, for diagnostics.
     *
     * <p>The credential itself must never reach a log, a span, or an error message. A digest is
     * stable enough to correlate one credential's failures across records without being the
     * credential.
     *
     * @param token the opaque credential
     * @return lowercase hex digest
     */
    public static String tokenDigest(String token) {
        try {
            MessageDigest sha = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(sha.digest(token.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required by every JRE", e);
        }
    }
}
