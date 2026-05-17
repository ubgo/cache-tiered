// doc.go — canonical package documentation (package tieredcache, github.com/ubgo/cache-tiered).
//
// Package role: this file is the authoritative overview for the ubgo/cache
// tiered composer; start here before reading tiered.go (the implementation).
//
// This file: holds ONLY the package doc comment below — no code. It explains
// the read path (probe L1→Ln, promote first hit upward) and write path
// (write-through, L1 error authoritative, deeper-tier errors best-effort) and
// the design invariants (panic-if-no-L1, 1-based per-tier TTL, invalidation
// goroutine touches only L1) that tiered.go implements.
//
// AI-context: the // Package … block below is the godoc package doc; do not
// duplicate it (revive flags duplicate package comments). The blank line
// after this header keeps it a file header, not a second package comment.

// Package tieredcache composes several cache.Cache backends into an L1/L2(/L3)
// hierarchy and is itself a cache.Cache, so callers see no difference.
//
//	t := tieredcache.New(
//	    tieredcache.WithL1(memCache),    // nanosecond local hits
//	    tieredcache.WithL2(redisCache),  // shared across pods
//	    tieredcache.WithPerTierTTL(map[int]time.Duration{
//	        1: 30 * time.Second, // bound cross-pod staleness of L1
//	        2: 5 * time.Minute,
//	    }),
//	)
//	defer t.Close()
//
// Read path: probe L1→Ln; the first hit is promoted into every shallower tier
// (per-tier TTL) so the next read is faster. Write path: write-through to all
// tiers; tier-1 (L1) errors fail the call, deeper-tier errors are best-effort
// (a cold backend being down must not break the hot path). Deletes, Flush and
// DeleteByPrefix cascade to every tier.
//
// Design invariants worth knowing before editing this package:
//
//   - tiers[0] is L1 and is authoritative for atomic ops (SetNX/Incr/Decr)
//     and for the error returned by writes. Deeper-tier write/promote errors
//     are intentionally swallowed.
//   - A non-ErrNotFound error from any tier during a read aborts the probe and
//     is returned: a backend outage must not masquerade as a cache miss (that
//     would hide the outage and stampede the origin).
//   - New panics if no L1 is configured — a tiered cache without its hot tier
//     is always a wiring bug, surfaced at startup, never softened to an error.
//   - WithPerTierTTL is 1-based (L1 == 1) while tier slices are 0-based;
//     ttlFor bridges them. Promotion writes use the destination tier's TTL.
//   - The invalidation goroutine only ever mutates L1; deeper tiers are shared
//     and authoritative. Close cancels it and waits before closing tiers.
package tieredcache
