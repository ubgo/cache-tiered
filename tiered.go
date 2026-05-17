// tiered.go — the L1/L2/L3 cache.Cache composer (package tieredcache, github.com/ubgo/cache-tiered).
//
// Package role: tieredcache composes several cache.Cache backends into an
// L1/L2(/L3) hierarchy and is itself a cache.Cache, so callers see no
// difference. See doc.go for the package overview and invariants.
//
// This file: defines WriteMode, Cache, the options (WithL1..WithL3,
// WithPerTierTTL, WithWriteMode, WithInvalidation), New, every cache.Cache
// method, and the helpers fanout/ttlFor/mirrorCounter/publishInval.
// Invariants an AI must keep: tiers[0] is L1 and is authoritative for the
// write error and for atomic ops (SetNX/Incr/Decr) — deeper-tier write and
// promote errors are intentionally swallowed (best-effort); a read promotes
// the first hit into every shallower tier using the destination tier's TTL;
// a non-ErrNotFound read error aborts the probe (an outage must not look like
// a miss); WithPerTierTTL is 1-based while tier slices are 0-based and ttlFor
// bridges them; the invalidation Subscribe goroutine only ever mutates L1 and
// Close cancels it and invWG.Wait()s before closing tiers.
//
// AI-context: composer-of-cache.Cache; New panics if no L1 is configured —
// a tiered cache without its hot tier is always a wiring bug surfaced at
// startup, never softened to a runtime error.

package tieredcache

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ubgo/cache"
)

// WriteMode controls how writes propagate across tiers.
type WriteMode int

const (
	// WriteThrough writes to every tier (default).
	WriteThrough WriteMode = iota
	// WriteOnlyL1 writes only to L1; deeper tiers fill lazily on eviction
	// pushes — here, simply not written on Set.
	WriteOnlyL1
)

// Cache is the tiered composer. Construct with New.
type Cache struct {
	tiers   []cache.Cache         // index 0 = L1
	perTTL  map[int]time.Duration // 1-based tier -> TTL override
	mode    WriteMode
	closed  atomic.Bool
	promote atomic.Int64 // observability: promotion count

	inval     cache.Invalidation
	invCancel context.CancelFunc
	invWG     sync.WaitGroup
}

// Option configures New.
type Option func(*Cache)

// WithL1 sets the fastest, closest tier (required).
func WithL1(c cache.Cache) Option { return func(t *Cache) { t.setTier(0, c) } }

// WithL2 sets the second tier.
func WithL2(c cache.Cache) Option { return func(t *Cache) { t.setTier(1, c) } }

// WithL3 sets the third tier.
func WithL3(c cache.Cache) Option { return func(t *Cache) { t.setTier(2, c) } }

// WithPerTierTTL overrides the TTL used when writing/promoting into a given
// 1-based tier. Tiers absent from the map use the caller-supplied TTL.
func WithPerTierTTL(m map[int]time.Duration) Option {
	return func(t *Cache) { t.perTTL = m }
}

// WithWriteMode selects the write propagation strategy (default WriteThrough).
func WithWriteMode(m WriteMode) Option { return func(t *Cache) { t.mode = m } }

// WithInvalidation wires a cross-process invalidation bus. The tiered cache
// then (a) drops the affected key from L1 when any peer publishes, and
// (b) publishes on Del/DeleteByPrefix/Flush so peers drop theirs. This lets
// L1 run a longer TTL safely. Delivery is best-effort.
func WithInvalidation(inv cache.Invalidation) Option {
	return func(t *Cache) { t.inval = inv }
}

func (t *Cache) setTier(i int, c cache.Cache) {
	for len(t.tiers) <= i {
		t.tiers = append(t.tiers, nil)
	}
	t.tiers[i] = c
}

// New builds a tiered cache. Panics if no L1 is configured — a tiered cache
// without its hot tier is always a construction bug, not a runtime condition.
func New(opts ...Option) *Cache {
	t := &Cache{mode: WriteThrough}
	for _, o := range opts {
		o(t)
	}
	live := t.tiers[:0]
	for _, c := range t.tiers {
		if c != nil {
			live = append(live, c)
		}
	}
	t.tiers = live
	if len(t.tiers) == 0 {
		panic("cache-tiered: at least one tier (WithL1) is required")
	}
	if t.inval != nil {
		// Long-lived subscribe goroutine. Its context is cancelled by Close,
		// which then invWG.Wait()s before closing tiers so the goroutine never
		// touches a closed L1. Subscribe blocks, hence the dedicated goroutine.
		ctx, cancel := context.WithCancel(context.Background())
		t.invCancel = cancel
		t.invWG.Add(1)
		go func() {
			defer t.invWG.Done()
			_ = t.inval.Subscribe(ctx, func(key string) {
				// Only L1 is dropped: deeper tiers are shared and
				// authoritative, so a peer's signal must not evict them.
				// InvalidateAll cannot be expressed key-by-key, so flush L1.
				if key == cache.InvalidateAll {
					_ = t.tiers[0].Flush(ctx)
					return
				}
				_ = t.tiers[0].Del(ctx, key) // drop only the local L1 copy
			})
		}()
	}
	return t
}

// publishInval best-effort announces invalidated keys to peers.
func (t *Cache) publishInval(ctx context.Context, keys ...string) {
	if t.inval != nil {
		_ = t.inval.Publish(ctx, keys...)
	}
}

// ttlFor returns the TTL to use when writing into 0-based tier idx. perTTL is
// caller-facing and 1-based (L1 == key 1), so the lookup is idx+1; tiers not
// in the map fall back to callerTTL. Promotion calls this with callerTTL=0 so
// a promoted entry adopts the destination tier's configured lifetime (e.g. a
// short L1 TTL that bounds cross-pod staleness) rather than the original TTL.
func (t *Cache) ttlFor(idx int, callerTTL time.Duration) time.Duration {
	if t.perTTL != nil {
		if d, ok := t.perTTL[idx+1]; ok {
			return d
		}
	}
	return callerTTL
}

// Get implements cache.Cache. First hit is promoted into shallower tiers.
func (t *Cache) Get(ctx context.Context, key string) ([]byte, error) {
	if t.closed.Load() {
		return nil, cache.ErrClosed
	}
	for i, tier := range t.tiers {
		v, err := tier.Get(ctx, key)
		if err == nil {
			// Promote the hit into every shallower tier so the next read is
			// faster. Best-effort: a failed promote must not fail the Get.
			for j := i - 1; j >= 0; j-- {
				if perr := t.tiers[j].Set(ctx, key, v, t.ttlFor(j, 0)); perr == nil {
					t.promote.Add(1)
				}
			}
			return v, nil
		}
		// A real backend error (not a miss) aborts the probe and surfaces:
		// treating an outage as a miss would hide it and stampede the origin.
		if !errors.Is(err, cache.ErrNotFound) {
			return nil, err
		}
	}
	return nil, cache.ErrNotFound
}

// GetMulti implements cache.Cache.
func (t *Cache) GetMulti(ctx context.Context, keys []string) (map[string][]byte, error) {
	if t.closed.Load() {
		return nil, cache.ErrClosed
	}
	out := make(map[string][]byte, len(keys))
	for _, k := range keys {
		if v, err := t.Get(ctx, k); err == nil {
			out[k] = v
		} else if !errors.Is(err, cache.ErrNotFound) {
			return nil, err
		}
	}
	return out, nil
}

// Has implements cache.Cache.
func (t *Cache) Has(ctx context.Context, key string) (bool, error) {
	if t.closed.Load() {
		return false, cache.ErrClosed
	}
	for _, tier := range t.tiers {
		ok, err := tier.Has(ctx, key)
		if err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}
	}
	return false, nil
}

// TTL implements cache.Cache (reports the L1-and-down first match).
func (t *Cache) TTL(ctx context.Context, key string) (time.Duration, error) {
	if t.closed.Load() {
		return 0, cache.ErrClosed
	}
	for _, tier := range t.tiers {
		d, err := tier.TTL(ctx, key)
		if err == nil {
			return d, nil
		}
		if !errors.Is(err, cache.ErrNotFound) {
			return 0, err
		}
	}
	return 0, cache.ErrNotFound
}

// fanout applies fn to every tier. The L1 error is authoritative; deeper-tier
// errors are best-effort so a cold backend cannot break the hot path.
func (t *Cache) fanout(maxTier int, fn func(idx int, c cache.Cache) error) error {
	for i, tier := range t.tiers {
		if i > maxTier {
			break
		}
		err := fn(i, tier)
		if i == 0 && err != nil {
			return err
		}
	}
	return nil
}

func (t *Cache) writeMaxTier() int {
	if t.mode == WriteOnlyL1 {
		return 0
	}
	return len(t.tiers) - 1
}

// Set implements cache.Cache.
func (t *Cache) Set(ctx context.Context, key string, val []byte, ttl time.Duration) error {
	if t.closed.Load() {
		return cache.ErrClosed
	}
	return t.fanout(t.writeMaxTier(), func(i int, c cache.Cache) error {
		return c.Set(ctx, key, val, t.ttlFor(i, ttl))
	})
}

// SetMulti implements cache.Cache.
func (t *Cache) SetMulti(ctx context.Context, items map[string]cache.Item) error {
	if t.closed.Load() {
		return cache.ErrClosed
	}
	return t.fanout(t.writeMaxTier(), func(i int, c cache.Cache) error {
		scoped := make(map[string]cache.Item, len(items))
		for k, it := range items {
			it.TTL = t.ttlFor(i, it.TTL)
			scoped[k] = it
		}
		return c.SetMulti(ctx, scoped)
	})
}

// SetNX implements cache.Cache. L1 is the source of truth for the NX decision;
// on success the value is propagated to deeper tiers.
func (t *Cache) SetNX(ctx context.Context, key string, val []byte, ttl time.Duration) (bool, error) {
	if t.closed.Load() {
		return false, cache.ErrClosed
	}
	ok, err := t.tiers[0].SetNX(ctx, key, val, t.ttlFor(0, ttl))
	if err != nil || !ok {
		return ok, err
	}
	for i := 1; i <= t.writeMaxTier(); i++ {
		_ = t.tiers[i].Set(ctx, key, val, t.ttlFor(i, ttl))
	}
	return true, nil
}

// Expire implements cache.Cache.
func (t *Cache) Expire(ctx context.Context, key string, ttl time.Duration) error {
	if t.closed.Load() {
		return cache.ErrClosed
	}
	return t.fanout(t.writeMaxTier(), func(_ int, c cache.Cache) error {
		err := c.Expire(ctx, key, ttl)
		if errors.Is(err, cache.ErrNotFound) {
			return nil // tier may legitimately not hold it
		}
		return err
	})
}

// Touch implements cache.Cache.
func (t *Cache) Touch(ctx context.Context, key string) error {
	return t.Expire(ctx, key, time.Hour)
}

// Incr implements cache.Cache. L1 is authoritative; the new value is mirrored
// to deeper tiers so reads stay consistent.
func (t *Cache) Incr(ctx context.Context, key string, delta int64) (int64, error) {
	if t.closed.Load() {
		return 0, cache.ErrClosed
	}
	n, err := t.tiers[0].Incr(ctx, key, delta)
	if err != nil {
		return 0, err
	}
	t.mirrorCounter(ctx, key, n)
	return n, nil
}

// Decr implements cache.Cache.
func (t *Cache) Decr(ctx context.Context, key string, delta int64) (int64, error) {
	return t.Incr(ctx, key, -delta)
}

// mirrorCounter converges every deeper tier's counter to L1's authoritative
// value n. Deeper tiers may have drifted (independent expiry, a missed mirror),
// so each is re-derived by applying the delta (n-cur) — read via Incr(key,0) —
// rather than blindly overwriting, which keeps each tier's value atomic.
func (t *Cache) mirrorCounter(ctx context.Context, key string, n int64) {
	for i := 1; i <= t.writeMaxTier(); i++ {
		// Re-derive each tier's counter to n by delta from its current value.
		cur, _ := t.tiers[i].Incr(ctx, key, 0)
		if cur != n {
			_, _ = t.tiers[i].Incr(ctx, key, n-cur)
		}
	}
}

// Del implements cache.Cache. Cascades to every tier.
func (t *Cache) Del(ctx context.Context, keys ...string) error {
	if t.closed.Load() {
		return cache.ErrClosed
	}
	err := t.fanout(len(t.tiers)-1, func(_ int, c cache.Cache) error {
		return c.Del(ctx, keys...)
	})
	t.publishInval(ctx, keys...)
	return err
}

// DeleteByPrefix implements cache.Cache. Cascades to every tier.
func (t *Cache) DeleteByPrefix(ctx context.Context, prefix string) error {
	if t.closed.Load() {
		return cache.ErrClosed
	}
	err := t.fanout(len(t.tiers)-1, func(_ int, c cache.Cache) error {
		return c.DeleteByPrefix(ctx, prefix)
	})
	// A prefix wipe can't be expressed key-by-key over the bus; tell peers to
	// drop their whole local view (conservative but correct).
	t.publishInval(ctx, cache.InvalidateAll)
	return err
}

// Flush implements cache.Cache. Cascades to every tier.
func (t *Cache) Flush(ctx context.Context) error {
	if t.closed.Load() {
		return cache.ErrClosed
	}
	err := t.fanout(len(t.tiers)-1, func(_ int, c cache.Cache) error {
		return c.Flush(ctx)
	})
	t.publishInval(ctx, cache.InvalidateAll)
	return err
}

// Iterate implements cache.Cache, scanning the deepest tier (the most complete
// view of the keyspace).
func (t *Cache) Iterate(ctx context.Context, opts cache.IterateOpts) cache.Iterator {
	return t.tiers[len(t.tiers)-1].Iterate(ctx, opts)
}

// Ping implements cache.Cache. Reports the first unhealthy tier.
func (t *Cache) Ping(ctx context.Context) error {
	if t.closed.Load() {
		return cache.ErrClosed
	}
	for _, tier := range t.tiers {
		if err := tier.Ping(ctx); err != nil {
			return err
		}
	}
	return nil
}

// Close implements cache.Cache. Idempotent; closes every tier once.
func (t *Cache) Close() error {
	if t.closed.Swap(true) {
		return nil
	}
	if t.invCancel != nil {
		t.invCancel()
		t.invWG.Wait()
	}
	var firstErr error
	for _, tier := range t.tiers {
		if err := tier.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Stats implements cache.Cache. Hits/misses/etc. are summed across tiers;
// Entries/Bytes reported from the deepest tier (the fullest set).
func (t *Cache) Stats() cache.Stats {
	var s cache.Stats
	for _, tier := range t.tiers {
		ts := tier.Stats()
		s.Hits += ts.Hits
		s.Misses += ts.Misses
		s.Sets += ts.Sets
		s.Deletes += ts.Deletes
		s.Evictions += ts.Evictions
	}
	deep := t.tiers[len(t.tiers)-1].Stats()
	s.Entries = deep.Entries
	s.Bytes = deep.Bytes
	return s
}

// Promotions returns how many times a deeper-tier hit was copied upward.
func (t *Cache) Promotions() int64 { return t.promote.Load() }

var _ cache.Cache = (*Cache)(nil)
