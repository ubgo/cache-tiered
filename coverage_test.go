// coverage_test.go — targeted branch coverage for the tiered composer:
// New panic / nil-tier compaction / WithL3, closed-adapter guards, error
// propagation (L1 authoritative vs deeper best-effort), promote, Touch,
// Stats, Ping-first-unhealthy, Close idempotency. Uses a fault-injecting
// in-process cache.Cache implemented in test code only. Deterministic.

package tieredcache_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ubgo/cache"
	memcache "github.com/ubgo/cache-mem"
	tieredcache "github.com/ubgo/cache-tiered"
)

var errBoom = errors.New("boom")

// faultCache wraps a real mem cache and forces an error from selected
// methods. Test-only; no production change.
type faultCache struct {
	inner cache.Cache
	fail  map[string]bool // method name -> always error errBoom
}

func newFault(fail ...string) *faultCache {
	m := map[string]bool{}
	for _, f := range fail {
		m[f] = true
	}
	return &faultCache{inner: memcache.New(), fail: m}
}

func (f *faultCache) Get(ctx context.Context, k string) ([]byte, error) {
	if f.fail["Get"] {
		return nil, errBoom
	}
	return f.inner.Get(ctx, k)
}
func (f *faultCache) GetMulti(ctx context.Context, ks []string) (map[string][]byte, error) {
	if f.fail["GetMulti"] {
		return nil, errBoom
	}
	return f.inner.GetMulti(ctx, ks)
}
func (f *faultCache) Has(ctx context.Context, k string) (bool, error) {
	if f.fail["Has"] {
		return false, errBoom
	}
	return f.inner.Has(ctx, k)
}
func (f *faultCache) TTL(ctx context.Context, k string) (time.Duration, error) {
	if f.fail["TTL"] {
		return 0, errBoom
	}
	return f.inner.TTL(ctx, k)
}
func (f *faultCache) Set(ctx context.Context, k string, v []byte, ttl time.Duration) error {
	if f.fail["Set"] {
		return errBoom
	}
	return f.inner.Set(ctx, k, v, ttl)
}
func (f *faultCache) SetMulti(ctx context.Context, items map[string]cache.Item) error {
	if f.fail["SetMulti"] {
		return errBoom
	}
	return f.inner.SetMulti(ctx, items)
}
func (f *faultCache) SetNX(ctx context.Context, k string, v []byte, ttl time.Duration) (bool, error) {
	if f.fail["SetNX"] {
		return false, errBoom
	}
	return f.inner.SetNX(ctx, k, v, ttl)
}
func (f *faultCache) Expire(ctx context.Context, k string, ttl time.Duration) error {
	if f.fail["Expire"] {
		return errBoom
	}
	return f.inner.Expire(ctx, k, ttl)
}
func (f *faultCache) Touch(ctx context.Context, k string) error {
	if f.fail["Touch"] {
		return errBoom
	}
	return f.inner.Touch(ctx, k)
}
func (f *faultCache) Incr(ctx context.Context, k string, d int64) (int64, error) {
	if f.fail["Incr"] {
		return 0, errBoom
	}
	return f.inner.Incr(ctx, k, d)
}
func (f *faultCache) Decr(ctx context.Context, k string, d int64) (int64, error) {
	if f.fail["Decr"] {
		return 0, errBoom
	}
	return f.inner.Decr(ctx, k, d)
}
func (f *faultCache) Del(ctx context.Context, ks ...string) error {
	if f.fail["Del"] {
		return errBoom
	}
	return f.inner.Del(ctx, ks...)
}
func (f *faultCache) DeleteByPrefix(ctx context.Context, p string) error {
	if f.fail["DeleteByPrefix"] {
		return errBoom
	}
	return f.inner.DeleteByPrefix(ctx, p)
}
func (f *faultCache) Flush(ctx context.Context) error {
	if f.fail["Flush"] {
		return errBoom
	}
	return f.inner.Flush(ctx)
}
func (f *faultCache) Iterate(ctx context.Context, o cache.IterateOpts) cache.Iterator {
	return f.inner.Iterate(ctx, o)
}
func (f *faultCache) Ping(ctx context.Context) error {
	if f.fail["Ping"] {
		return errBoom
	}
	return f.inner.Ping(ctx)
}
func (f *faultCache) Close() error {
	if f.fail["Close"] {
		return errBoom
	}
	return f.inner.Close()
}
func (f *faultCache) Stats() cache.Stats {
	s := f.inner.Stats()
	s.Hits += 7 // make summation observable
	return s
}

func TestNewPanicsWithoutL1(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("New without WithL1 must panic")
		}
	}()
	tieredcache.New()
}

func TestNewCompactsNilTiers(t *testing.T) {
	// WithL3 set but no WithL2: the nil L2 slot must be compacted away so the
	// 3-arg slice becomes a valid 2-tier cache (L1 + the former L3).
	ctx := context.Background()
	l1, l3 := memcache.New(), memcache.New()
	tc := tieredcache.New(tieredcache.WithL1(l1), tieredcache.WithL3(l3))
	defer func() { _ = tc.Close() }()
	if err := tc.Set(ctx, "k", []byte("v"), time.Hour); err != nil {
		t.Fatal(err)
	}
	if ok, _ := l1.Has(ctx, "k"); !ok {
		t.Fatal("L1 missing")
	}
	if ok, _ := l3.Has(ctx, "k"); !ok {
		t.Fatal("compacted L3 missing")
	}
}

func TestThreeTierPromotionAndTTLZero(t *testing.T) {
	ctx := context.Background()
	l1, l2, l3 := memcache.New(), memcache.New(), memcache.New()
	tc := tieredcache.New(
		tieredcache.WithL1(l1), tieredcache.WithL2(l2), tieredcache.WithL3(l3),
		tieredcache.WithPerTierTTL(map[int]time.Duration{1: time.Hour, 2: time.Hour}),
	)
	defer func() { _ = tc.Close() }()

	if err := l3.Set(ctx, "k", []byte("v"), time.Hour); err != nil {
		t.Fatal(err)
	}
	v, err := tc.Get(ctx, "k")
	if err != nil || string(v) != "v" {
		t.Fatalf("3-tier get: %q %v", v, err)
	}
	if ok, _ := l1.Has(ctx, "k"); !ok {
		t.Fatal("not promoted into L1")
	}
	if ok, _ := l2.Has(ctx, "k"); !ok {
		t.Fatal("not promoted into L2")
	}
	if tc.Promotions() < 2 {
		t.Fatalf("want >=2 promotions, got %d", tc.Promotions())
	}
}

func TestClosedReturnsErrClosedAllMethods(t *testing.T) {
	ctx := context.Background()
	tc := tieredcache.New(tieredcache.WithL1(memcache.New()))
	if err := tc.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tc.Close(); err != nil {
		t.Fatalf("Close idempotent: %v", err)
	}
	if _, err := tc.Get(ctx, "k"); err != cache.ErrClosed {
		t.Fatalf("Get: %v", err)
	}
	if _, err := tc.GetMulti(ctx, []string{"k"}); err != cache.ErrClosed {
		t.Fatalf("GetMulti: %v", err)
	}
	if _, err := tc.Has(ctx, "k"); err != cache.ErrClosed {
		t.Fatalf("Has: %v", err)
	}
	if _, err := tc.TTL(ctx, "k"); err != cache.ErrClosed {
		t.Fatalf("TTL: %v", err)
	}
	if err := tc.Set(ctx, "k", []byte("v"), 0); err != cache.ErrClosed {
		t.Fatalf("Set: %v", err)
	}
	if err := tc.SetMulti(ctx, map[string]cache.Item{"k": {Value: []byte("v")}}); err != cache.ErrClosed {
		t.Fatalf("SetMulti: %v", err)
	}
	if _, err := tc.SetNX(ctx, "k", []byte("v"), 0); err != cache.ErrClosed {
		t.Fatalf("SetNX: %v", err)
	}
	if err := tc.Expire(ctx, "k", time.Minute); err != cache.ErrClosed {
		t.Fatalf("Expire: %v", err)
	}
	if err := tc.Touch(ctx, "k"); err != cache.ErrClosed {
		t.Fatalf("Touch: %v", err)
	}
	if _, err := tc.Incr(ctx, "k", 1); err != cache.ErrClosed {
		t.Fatalf("Incr: %v", err)
	}
	if _, err := tc.Decr(ctx, "k", 1); err != cache.ErrClosed {
		t.Fatalf("Decr: %v", err)
	}
	if err := tc.Del(ctx, "k"); err != cache.ErrClosed {
		t.Fatalf("Del: %v", err)
	}
	if err := tc.DeleteByPrefix(ctx, "k"); err != cache.ErrClosed {
		t.Fatalf("DeleteByPrefix: %v", err)
	}
	if err := tc.Flush(ctx); err != cache.ErrClosed {
		t.Fatalf("Flush: %v", err)
	}
	if err := tc.Ping(ctx); err != cache.ErrClosed {
		t.Fatalf("Ping: %v", err)
	}
}

func TestGetNonNotFoundAborts(t *testing.T) {
	ctx := context.Background()
	l1 := newFault("Get")
	l2 := memcache.New()
	tc := tieredcache.New(tieredcache.WithL1(l1), tieredcache.WithL2(l2))
	defer func() { _ = tc.Close() }()
	_ = l2.Set(ctx, "k", []byte("v"), time.Hour)
	if _, err := tc.Get(ctx, "k"); !errors.Is(err, errBoom) {
		t.Fatalf("Get must surface backend error, got %v", err)
	}
}

func TestGetMultiAbortsOnError(t *testing.T) {
	ctx := context.Background()
	tc := tieredcache.New(tieredcache.WithL1(newFault("Get")))
	defer func() { _ = tc.Close() }()
	if _, err := tc.GetMulti(ctx, []string{"k"}); !errors.Is(err, errBoom) {
		t.Fatalf("GetMulti must abort on non-miss error, got %v", err)
	}
}

func TestHasErrorPropagates(t *testing.T) {
	ctx := context.Background()
	tc := tieredcache.New(tieredcache.WithL1(newFault("Has")))
	defer func() { _ = tc.Close() }()
	if _, err := tc.Has(ctx, "k"); !errors.Is(err, errBoom) {
		t.Fatalf("Has error: %v", err)
	}
}

func TestTTLErrorAndChain(t *testing.T) {
	ctx := context.Background()
	// L1 returns not-found, L2 errors -> non-NotFound error surfaces.
	l1 := memcache.New()
	l2 := newFault("TTL")
	tc := tieredcache.New(tieredcache.WithL1(l1), tieredcache.WithL2(l2))
	defer func() { _ = tc.Close() }()
	if _, err := tc.TTL(ctx, "missing"); !errors.Is(err, errBoom) {
		t.Fatalf("TTL chain error: %v", err)
	}
	// All tiers miss -> ErrNotFound.
	tc2 := tieredcache.New(tieredcache.WithL1(memcache.New()), tieredcache.WithL2(memcache.New()))
	defer func() { _ = tc2.Close() }()
	if _, err := tc2.TTL(ctx, "missing"); err != cache.ErrNotFound {
		t.Fatalf("TTL all-miss: %v", err)
	}
	// First match returns.
	tc3 := tieredcache.New(tieredcache.WithL1(memcache.New()))
	defer func() { _ = tc3.Close() }()
	_ = tc3.Set(ctx, "k", []byte("v"), time.Hour)
	if d, err := tc3.TTL(ctx, "k"); err != nil || d <= 0 {
		t.Fatalf("TTL match: %v %v", d, err)
	}
}

func TestL1AuthoritativeWriteErrors(t *testing.T) {
	ctx := context.Background()

	// L1 Set fails -> Set returns error (fanout i==0).
	tc := tieredcache.New(tieredcache.WithL1(newFault("Set")), tieredcache.WithL2(memcache.New()))
	defer func() { _ = tc.Close() }()
	if err := tc.Set(ctx, "k", []byte("v"), 0); !errors.Is(err, errBoom) {
		t.Fatalf("Set L1 err: %v", err)
	}

	// Deeper-tier Set fails -> swallowed, Set succeeds.
	tc2 := tieredcache.New(tieredcache.WithL1(memcache.New()), tieredcache.WithL2(newFault("Set")))
	defer func() { _ = tc2.Close() }()
	if err := tc2.Set(ctx, "k", []byte("v"), 0); err != nil {
		t.Fatalf("deeper Set err must be swallowed: %v", err)
	}

	// SetMulti L1 fails.
	tc3 := tieredcache.New(tieredcache.WithL1(newFault("SetMulti")))
	defer func() { _ = tc3.Close() }()
	if err := tc3.SetMulti(ctx, map[string]cache.Item{"k": {Value: []byte("v")}}); !errors.Is(err, errBoom) {
		t.Fatalf("SetMulti L1 err: %v", err)
	}

	// SetNX L1 fails.
	tc4 := tieredcache.New(tieredcache.WithL1(newFault("SetNX")))
	defer func() { _ = tc4.Close() }()
	if _, err := tc4.SetNX(ctx, "k", []byte("v"), 0); !errors.Is(err, errBoom) {
		t.Fatalf("SetNX L1 err: %v", err)
	}

	// SetNX L1 returns ok=false -> short-circuits without error.
	l1 := memcache.New()
	_ = l1.Set(ctx, "k", []byte("x"), time.Hour)
	tc5 := tieredcache.New(tieredcache.WithL1(l1), tieredcache.WithL2(memcache.New()))
	defer func() { _ = tc5.Close() }()
	if ok, err := tc5.SetNX(ctx, "k", []byte("v"), 0); err != nil || ok {
		t.Fatalf("SetNX existing: %v %v", ok, err)
	}

	// SetNX success mirrors to deeper tiers.
	l2 := memcache.New()
	tc6 := tieredcache.New(tieredcache.WithL1(memcache.New()), tieredcache.WithL2(l2))
	defer func() { _ = tc6.Close() }()
	if ok, err := tc6.SetNX(ctx, "n", []byte("v"), time.Hour); err != nil || !ok {
		t.Fatalf("SetNX new: %v %v", ok, err)
	}
	if has, _ := l2.Has(ctx, "n"); !has {
		t.Fatal("SetNX did not mirror to L2")
	}

	// Expire: ErrNotFound from a tier is tolerated; L1 real error surfaces.
	tc7 := tieredcache.New(tieredcache.WithL1(memcache.New()), tieredcache.WithL2(memcache.New()))
	defer func() { _ = tc7.Close() }()
	_ = tc7.Set(ctx, "k", []byte("v"), time.Hour)
	if err := tc7.Expire(ctx, "k", time.Minute); err != nil {
		t.Fatalf("Expire ok: %v", err)
	}
	if err := tc7.Touch(ctx, "k"); err != nil {
		t.Fatalf("Touch ok: %v", err)
	}
	// Expire on a key absent everywhere: each tier ErrNotFound -> tolerated
	// per tier, fanout returns nil.
	if err := tc7.Expire(ctx, "ghost", time.Minute); err != nil {
		t.Fatalf("Expire ghost should be tolerated: %v", err)
	}
	tc8 := tieredcache.New(tieredcache.WithL1(newFault("Expire")))
	defer func() { _ = tc8.Close() }()
	if err := tc8.Expire(ctx, "k", time.Minute); !errors.Is(err, errBoom) {
		t.Fatalf("Expire L1 real err: %v", err)
	}
}

func TestIncrDecrMirrorAndError(t *testing.T) {
	ctx := context.Background()
	l1, l2 := memcache.New(), memcache.New()
	tc := tieredcache.New(tieredcache.WithL1(l1), tieredcache.WithL2(l2))
	defer func() { _ = tc.Close() }()

	if n, err := tc.Incr(ctx, "c", 5); err != nil || n != 5 {
		t.Fatalf("Incr: %v %v", n, err)
	}
	// L2 mirrored to 5.
	if v, _ := l2.Incr(ctx, "c", 0); v != 5 {
		t.Fatalf("L2 counter not mirrored: %d", v)
	}
	if n, err := tc.Decr(ctx, "c", 2); err != nil || n != 3 {
		t.Fatalf("Decr: %v %v", n, err)
	}
	if v, _ := l2.Incr(ctx, "c", 0); v != 3 {
		t.Fatalf("L2 counter after Decr: %d", v)
	}
	// L1 Incr error surfaces.
	tc2 := tieredcache.New(tieredcache.WithL1(newFault("Incr")))
	defer func() { _ = tc2.Close() }()
	if _, err := tc2.Incr(ctx, "c", 1); !errors.Is(err, errBoom) {
		t.Fatalf("Incr L1 err: %v", err)
	}
}

func TestDelPrefixFlushCascadeAndErrors(t *testing.T) {
	ctx := context.Background()
	l1, l2 := memcache.New(), memcache.New()
	tc := tieredcache.New(tieredcache.WithL1(l1), tieredcache.WithL2(l2))
	defer func() { _ = tc.Close() }()
	_ = tc.Set(ctx, "p:1", []byte("v"), time.Hour)
	_ = tc.Set(ctx, "p:2", []byte("v"), time.Hour)
	if err := tc.DeleteByPrefix(ctx, "p:"); err != nil {
		t.Fatal(err)
	}
	if ok, _ := l2.Has(ctx, "p:1"); ok {
		t.Fatal("DeleteByPrefix did not cascade to L2")
	}
	_ = tc.Set(ctx, "x", []byte("v"), time.Hour)
	if err := tc.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if ok, _ := l2.Has(ctx, "x"); ok {
		t.Fatal("Flush did not cascade")
	}
	// L1 error paths.
	tc2 := tieredcache.New(tieredcache.WithL1(newFault("Del")))
	defer func() { _ = tc2.Close() }()
	if err := tc2.Del(ctx, "k"); !errors.Is(err, errBoom) {
		t.Fatalf("Del L1 err: %v", err)
	}
	tc3 := tieredcache.New(tieredcache.WithL1(newFault("DeleteByPrefix")))
	defer func() { _ = tc3.Close() }()
	if err := tc3.DeleteByPrefix(ctx, "p"); !errors.Is(err, errBoom) {
		t.Fatalf("DeleteByPrefix L1 err: %v", err)
	}
	tc4 := tieredcache.New(tieredcache.WithL1(newFault("Flush")))
	defer func() { _ = tc4.Close() }()
	if err := tc4.Flush(ctx); !errors.Is(err, errBoom) {
		t.Fatalf("Flush L1 err: %v", err)
	}
}

func TestPingFirstUnhealthyAndStats(t *testing.T) {
	ctx := context.Background()
	l1 := memcache.New()
	l2 := newFault("Ping")
	tc := tieredcache.New(tieredcache.WithL1(l1), tieredcache.WithL2(l2))
	defer func() { _ = tc.Close() }()
	if err := tc.Ping(ctx); !errors.Is(err, errBoom) {
		t.Fatalf("Ping must report first unhealthy tier: %v", err)
	}

	// Healthy ping.
	tc2 := tieredcache.New(tieredcache.WithL1(memcache.New()))
	defer func() { _ = tc2.Close() }()
	if err := tc2.Ping(ctx); err != nil {
		t.Fatalf("healthy Ping: %v", err)
	}

	// Stats: hits summed across tiers, entries from deepest.
	l1b, l2b := memcache.New(), &faultCache{inner: memcache.New(), fail: map[string]bool{}}
	tc3 := tieredcache.New(tieredcache.WithL1(l1b), tieredcache.WithL2(l2b))
	defer func() { _ = tc3.Close() }()
	_ = tc3.Set(ctx, "k", []byte("v"), time.Hour)
	s := tc3.Stats()
	if s.Hits < 7 {
		t.Fatalf("Stats hits not summed: %+v", s)
	}
}

func TestCloseReturnsFirstTierError(t *testing.T) {
	tc := tieredcache.New(
		tieredcache.WithL1(memcache.New()),
		tieredcache.WithL2(newFault("Close")),
	)
	if err := tc.Close(); !errors.Is(err, errBoom) {
		t.Fatalf("Close must return first tier error: %v", err)
	}
}

func TestInvalidationCloseStopsSubscribe(t *testing.T) {
	bus := cache.NewInProcInvalidation()
	tc := tieredcache.New(
		tieredcache.WithL1(memcache.New()),
		tieredcache.WithInvalidation(bus),
	)
	settle()
	// Close cancels the subscribe ctx and waits for the goroutine.
	if err := tc.Close(); err != nil {
		t.Fatalf("Close with invalidation: %v", err)
	}
	if err := tc.Close(); err != nil {
		t.Fatalf("second Close idempotent: %v", err)
	}
}
