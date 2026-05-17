// tiered_test.go — tests for the tiered composer (conformance L1-only & L1+L2, read promotion, write-through, WriteOnlyL1, per-tier TTL).

package tieredcache_test

import (
	"context"
	"testing"
	"time"

	"github.com/ubgo/cache"
	memcache "github.com/ubgo/cache-mem"
	tieredcache "github.com/ubgo/cache-tiered"
	"github.com/ubgo/cache/cachetest"
)

func TestConformanceL1Only(t *testing.T) {
	cachetest.Run(t, func(t *testing.T) cache.Cache {
		c := tieredcache.New(tieredcache.WithL1(memcache.New()))
		t.Cleanup(func() { _ = c.Close() })
		return c
	})
}

func TestConformanceL1L2(t *testing.T) {
	cachetest.Run(t, func(t *testing.T) cache.Cache {
		c := tieredcache.New(
			tieredcache.WithL1(memcache.New()),
			tieredcache.WithL2(memcache.New()),
		)
		t.Cleanup(func() { _ = c.Close() })
		return c
	})
}

func TestReadPromotesIntoL1(t *testing.T) {
	ctx := context.Background()
	l1 := memcache.New()
	l2 := memcache.New()
	tc := tieredcache.New(tieredcache.WithL1(l1), tieredcache.WithL2(l2))
	defer func() { _ = tc.Close() }()

	// Seed L2 directly; L1 is cold.
	if err := l2.Set(ctx, "k", []byte("v"), time.Hour); err != nil {
		t.Fatal(err)
	}
	if ok, _ := l1.Has(ctx, "k"); ok {
		t.Fatal("precondition: L1 should be cold")
	}
	// Read through the tiered cache: hits L2, promotes into L1.
	v, err := tc.Get(ctx, "k")
	if err != nil || string(v) != "v" {
		t.Fatalf("get: %q %v", v, err)
	}
	if ok, _ := l1.Has(ctx, "k"); !ok {
		t.Fatal("expected L2 hit to be promoted into L1")
	}
	if tc.Promotions() == 0 {
		t.Fatal("promotion counter not incremented")
	}
}

func TestWriteThroughHitsAllTiers(t *testing.T) {
	ctx := context.Background()
	l1, l2 := memcache.New(), memcache.New()
	tc := tieredcache.New(tieredcache.WithL1(l1), tieredcache.WithL2(l2))
	defer func() { _ = tc.Close() }()

	if err := tc.Set(ctx, "k", []byte("v"), time.Hour); err != nil {
		t.Fatal(err)
	}
	if ok, _ := l1.Has(ctx, "k"); !ok {
		t.Fatal("L1 missing after write-through")
	}
	if ok, _ := l2.Has(ctx, "k"); !ok {
		t.Fatal("L2 missing after write-through")
	}
	if err := tc.Del(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	if ok, _ := l2.Has(ctx, "k"); ok {
		t.Fatal("Del did not cascade to L2")
	}
}

func TestWriteOnlyL1(t *testing.T) {
	ctx := context.Background()
	l1, l2 := memcache.New(), memcache.New()
	tc := tieredcache.New(
		tieredcache.WithL1(l1), tieredcache.WithL2(l2),
		tieredcache.WithWriteMode(tieredcache.WriteOnlyL1),
	)
	defer func() { _ = tc.Close() }()

	if err := tc.Set(ctx, "k", []byte("v"), time.Hour); err != nil {
		t.Fatal(err)
	}
	if ok, _ := l1.Has(ctx, "k"); !ok {
		t.Fatal("L1 should hold the write")
	}
	if ok, _ := l2.Has(ctx, "k"); ok {
		t.Fatal("WriteOnlyL1 must not write to L2")
	}
}

func TestPerTierTTL(t *testing.T) {
	ctx := context.Background()
	l1, l2 := memcache.New(), memcache.New()
	tc := tieredcache.New(
		tieredcache.WithL1(l1), tieredcache.WithL2(l2),
		tieredcache.WithPerTierTTL(map[int]time.Duration{1: 40 * time.Millisecond}),
	)
	defer func() { _ = tc.Close() }()

	if err := tc.Set(ctx, "k", []byte("v"), time.Hour); err != nil {
		t.Fatal(err)
	}
	time.Sleep(90 * time.Millisecond)
	// L1 entry expired (40ms override); L2 still holds it (1h).
	if ok, _ := l1.Has(ctx, "k"); ok {
		t.Fatal("L1 should have expired under per-tier TTL")
	}
	if ok, _ := l2.Has(ctx, "k"); !ok {
		t.Fatal("L2 should still hold the entry")
	}
	// Tiered Get still succeeds via L2 and re-promotes into L1.
	if v, err := tc.Get(ctx, "k"); err != nil || string(v) != "v" {
		t.Fatalf("tiered get via L2: %q %v", v, err)
	}
}
