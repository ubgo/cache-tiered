package tieredcache_test

import (
	"context"
	"testing"
	"time"

	"github.com/ubgo/cache"
	memcache "github.com/ubgo/cache-mem"
	tieredcache "github.com/ubgo/cache-tiered"
)

// settle gives the New()-spawned Subscribe goroutine time to register before
// we publish (pub/sub has no delivery to not-yet-subscribed peers).
func settle() { time.Sleep(50 * time.Millisecond) }

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met within deadline")
}

// A peer's Del must drop this node's L1 copy via the shared bus.
func TestInvalidationDropsL1OnPeerDelete(t *testing.T) {
	ctx := context.Background()
	bus := cache.NewInProcInvalidation()

	node := tieredcache.New(
		tieredcache.WithL1(memcache.New()),
		tieredcache.WithInvalidation(bus),
	)
	defer func() { _ = node.Close() }()

	settle()
	if err := node.Set(ctx, "k", []byte("v"), time.Hour); err != nil {
		t.Fatal(err)
	}
	if ok, _ := node.Has(ctx, "k"); !ok {
		t.Fatal("precondition: key should be in L1")
	}

	// Simulate another pod publishing an invalidation for "k".
	if err := bus.Publish(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { ok, _ := node.Has(ctx, "k"); return !ok })
}

// InvalidateAll must flush this node's L1.
func TestInvalidateAllFlushesL1(t *testing.T) {
	ctx := context.Background()
	bus := cache.NewInProcInvalidation()
	node := tieredcache.New(
		tieredcache.WithL1(memcache.New()),
		tieredcache.WithInvalidation(bus),
	)
	defer func() { _ = node.Close() }()

	settle()
	_ = node.Set(ctx, "a", []byte("1"), time.Hour)
	_ = node.Set(ctx, "b", []byte("2"), time.Hour)
	if err := bus.Publish(ctx, cache.InvalidateAll); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		oa, _ := node.Has(ctx, "a")
		ob, _ := node.Has(ctx, "b")
		return !oa && !ob
	})
}

// Local Del must propagate to a peer node sharing the bus.
func TestLocalDeletePropagatesToPeer(t *testing.T) {
	ctx := context.Background()
	bus := cache.NewInProcInvalidation()

	a := tieredcache.New(tieredcache.WithL1(memcache.New()), tieredcache.WithInvalidation(bus))
	b := tieredcache.New(tieredcache.WithL1(memcache.New()), tieredcache.WithInvalidation(bus))
	defer func() { _ = a.Close() }()
	defer func() { _ = b.Close() }()

	settle()
	_ = b.Set(ctx, "k", []byte("v"), time.Hour)
	if ok, _ := b.Has(ctx, "k"); !ok {
		t.Fatal("precondition: b should hold k")
	}
	// a deletes k → bus → b drops its local copy.
	if err := a.Del(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { ok, _ := b.Has(ctx, "k"); return !ok })
}
