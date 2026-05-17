# Construction & options

### New

`func New(opts ...Option) *Cache`

What it is: builds the tiered cache. **Panics** if no L1 is configured — a
tiered cache without its hot tier is always a wiring bug, surfaced at startup,
never softened to a runtime error. Nil tiers (gaps) are compacted away.

Use cases:

- Local L1 in front of a shared L2 (the canonical setup).
- Three tiers: in-memory → Redis → durable Postgres.

```go
package main

import (
	"time"

	memcache "github.com/ubgo/cache-mem"
	rediscache "github.com/ubgo/cache-redis"
	tieredcache "github.com/ubgo/cache-tiered"
	"github.com/redis/go-redis/v9"
)

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	t := tieredcache.New(
		tieredcache.WithL1(memcache.New()),
		tieredcache.WithL2(rediscache.New(rdb)),
		tieredcache.WithPerTierTTL(map[int]time.Duration{
			1: 30 * time.Second, // bound cross-pod staleness of L1
			2: 5 * time.Minute,
		}),
	)
	defer t.Close()
}
```

### Cache

`type Cache struct { ... }`

What it is: the composer. `tiers[0]` is L1 and is authoritative for atomic ops
and for the error returned by writes; deeper-tier write/promote errors are
intentionally swallowed so a cold backend cannot break the hot path.

```go
var generic cache.Cache = tieredcache.New(tieredcache.WithL1(memcache.New()))
```

### Option

`type Option func(*Cache)` — the functional-option type used by `New`.

### WriteMode

`type WriteMode int`

What it is: controls how writes propagate across tiers. Two values:
`WriteThrough`, `WriteOnlyL1`.

```go
t := tieredcache.New(
	tieredcache.WithL1(mem), tieredcache.WithL2(redis),
	tieredcache.WithWriteMode(tieredcache.WriteOnlyL1),
)
```

### WriteThrough

`const WriteThrough WriteMode = iota` (the **default**)

What it is: every `Set`/`SetMulti`/`Expire`/`Touch` writes to **every** tier.

Use cases:

- Keep deeper tiers fully populated so they survive an L1 restart/eviction.

```go
t := tieredcache.New(
	tieredcache.WithL1(mem), tieredcache.WithL2(redis),
	tieredcache.WithWriteMode(tieredcache.WriteThrough),
)
```

### WriteOnlyL1

`const WriteOnlyL1 WriteMode`

What it is: writes go only to L1; deeper tiers are not written on `Set` (they
fill lazily on a later read miss being promoted, depending on your usage).

Use cases:

- Write-heavy hot path where the deeper tier should only hold read-promoted
  values (avoid write amplification to Redis/DB on every `Set`).

```go
t := tieredcache.New(
	tieredcache.WithL1(mem), tieredcache.WithL2(redis),
	tieredcache.WithWriteMode(tieredcache.WriteOnlyL1),
)
```

### WithL1

`func WithL1(c cache.Cache) Option`

What it is: sets the fastest, closest tier. **Required** — `New` panics
without it.

Use cases:

- An in-process `cache-mem` for nanosecond local hits.

```go
tieredcache.WithL1(memcache.New(memcache.WithMaxEntries(100_000)))
```

### WithL2

`func WithL2(c cache.Cache) Option`

What it is: sets the second tier (typically shared across pods).

```go
tieredcache.WithL2(rediscache.New(rdb))
```

### WithL3

`func WithL3(c cache.Cache) Option`

What it is: sets the third tier (typically durable).

```go
tieredcache.WithL3(pgcache.New(db))
```

### WithPerTierTTL

`func WithPerTierTTL(m map[int]time.Duration) Option`

What it is: overrides the TTL used when writing/promoting into a given
**1-based** tier (L1 == key `1`). Tiers absent from the map use the
caller-supplied TTL. Promotion writes use the destination tier's configured
TTL, not the original.

Use cases:

- Short L1 TTL to bound cross-pod staleness while L2 keeps a long TTL.
- Tighter durable-tier TTL than the volatile tiers.

```go
tieredcache.WithPerTierTTL(map[int]time.Duration{
	1: 30 * time.Second, // L1
	2: 10 * time.Minute, // L2
})
```

### WithWriteMode

`func WithWriteMode(m WriteMode) Option`

What it is: selects the write-propagation strategy (default `WriteThrough`).

```go
tieredcache.WithWriteMode(tieredcache.WriteOnlyL1)
```

### WithInvalidation

`func WithInvalidation(inv cache.Invalidation) Option`

What it is: wires a cross-process invalidation bus. The tiered cache then
(a) drops the affected key from **L1 only** when any peer publishes, and
(b) publishes on `Del`/`DeleteByPrefix`/`Flush` so peers drop theirs. This lets
L1 run a longer TTL safely. Delivery is best-effort. The subscribe goroutine is
cancelled and waited on by `Close` before tiers are closed.

Use cases:

- Multi-pod deployment with a local L1 that must not serve stale data after
  another pod mutates a key.

```go
import rediscache "github.com/ubgo/cache-redis"

inv := rediscache.NewInvalidation(rdb, "cache:invalidate")
t := tieredcache.New(
	tieredcache.WithL1(memcache.New()),
	tieredcache.WithL2(rediscache.New(rdb)),
	tieredcache.WithInvalidation(inv),
)
defer t.Close() // cancels + waits for the subscribe goroutine first
```
