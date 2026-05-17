# Read & write semantics

Snippets assume:

```go
ctx := context.Background()
t := tieredcache.New(
	tieredcache.WithL1(memcache.New()),
	tieredcache.WithL2(rediscache.New(rdb)),
)
defer t.Close()
```

## Read path

### Get

`Get(ctx, key)` probes L1→Ln. The **first hit is promoted into every shallower
tier** (using each destination's per-tier TTL) so the next read is faster. A
failed promote never fails the `Get`. Crucially, a non-`ErrNotFound` error from
any tier **aborts the probe and is returned** — a backend outage must not
masquerade as a cache miss (that would hide the outage and stampede the
origin).

Use cases:

- Hot keys served from nanosecond L1 after the first deeper-tier hit.
- Surfacing a Redis outage instead of silently stampeding the DB.

```go
v, err := t.Get(ctx, "user:42")
if errors.Is(err, cache.ErrNotFound) {
	// genuine miss in all tiers
} else if err != nil {
	// a tier is unhealthy — do NOT treat as a miss
}
```

### GetMulti

`GetMulti(ctx, keys)` is a per-key `Get` (so each key is promoted
individually); absent keys are omitted, a real error aborts.

```go
m, _ := t.GetMulti(ctx, []string{"a", "b"})
```

## Has & TTL

`Has` returns true at the first tier that has the key (a real error from any
tier is returned). `TTL` returns the first L1-and-down match's remaining
duration; all-miss → `cache.ErrNotFound`.

```go
ok, _ := t.Has(ctx, "k")
d, err := t.TTL(ctx, "k")
```

## Write path

### Set / SetMulti / Expire / Touch

These fan out across tiers up to the write-mode limit (`WriteThrough` → every
tier; `WriteOnlyL1` → L1 only). The **L1 error is authoritative** — it fails
the call; deeper-tier errors are best-effort and swallowed. Each tier's write
uses its per-tier TTL. `Expire` treats a deeper tier's `ErrNotFound` as fine
(that tier may legitimately not hold the key). `Touch` == `Expire(key, 1h)`.

```go
_ = t.Set(ctx, "user:42", []byte("v"), 5*time.Minute) // every tier (WriteThrough)
_ = t.SetMulti(ctx, map[string]cache.Item{"a": {Value: []byte("1")}})
_ = t.Expire(ctx, "user:42", time.Hour)
_ = t.Touch(ctx, "user:42")
```

## Atomic ops

### SetNX / Incr / Decr

L1 is the **source of truth**. `SetNX` makes the NX decision on L1; on success
the value is propagated to deeper tiers. `Incr`/`Decr` apply to L1, then
`mirrorCounter` converges every deeper tier to L1's authoritative value by
re-deriving each tier's delta (read via `Incr(key,0)`) rather than blindly
overwriting — keeping each tier's value atomic even if it drifted.
`Decr(k,d)` == `Incr(k,-d)`.

Use cases:

- Cross-pod-safe locks (L1 is in-memory; back it with a shared L1 if you need
  multi-pod NX — typically L1 is per-pod, so use this for in-pod dedup).
- Counters whose authoritative value is L1 but mirrored down for durability.

```go
ok, _ := t.SetNX(ctx, "lock:job", []byte("1"), 30*time.Second)
n, _ := t.Incr(ctx, "rl:ip", 1)
_, _ = t.Decr(ctx, "stock:sku9", 1)
```

## Delete & Flush

### Del / DeleteByPrefix / Flush

All **cascade to every tier** (regardless of write mode) and then publish on
the invalidation bus if `WithInvalidation` is set: `Del` publishes the keys;
`DeleteByPrefix` and `Flush` publish the `cache.InvalidateAll` sentinel (a
prefix/flush can't be expressed key-by-key, so peers drop their whole local
L1 — conservative but correct).

```go
_ = t.Del(ctx, "user:42")          // all tiers + publish "user:42"
_ = t.DeleteByPrefix(ctx, "user:") // all tiers + publish InvalidateAll
_ = t.Flush(ctx)                   // all tiers + publish InvalidateAll
```

## Iterate

`Iterate(ctx, opts)` scans the **deepest tier** (the most complete view of the
keyspace). Always `Close()` the iterator.

```go
it := t.Iterate(ctx, cache.IterateOpts{Prefix: "user:"})
defer it.Close()
for it.Next() { fmt.Println(it.Key()) }
```

## Lifecycle

### Ping / Close / Stats

`Ping` reports the first unhealthy tier. `Close` is idempotent: it cancels and
**waits for** the invalidation goroutine before closing every tier once
(returns the first close error). `Stats` sums hits/misses/sets/deletes/evictions
across all tiers and reports `Entries`/`Bytes` from the deepest tier.

```go
if err := t.Ping(ctx); err != nil { /* a tier is down */ }
s := t.Stats()
fmt.Printf("hit ratio %.3f, deepest entries %d\n", s.HitRatio(), s.Entries)
```

## Promotions

### Promotions

`func (t *Cache) Promotions() int64`

What it is: how many times a deeper-tier hit was copied upward. Pure
observability counter.

Use cases:

- Track how effective L1 warming is (high promotions early, tapering off).
- Alert if promotions stay high (L1 too small / TTL too short).

```go
log.Printf("upward promotions so far: %d", t.Promotions())
```
