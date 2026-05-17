# Contributing to ubgo/cache-tiered

Thanks for helping improve the L1/L2/L3 composer for `github.com/ubgo/cache`.

## Local gate (must be green before every commit / PR)

```sh
gofmt -w .
go build ./...
go test -race -count=1 ./...
golangci-lint run ./...
```

Or `task check`. CI is identical. Not done until **0 failures, 0 lint issues** (`golangci-lint`: revive, staticcheck, govet, errcheck, gocritic, misspell, unconvert, ineffassign, unused — see `.golangci.yml`). The race detector matters here: this module owns a background invalidation goroutine and shared counters.

## Conformance contract

This composer implements `cache.Cache` and **must keep passing** `github.com/ubgo/cache/cachetest`, in both L1-only and L1+L2 configurations:

```go
func TestConformanceL1Only(t *testing.T) { cachetest.Run(t, /* WithL1(mem) */) }
func TestConformanceL1L2(t *testing.T)   { cachetest.Run(t, /* WithL1(mem), WithL2(mem) */) }
```

`cachetest.Run` is the executable contract. Composer-specific invariants the suite + `tiered_test.go` enforce:

- `Get` returns `(nil, cache.ErrNotFound)` only when **every** tier misses; a non-`ErrNotFound` error from any tier aborts the probe.
- Read promotes the first hit into every shallower tier; `Promotions()` increments.
- Write-through hits all tiers; L1 error is authoritative, deeper-tier errors are best-effort.
- `WriteOnlyL1` writes L1 only. `WithPerTierTTL` applies the per-tier override on write and promote.
- `SetNX`/`Incr`/`Decr`: L1 decides, result mirrored down.
- `Del`/`Flush`/`DeleteByPrefix` cascade to every tier and publish on the invalidation bus.
- `Close()` is idempotent, stops the invalidation goroutine, closes each tier once.

## Docker-free tests (mem + mem)

`tiered_test.go` composes two in-process `github.com/ubgo/cache-mem` instances as L1/L2 — no Redis, no Docker. TTL assertions use real `time.Sleep` against the mem backend's wall clock. The invalidation lifecycle is exercised in `invalidation_test.go` with an in-process bus. Keep tests backend-free; do not introduce a networked dependency into the gate.

## Local dependency (`replace`)

`go.mod` carries `replace github.com/ubgo/cache => ../cache` (sibling, not yet tagged). **Do not edit `go.mod`, `go.sum`, `LICENSE`, `NOTICE`, or `.gitignore`** in a feature change; the `replace` is removed at release time.

```
ubgo/
  cache/          # contract + cachetest
  cache-mem/      # in-process backend used as L1/L2 in tests
  cache-tiered/   # this module (replace -> ../cache)
```

## Doc-comment style

- Every exported symbol has a doc comment starting with its name (`revive`).
- Comments explain **why**: why L1 is authoritative, why deeper-tier errors are best-effort, why `New` panics without L1, the invalidation subscribe/cancel lifecycle. Preserve these on refactor.
- `ctx` stays named even when a tier wrapper does not consult it; `.golangci.yml` excludes that revive warning. Never rename to `_`.
- Keep `doc.go` accurate — it is the godoc landing page and states the read/write/cascade contract.

## Pull requests

1. Keep the gate green (race detector included).
2. Add/extend a test for any behaviour change (conformance first, then targeted promote/write-mode/invalidation test).
3. Update `README.md` / `CHANGELOG.md` on public behaviour changes.
4. One logical change per PR.
