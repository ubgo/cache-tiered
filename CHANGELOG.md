# Changelog

All notable changes to `github.com/ubgo/cache-tiered` are documented here.
Format follows Keep a Changelog; the project follows SemVer (pre-GA in `v0.x`).

## [Unreleased]

### Added

- L1/L2/L3 composer implementing `cache.Cache`.
- Read promotion (deeper-tier hit copied into shallower tiers; `Promotions()`).
- `WriteThrough` (default) and `WriteOnlyL1` write modes; L1-authoritative
  errors with best-effort deeper tiers.
- `WithPerTierTTL` per-tier TTL overrides (1-based tier index).
- L1-authoritative `SetNX` / `Incr` / `Decr` with propagation; cascading
  `Del` / `DeleteByPrefix` / `Flush`.
- Passes the shared `github.com/ubgo/cache/cachetest` suite under `-race` for
  L1-only and L1+L2 compositions.
- `WithInvalidation`: subscribes to a `cache.Invalidation` bus to drop L1 on
  peer mutations and publishes on Del/DeleteByPrefix/Flush; `Close` stops the
  subscription cleanly.

[Unreleased]: https://github.com/ubgo/cache-tiered/commits/main
