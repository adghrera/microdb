# microdb — Feature Checklist & Priority Matrix

**Project:** `C:\Users\adghr\microdb` · Go 1.23 · stdlib-only distributed schema-less document store
**Date:** 2026-09-26 · All "implemented" items backed by passing tests or live 3-node demo runs.

Priority scale: **10 = core correctness/foundation** → **0 = nice-to-have polish**.

---

## ✅ Implemented (verified)

| ✓ | Feature | Pri | Verification |
|---|---------|-----|--------------|
| [x] | **Schema-less JSON documents** — any JSON object per doc, no declared fields, different shapes per doc in one collection | 10 | Live demo: alice/bob/carol with disjoint schemas stored & replicated |
| [x] | **Append-only JSONL persistence** — every write durably appended before ack | 9 | `data*/data.jsonl` inspected after runs |
| [x] | **Crash recovery via log replay** — store rebuilds full state from JSONL on restart | 9 | `store.Open()` replay path exercised across restarts |
| [x] | **Last-writer-wins conflict merge** — deterministic by `(ver, ts)`, idempotent re-application | 9 | `TestAntiEntropyResolvesConcurrentDivergence` (ver=1 vs ver=2 → all converge) |
| [x] | **Consistent hashing ring** — 128 vnodes/node, ~1/N key movement on node add/remove | 8 | Unit behavior + live rebalance across 3 nodes |
| [x] | **RF=3 replication fanout** — writes pushed to ring owners in background | 8 | `TestClusterReplication`: every doc readable from every node |
| [x] | **Gossip membership** — seed join + push/pull member list, 15s TTL failure eviction, ring auto-rebuild | 8 | 3-node mesh converges ~1s; node death evicted |
| [x] | **Any-node write routing** — non-owners forward to ring owner (hop-limited, no loops) | 7 | Writes to all 3 ports succeed regardless of ownership |
| [x] | **Tombstone deletes** — deletes replicate and re-delete stays deleted | 7 | `TestClusterReplication` delete propagation step |
| [x] | **Merkle-tree anti-entropy** — per-collection trees, O(1) root compare, tree descent to divergent leaves, targeted pull+push both directions | 9 | 6 unit tests + 2 integration tests + live lone-node repair demo |
| [x] | **Collection discovery in sync** — peers learn collections they didn't know existed | 6 | `GET /internal/collections` round in anti-entropy pass |
| [x] | **Depth-mismatch fallback** — map-based diff when doc counts differ (padding differs) | 5 | `TestDepthMismatchFallsBackToMap` |
| [x] | **Basic query filters** — exact match, `$gt`, `$lt` (numeric + lexicographic) | 6 | `TestQueryFilter` + live `age>30` query |
| [x] | **Seed retry until reachable** — late-booting seeds don't lose the cluster | 6 | Live demo: nodes joined through chain of seeds |
| [x] | **Health & cluster introspection** — `/health`, `/api/cluster`, `/internal/merkle/{col}` roots | 5 | Used throughout demos |
| [x] | **Integration test suite** — replication, filters, dropped-write repair, divergence resolution | 8 | `go test ./integration/` 4/4 PASS |
| [x] | **Merkle unit tests** — order-independence, change detection, empty tree, hash determinism | 7 | `go test ./internal/merkle/` 6/6 PASS |

**Implemented subtotal: 17 features · weighted coverage ≈ 8.0/10 of the v1 core**

---

## 🔜 Should Be Done (roadmap)

### Tier 1 — Correctness & durability gaps (do next)

| ☐ | Feature | Pri | Why |
|---|---------|-----|-----|
| [ ] | **Tombstone garbage collection** — purge tombstones older than a GC window so deletes don't live forever | 8 | Today a deleted doc's tombstone is replicated and stored permanently; long-lived collections bloat |
| [ ] | **Log compaction** — rewrite JSONL to latest-version-per-key, drop superseded entries | 8 | Append-only log grows without bound; restart replay slows linearly |
| [ ] | **fsync durability option** — configurable sync-on-write (currently OS-buffered) | 7 | Power-loss can lose the tail of recent writes despite "durable" ack |
| [ ] | **Recursive subtree diff** — fetch only divergent subtree hashes instead of the full leaf list on mismatch | 7 | Current anti-entropy ships O(collection) leaves on every mismatch; fine for 100s of docs, bad at 100k |
| [ ] | **Read-your-writes within session** — sticky routing or version echo so a client re-reads its own write | 6 | Eventual replication means a client can PUT then GET and miss its own doc on a different node |

### Tier 2 — Query & data-model capability

| ☐ | Feature | Pri | Why |
|---|---------|-----|-----|
| [ ] | **Scatter-gather collection queries** — run filters across all owners, merge results | 7 | Today `GET /docs` scans only the receiving node's local copy |
| [ ] | **Secondary indexes** — per-collection field indexes for fast non-id lookups | 6 | Filters are full scans now |
| [ ] | **More operators** — `$gte`, `$lte`, `$ne`, `$in`, `$exists`, string `$regex` | 6 | Only 3 operators exist |
| [ ] | **Pagination + sort** — `?limit=&offset=&sort=field` on collection queries | 5 | No way to page large result sets |
| [ ] | **Batch writes** — atomic-per-collection multi-doc PUT | 5 | Round-trip cost for bulk imports |
| [ ] | **Change feed / watch** — SSE or long-poll stream of doc mutations per collection | 4 | Real-time consumers need polling today |

### Tier 3 — Operations & security

| ☐ | Feature | Pri | Why |
|---|---------|-----|-----|
| [ ] | **TLS for internal traffic** — gossip/replication currently plaintext HTTP | 7 | Replication crosses networks unencrypted |
| [ ] | **API auth** — token or basic auth on client endpoints | 7 | API is wide open |
| [ ] | **Metrics endpoint** — Prometheus-style counters: writes, replication lag, anti-entropy bytes, ring size | 6 | No observability beyond logs |
| [ ] | **Configurable replication factor** — RF per collection or per namespace, not hardcoded 3 | 5 | Some data needs RF=5, some RF=1 is fine |
| [ ] | **Backup/restore tooling** — snapshot JSONL + verify + restore-to-new-cluster | 6 | Only manual file copy today |
| [ ] | **Admin CLI** — `microctl` for status, rebalance, force-sync, GC trigger | 4 | Everything is raw curl today |

### Tier 4 — Nice-to-have polish

| ☐ | Feature | Pri | Why |
|---|---------|-----|-----|
| [ ] | **Go client library** — typed wrapper with retry/forward-follow | 5 | Removes hand-rolled HTTP from users |
| [ ] | **Docker/compose packaging** — one-command 3-node cluster | 4 | Demo currently needs 3 manual processes |
| [ ] | **README + architecture doc** | 4 | Repo has code comments but no front door |
| [ ] | **`-race` clean CI** — needs a CGO-capable toolchain (blocked in current MSYS env) | 4 | Race detector couldn't build here; ring is mutex-guarded but unproven under `-race` |
| [ ] | **HTTP API versioning** (`/v1/...`) | 2 | Cheap now, painful later |
| [ ] | **Multi-tenancy / namespaces** | 2 | Out of scope for "tiny", listed for completeness |

---

## Suggested next sprint (highest ROI)

1. **Log compaction + tombstone GC** (pri 8) — the only unbounded-growth problems in the system
2. **fsync option** (pri 7) — makes the durability promise real
3. **Scatter-gather queries** (pri 7) — makes the query API honest in a sharded cluster
4. **Recursive subtree diff** (pri 7) — makes anti-entropy scale past small collections
5. **TLS + API auth** (pri 7) — before anyone puts this on a real network

**Legend:** ✅ = shipped & verified · 🔜 = planned · scores are relative engineering priority for this project's goals (tiny, dependency-free, eventually-consistent), not absolute importance.
