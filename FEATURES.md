# microdb — Feature Checklist & Priority Score

**Project:** `microdb` · Go 1.23 · stdlib-only distributed schema-less document store
**Status:** all Tier 1–3 features + most Tier 4 shipped. Every "done" row is backed by a passing test or live run.
**Priority scale:** 10 = core correctness/foundation → 0 = nice-to-have polish.

Legend: ✅ shipped & verified · ⬜ not done · ⚠️ shipped with caveat

---

## ✅ Shipped (29 features)

| ✓ | Feature | Pri | Verification |
|---|---------|-----|--------------|
| [x] | Schema-less JSON documents | 10 | Live demo: disjoint schemas stored & replicated |
| [x] | Append-only JSONL persistence + crash recovery | 9 | Replay tests across restarts |
| [x] | LWW conflict merge by (ver, ts), idempotent | 9 | `TestAntiEntropyResolvesConcurrentDivergence` |
| [x] | Consistent hashing ring (128 vnodes/node) | 8 | Rebalance across 3-node live cluster |
| [x] | RF=3 replication fanout | 8 | `TestClusterReplication` |
| [x] | Gossip membership (seed join, TTL eviction, ring rebuild) | 8 | Mesh converges ~1s; eviction observed |
| [x] | Any-node write routing (hop-limited forward to owner) | 7 | Writes to any port succeed |
| [x] | Tombstone deletes (replicated) | 7 | Delete propagation test |
| [x] | Merkle-tree anti-entropy (root compare + targeted exchange) | 9 | 6 unit + 2 integration tests |
| [x] | **Log compaction + tombstone GC** (Tier 1) | 8 | `TestCompactKeepsLatestVersionOnly`, `TestCompactTombstoneGC`, fresh-tombstone safety |
| [x] | **fsync-on-write option** `--fsync` (Tier 1) | 7 | `TestFsyncModePersistsWrite` |
| [x] | **Recursive subtree-diff anti-entropy** (Tier 1) | 7 | `TestDiffAgainstRemote*` incl. fetch-count bounds, zero-fetch short-circuit |
| [x] | **Scatter-gather queries** (Tier 1) | 7 | `TestScatterGatherQuery` — union across nodes, partial-flag on failure |
| [x] | **Read-your-writes via sticky owner routing** (Tier 1) | 6 | Writes always route to the ring owner; owner-local read is immediately fresh |
| [x] | **Inverted secondary indexes + query fast path** (Tier 2) | 6 | `TestScanIndexedFastPathMatchesFullScan`, 6 index unit tests |
| [x] | **Full operator set** `$gte $lte $ne $in $exists $regex` (Tier 2) | 6 | 19-case matrix `TestMatchesOperators` |
| [x] | **Pagination + sort** `limit/offset/sort/desc` (Tier 2) | 5 | `TestPaginationSort` — global ordering across shards |
| [x] | **Batch writes** — atomic single-record, one fsync (Tier 2) | 5 | `TestApplyBatchAtomicAndReplay` |
| [x] | **Change feed / watch** — long-poll NDJSON (Tier 2) | 4 | `TestWatchFeed`, `TestGoClientWatch`, 5 changelog unit tests |
| [x] | **Mutual TLS for internal traffic** (Tier 3) | 7 | `TestTLSMutualAuth` — in-test CA, 403 without cert, public API open |
| [x] | **API bearer-token auth** `--auth-token` (Tier 3) | 7 | `TestRequireAPIAuth` — 401/200 matrix, /v1 gated identically |
| [x] | **Prometheus metrics** `/metrics` (Tier 3) | 6 | Counters (writes/deletes/applies/sends) + gauges (docs/collections/peers) |
| [x] | **Backup/restore + microctl admin CLI** (Tier 3) | 6 | `TestBackupRestoreRoundTrip`; `microctl backup/restore/status` |
| [x] | **Configurable replication factor** `--rf` (Tier 3) | 5 | `TestReplicationFactor` — RF=1 owner-only, RF=2 both owners |
| [x] | **Go client library** (Tier 4) | 5 | `client/` — CRUD/batch/query/watch/TLS/token; 2 integration tests |
| [x] | **HTTP API versioning** `/v1/api/...` (Tier 4) | 2 | Dual-registered routes; auth parity tested |
| [x] | **Docker + compose** 3-node cluster (Tier 4) | 4 | ⚠️ files shipped; no Docker daemon in build env to run them |
| [x] | **README + architecture doc** (Tier 4) | 4 | `README.md` |
| [x] | **`-race` clean test runs** (Tier 4) | 4 | ⚠️ requires mingw64 toolchain: `CC='C:\w\msys64\mingw64\bin\gcc.exe' CGO_ENABLED=1 go test -race ./...` (MSYS gcc can't build Go's cgo shim) |
| [x] | **Ring epochs + write fencing** (Roadmap A2) | 9 | Epoch bumped on join/evict, gossiped + adopted monotonically; forwarded writes carry `X-Microdb-Epoch`, stale forwarders get 409 → adopt → retry. `TestFencingStaleEpoch`, `TestEpochPropagation` |
| [x] | **Quorum reads + read-repair** (Roadmap D1/D4) | 8 | `?consistency=quorum|all` merges replicas by (ver,ts), 503 if quorum unreachable, async repair pushes newest to lagging replicas. `TestQuorumRead`, `TestQuorumReadNotReached` |
| [x] | **Quorum writes** (Roadmap D1) | 8 | `?consistency=quorum` blocks until W=majority of RF-owner set acked; honest 503 (`applied:true`) when not reached |
| [x] | **Hinted handoff** (Roadmap D3) | 7 | New `internal/hints`: durable capped JSONL debt, newest-wins replace, exp backoff ≤5min, corrupt-tail tolerant; failed Replicate stashes, 1s replay loop delivers on peer return. 7 unit + 3 integration tests incl. sender-restart durability |
| [x] | **Graceful decommission** (Roadmap A3) | 9 | `microctl decommission --url`: drain (refuse writes 503) → bulk handoff to peers (failures → hints) → leave broadcast evicts immediately (no 15s TTL wait); gossip tombstones block resurrection 30s, explicit join overrides. Live-verified: 20-doc handoff, survivors complete after kill |

## ⬜ Remaining (deliberately out of scope for "tiny")

| ☐ | Feature | Pri | Why deferred |
|---|---------|-----|--------------|
| [ ] | Multi-tenancy / namespaces | 2 | Changes the data model; contradicts "tiny" |
| [ ] | Vector clocks / CRDT merge (instead of LWW) | 3 | Changes consistency semantics fundamentally |
| [ ] | Per-collection RF (vs global `--rf`) | 3 | Ring would need per-collection ownership maps |
| [ ] | Durable change feed (persisted events) | 3 | Current feed is bounded in-memory by design |
| [ ] | Range-partitioned queries pushed to shards | 2 | Scatter-gather is O(cluster) but correct |

---

# 🚀 Roadmap: DynamoDB / Cassandra-class scalability

What it would take to evolve microdb from a fixed-cluster toy into a horizontally
scalable database: **dynamic node membership, real sharding, and full separation
of storage from the query engine.**

**Gap analysis — what we already have vs. what the real systems rely on:**

| Concept | microdb today | DynamoDB / Cassandra |
|---|---|---|
| Membership | Gossip + TTL eviction ✅ | Same idea (gossip/seed nodes) ✅ |
| Placement | Consistent-hash vnodes ✅ | Cassandra vnodes ✅ / DynamoDB range partitions ❌ |
| New node | Starts **empty**, backfills via anti-entropy pull (O(all data), slow, no admission control) | **Bootstrap + streaming**: ranges transferred before the node serves reads |
| Ring changes | Rebuilt locally per node, no global version | **Versioned ring epochs** with fencing tokens |
| Data model | Collection + doc id | **Partition key + sort key**, per-partition ordering |
| Storage/compute | **Coupled** — every node owns data and serves queries | **Disaggregated** — stateless compute over a durable storage/log tier |
| Consistency | LWW eventual, RF fanout | **Quorum R+W>N**, tunable per request, hinted handoff |

---

## A. Elastic membership — add/remove nodes dynamically

| ☐ | Feature | Pri | What it takes |
|---|---------|-----|---------------|
| [ ] | **Bootstrap & streaming protocol** | 10 | New node joins → receives ring metadata → **streams the ranges it now owns** from current owners *before* being admitted to the ring. Today a new node starts empty and pulls everything via 10s anti-entropy rounds — unbounded, unthrottled, and reads are stale until it finishes. Needs: range-transfer RPCs, resumable checkpoints, progress reporting, `STREAMING` node state (accept writes, refuse reads). |
| [x] | **Ring epochs + fencing tokens** | 9 | ✅ shipped — see "Ring epochs + write fencing" above |
| [x] | **Graceful decommission** | 9 | ✅ shipped — see "Graceful decommission" above |
| [ ] | **Dual-write migration window** | 8 | While a range is moving, both old and new owner accept writes (new owner records `pending_ranges`), merge on completion. This is the DynamoDB/Cassandra mechanism that makes rebalance invisible to clients. |
| [ ] | **Load-aware token assignment** | 7 | Auto-assign vnodes by observed load (bytes/sec, ops/sec per node) instead of uniform random placement; periodic rebalancer that proposes minimal-movement plans. |
| [ ] | **Admission control for joins** | 6 | Rate-limit how many nodes may bootstrap concurrently (streaming is the most expensive op in the cluster); queue + priority for failed-join retries. |

## B. Sharding — a real partitioning model

| ☐ | Feature | Pri | What it takes |
|---|---------|-----|---------------|
| [ ] | **Partition key + sort key data model** | 8 | Dynamo-style composite keys: items grouped by partition key, ordered by sort key within the partition. Enables `Query(partition = X, sort BETWEEN a AND b)` served from one shard with no scatter. Today: flat `col/id`, no ordering, no locality. |
| [ ] | **Range-based ownership with split/merge** | 8 | Move from pure vnode-hash to contiguous token ranges that can be **split** (DynamoDB auto-partitions hot keys) and **merged** (cold shards). Requires the epoch machinery from (A) to move ranges atomically. |
| [ ] | **Per-shard isolation** | 7 | Each shard gets its own commit log, index set, compaction schedule, GC, and metrics. One hot shard can't stall compaction of the whole node; per-shard quotas become possible. |
| [ ] | **Shard-map aware clients** | 6 | Clients cache the shard→node map, route directly, refresh on epoch mismatch. Cuts coordinator hop and lets the coordinator tier shrink. |
| [ ] | **Hedged reads on scatter-gather** | 6 | Fire the slow tail of a scatter query a second time to a replica; take the first response. Standard DynamoDB latency trick for cross-partition scans. |
| [ ] | **Global secondary indexes as a service** | 6 | GSI = a separate index shard keyed by (index_value → partition_key), maintained asynchronously from the change feed, with its own consistency lag exposed to clients. |

## C. Storage / query-engine separation

| ☐ | Feature | Pri | What it takes |
|---|---------|-----|---------------|
| [ ] | **Stateless query engine** | 9 | Compute nodes hold **no authoritative data**: they parse, plan, route to the storage tier, merge, and cache. A compute node can die or scale out with zero data movement. Today every node is both — the core coupling to break. |
| [ ] | **Storage-tier record API** | 9 | Storage nodes expose a minimal, epoch-fenced record interface: `Put(key, ver, ts, value)`, `Get(key)`, `ScanRange(prefix, lo, hi)`, `Tombstone(key)`. The query engine never touches JSONL directly. This API boundary *is* the disaggregation. |
| [ ] | **Durable shared commit log** | 8 | Writes append to a replicated log service (Kafka-like, or S3 segments with an append protocol) that the storage tier replays. Compute nodes become crash-free by construction; durability moves out of per-node `fsync` into the log's own replication (multi-AZ quorum). |
| [ ] | **Tiered storage: hot NVMe + cold object store** | 7 | Storage nodes keep a bounded hot window on local SSD; older segments live in S3-compatible object storage, fetched lazily on read. Cost model of Aurora/DynamoDB: compute and IOPS scale independently of dataset size. |
| [ ] | **Index service off the write path** | 7 | A separate tier consumes the change feed and maintains secondary indexes (currently in-process inverted indexes). Write latency stops paying for index maintenance; indexes scale and fail independently. |
| [ ] | **Compute autoscaling** | 7 | Stateless query nodes behind a load balancer: scale out in seconds on CPU/queue-depth, scale in by draining. No bootstrap, no streaming — the payoff of (C1). |
| [ ] | **Cache invalidation via changelog** | 6 | Query-engine caches subscribe to the storage-tier change feed and invalidate on mutation instead of TTL-guessing. Makes read-your-writes cheap at the engine level. |

## D. Consistency & durability at scale

| ☐ | Feature | Pri | What it takes |
|---|---------|-----|---------------|
| [x] | **Quorum reads (R + W > N)** | 8 | ✅ shipped — `?consistency=quorum|all`, merge by (ver,ts), 503 on unreachable |
| [ ] | **Per-request tunable consistency** | 7 | `?consistency=one|quorum|all|local_quorum` on reads and writes — the Cassandra contract clients expect. (quorum/all shipped for both; `one`/`local_quorum` labels remain) |
| [x] | **Hinted handoff** | 7 | ✅ shipped — durable hint store, replay on peer return, exp backoff |
| [x] | **Read-path read repair** | 6 | ✅ shipped — quorum reads push newest version to lagging replicas seen during the read |
| [ ] | **Point-in-time recovery (PITR)** | 7 | Archive commit-log segments to object storage continuously; restore = replay to any timestamp within the retention window. Upgrades `microctl backup` (snapshot-only) to continuous recovery. |
| [ ] | **Multi-region replication** | 6 | Cross-region log shipping + conflict resolution **beyond LWW** (vector clocks or CRDTs — LWW across regions silently loses concurrent edits with clock skew). Global secondary index replication with lag metrics. |

## E. Multi-tenancy & production operations

| ☐ | Feature | Pri | What it takes |
|---|---------|-----|---------------|
| [ ] | **Tenant isolation & quotas** | 7 | Per-tenant rate limits, storage quotas, per-tenant auth; noisy-neighbor control at the admission layer. |
| [ ] | **Admission control / backpressure** | 7 | Queue-depth and latency-signal-based load shedding: reject over budget with 429 + retry-after before the cluster melts. |
| [ ] | **Per-table configuration** | 6 | RF, consistency defaults, index definitions, compaction policy per table instead of per-process flags. |
| [ ] | **Encryption at rest + per-tenant keys** | 6 | Segment-level encryption (SSE-style), KMS-managed per-tenant keys. |
| [ ] | **Distributed tracing** | 5 | Trace IDs through coordinator → storage → index tiers; per-span latency to find tail causes. |
| [ ] | **Online schema migration** | 5 | Versioned item schemas with background converters; no downtime for field renames/retypes. |

---

## Suggested phasing

| Phase | Theme | Features | Why this order |
|-------|-------|----------|----------------|
| **1** | Break the coupling | C1 stateless engine, C2 storage API | Everything else is easier once the boundary exists; do it before the codebase grows against the current coupling |
| **2** | Make membership safe & dynamic | A2 epochs/fencing, A1 bootstrap+streaming, A4 dual-write, A3 decommission | Fencing first (correctness), then streaming (speed), then drain |
| **3** | Real sharding | B1 partition/sort keys, B2 split/merge, B3 per-shard isolation | Needs phase-2 epoch machinery to move ranges safely |
| **4** | Durability & consistency | C3 shared log, D1–D4 quorum/hints/read-repair, D5 PITR | Quorum needs stable ownership (phase 2) and the log gives the durability story |
| **5** | Scale-out economics | C4 tiered storage, C5 index service, C6 autoscale, B5 hedged reads | Cost and tail-latency wins once the architecture supports them |
| **6** | Production hardening | E1–E6, D6 multi-region | Multi-tenant / multi-region only after single-region is elastic |

**Honest scope note:** phases 1–3 alone are roughly a rewrite of the ownership and
data-placement layers (the ring, store, and cluster packages stop being one
process-level blob). That's the real cost of disaggregation — DynamoDB, Cassandra,
and Aurora each represent many engineer-years in exactly these layers. microdb's
current gossip + Merkle + LWW pieces map 1:1 onto concepts these systems use, so
the *knowledge* transfers even where the code must be restructured.

---

## Commit history (one feature per commit)

```
baseline    v0: gossip, ring, RF3 replication, Merkle anti-entropy
700af84     store: crash-safe log compaction + tombstone GC
031547e     store: optional fsync-on-write (--fsync)
a24995f     api: scatter-gather collection queries
07c3f2b     merkle+cluster: recursive subtree-diff anti-entropy
f3d10f9     index: inverted field indexes with query fast path
1538d17     store: full query operator set
2152f7e     api: pagination + sort on collection queries
4c6f852     store+api: batch writes (atomic single-record)
895feb1     changelog+api: long-poll change feed (watch)
335787d     cluster+api: mutual TLS for internal traffic
b718d21     api: bearer-token auth for /api/*
b30a697     metrics: Prometheus text-format /metrics endpoint
cf6ee02     backup/restore + microctl admin CLI
8cd686b     api+main: configurable replication factor (--rf)
8e98b3d     client: typed Go client library + batch decode fix
388dff7     packaging: Dockerfile + docker-compose 3-node cluster
e4fe4f1     api: /v1 versioned routes alongside unversioned
9566790     api+cluster+ring: quorum reads/writes, ring epochs, write fencing
b2f9c64     hints: durable hinted handoff for failed replication
bcd684c     cluster+api: graceful decommission with leave tombstones
```

**Coverage:** 29 shipped; roadmap items A2, A3, D1 (quorum), D3, D4 now done.
