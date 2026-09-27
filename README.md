# microdb

A tiny distributed, schema-less document store in pure Go — **stdlib only, zero dependencies**.
Built to show how the core ideas of Dynamo-style databases actually work: consistent hashing,
gossip membership, replication, Merkle-tree anti-entropy, and last-writer-wins conflict resolution.

```
PUT    /api/collections/users/docs/alice   {"name":"Alice","tags":["admin"]}
GET    /api/collections/users/docs/alice
GET    /api/collections/users/docs?filter={"age":{"$gt":30}}&sort=age&limit=10
```

## Quick start

```bash
go build -o bin/microdb ./cmd/microdb
go build -o bin/microctl ./cmd/microctl

# three-node cluster
./bin/microdb --addr :8001 --dir ./data1 &
./bin/microdb --addr :8002 --dir ./data2 --join http://127.0.0.1:8001 &
./bin/microdb --addr :8003 --dir ./data3 --join http://127.0.0.1:8002 &

# write to any node, read from every node
curl -X PUT localhost:8002/api/collections/users/docs/alice \
  -H 'Content-Type: application/json' -d '{"name":"Alice","age":31}'
curl localhost:8003/api/collections/users/docs/alice

# or with docker
docker compose up -d
```

## Architecture

```
┌───────────────────────────── node ─────────────────────────────┐
│  HTTP API (api)                                               │
│   ├─ writes: own? apply+fanout : forward to owner (1 hop)     │
│   ├─ queries: scatter-gather across all members, merge by LWW │
│   └─ watch: long-poll NDJSON change feed                      │
│                                                              │
│  Store (store)          Index (index)     Changelog (changelog)│
│   ├─ in-memory map       ├─ field→value→ids  └─ bounded events │
│   ├─ append-only JSONL   ├─ exact fast path     per collection │
│   └─ LWW merge (ver,ts) └─ rebuilt on replay                 │
│                                                              │
│  Cluster (cluster)                                            │
│   ├─ gossip: 1s push/pull member list, 15s TTL eviction       │
│   ├─ replication: fire-and-forget push to ring owners (RF)    │
│   └─ anti-entropy: 10s Merkle subtree diff + targeted exchange│
│                                                              │
│  Ring (ring): consistent hash, 128 vnodes/node, RF owners     │
└───────────────────────────────────────────────────────────────┘
```

### Consistency model
- **Eventual consistency.** Writes ack at the owner; replicas converge via fast-path
  replication (ms) and anti-entropy (≤10s) if the fast path dropped anything.
- **Conflicts resolve last-writer-wins** by `(ver, ts)` — deterministic and idempotent,
  every node converges to the same state regardless of delivery order.
- **No quorum reads, no transactions.** Single-writer-per-key by ring ownership.

### Why a Merkle tree?
Steady state, two replicas compare one 64-hex-char root per collection: equal roots
mean identical data, nothing else crosses the wire. When roots differ, the diff descends
only divergent branches — O(k·depth) tiny hash fetches for k changed docs — then
exchanges just those documents, both directions.

### Failure handling
- Node down: peers evict it after 15s of silence; the ring rebuilds; writes to keys
  it owned now route to the next owners. When it returns, anti-entropy reconciles.
- Partition: each side accepts writes locally (owner routing); heals on merge by LWW.
- Crash: append-only log + replay restores full state; compaction keeps the log small.

## API reference

| Method | Path | Notes |
|---|---|---|
| `PUT` | `/api/collections/{col}/docs/{id}` | Upsert. Any JSON object. Forwarded to owner if needed. `?consistency=quorum` blocks until W replicas ack. |
| `GET` | `/api/collections/{col}/docs/{id}` | Read. `?consistency=quorum\|all` for strong reads (merge replicas + read-repair). |
| `DELETE` | `/api/collections/{col}/docs/{id}` | Tombstone delete, replicated. |
| `POST` | `/api/collections/{col}/docs/batch` | `{"docs":{"id":{...},...}}` — one atomic record. |
| `GET` | `/api/collections/{col}/docs` | Scatter-gather. `filter`, `sort`, `desc`, `limit`, `offset`. |
| `GET` | `/api/collections/{col}/watch?since=N&wait=S` | NDJSON change feed (long-poll). |
| `GET` | `/api/cluster` | Self + live peers. |
| `GET` | `/health` | Liveness (never gated by auth). |
| `GET` | `/metrics` | Prometheus text format. |

**Filter operators:** exact, `$gt`, `$gte`, `$lt`, `$lte`, `$ne`, `$in`, `$exists`, `$regex`.
Multiple conditions AND together.

**CLI flags:** `--addr`, `--dir`, `--join <seed,seed>`, `--rf <n>`, `--fsync`,
`--auth-token <bearer>`, `--tls-cert/--tls-key/--tls-ca` (mutual TLS on `/internal/*`).

**microctl:** `backup --dir D --out F` · `restore --dir D --in F` · `status --url U` ·
`hints --url U` (handoff debt) · `decommission --url U` (drain a node before stopping it)

**Go client:** `client.New(url)` → `Put/Get/Delete/Batch/Query/Watch/Cluster/Health`,
bearer token via `c.Token`, TLS via `client.NewTLS(url, caFile)`.

## Testing

```bash
go test ./...            # unit + integration (3-node clusters spun up per test)
```

Covers: replication, dropped-write repair, concurrent-divergence resolution, scatter-gather,
pagination/sort, batch atomicity, watch feeds, TLS mutual auth, bearer auth, backup/restore,
compaction/GC, index correctness, Merkle diff bounds.

## Limitations (by design — it's tiny)
- LWW can lose concurrent same-key writes (no vector clocks / CRDT merge).
- Watch feed is per-node and bounded (10k events / 5 min), not durable.
- Scatter-gather queries are O(cluster) — fine for small clusters.
- No per-collection RF, no namespacing, no ACID.

## Layout
```
cmd/microdb/     server entrypoint
cmd/microctl/    admin CLI (backup/restore/status)
client/          typed Go client library
internal/store/      documents, JSONL log, replay, compaction, GC, backup/restore
internal/ring/       consistent hashing
internal/cluster/    gossip, replication, anti-entropy, TLS options
internal/merkle/     Merkle trees + remote subtree diff
internal/index/      inverted field indexes
internal/changelog/  bounded mutation feed
internal/metrics/    Prometheus registry
integration/     end-to-end tests (multi-node)
```
