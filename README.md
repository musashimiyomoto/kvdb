# kvdb

A small **networked key-value database** written in Rust, exposed over an
**HTTP/REST API** with HTTP Basic authentication. It's a learning project that
mirrors the core architecture of LSM-tree engines (RocksDB, LevelDB): recent
writes live in a memtable and a write-ahead log (WAL), while flushed data lives
in immutable SSTables. On startup, the manifest is loaded first and the WAL is
then replayed over the listed tables.

## Architecture

```
src/
├── lib.rs        — public module exports
├── store.rs      — WAL, memtable, manifest, recovery, MVCC, compaction
├── sstable.rs    — SSTable codec, sparse index, and Bloom filter
├── limits.rs     — storage input and allocation limits
├── http.rs       — Axum router, bounded storage worker, group commit, auth
├── log.rs        — dependency-free structured line logging
└── bin/
    ├── server.rs — HTTP server (axum + tokio)
    └── client.rs — HTTP client: interactive REPL and one-shot mode
tests/
├── store.rs      — storage engine integration tests
├── http.rs       — HTTP router tests via tower oneshot
├── load.rs       — ignored release-mode correctness workloads
└── perf.rs       — ignored informational performance scenarios
```

**Durability guarantee:** by default every mutation is appended to the WAL,
flushed, and synchronized with `sync_data` before it is applied to the memtable
or acknowledged. A trailing record left torn by a crash is treated as
uncommitted and dropped during recovery. `KVDB_DURABILITY=buffered` is an
explicit performance mode that flushes only to the operating system and can
lose acknowledged writes after an OS crash or power loss.

HTTP storage operations run on one dedicated blocking worker behind a bounded
FIFO queue. Adjacent writes are committed as separate logical WAL records and
sequence numbers but share one flush/fsync; an intervening GET is never
reordered. A saturated queue fails immediately with `503 Service Unavailable`
instead of growing without bound.

## REST API

| Method | Path             | Auth | Behavior                              |
|--------|------------------|------|---------------------------------------|
| GET    | `/health`        | no   | `200 "PONG"` — liveness probe / PING  |
| GET    | `/v1/keys/{key}` | yes  | `200` body = value, or `404`          |
| PUT    | `/v1/keys/{key}` | yes  | request body = value → `200 "OK"`     |
| DELETE | `/v1/keys/{key}` | yes  | `200 "OK"`, or `404` if key was absent|

Requests to `/v1/*` without valid credentials get `401 Unauthorized` with a
`WWW-Authenticate: Basic` challenge.

## Authentication

Credentials are read from the environment and are **required** — the server
refuses to start without them:

```sh
export KVDB_USER=admin
export KVDB_PASSWORD=secret
```

Clients authenticate with HTTP Basic auth (`curl -u user:pass`, or the
`Authorization: Basic ...` header).

## Build

```sh
cargo build --release --locked
cargo test --all --locked  # fast suite: storage engine + HTTP router tests

# Heavier suites are opt-in (kept out of the default run):
cargo test --release --locked --test load -- --ignored --nocapture --test-threads=1
KVDB_DURABILITY=buffered cargo test --release --locked --test perf -- --ignored --nocapture --test-threads=1
cargo bench --locked --bench kvdb_bench -- --profile standard
```

The repository pins Rust 1.96.0 in `rust-toolchain.toml`.

GitHub Actions runs formatting, Clippy, the fast suite, sequential release load
tests, informational benchmarks, and a Docker persistence smoke test on every
push to `master` and pull request. Load/performance logs are retained as build
artifacts for 14 days; benchmark timings appear in the run summary but do not
gate the build because shared-runner performance is noisy. The workflow can
also be started manually with `workflow_dispatch`.

## Load tests and benchmarks

The performance tooling has three separate jobs:

- `tests/load.rs` asserts correctness under volume, concurrency, repeated
  reopen, compaction, and MVCC retention. Timing never decides whether it
  passes.
- `tests/perf.rs` contains small component microbenchmarks useful for spotting
  regressions in individual storage paths.
- `benches/kvdb_bench.rs` is the end-to-end benchmark. It measures explicit
  buffered and durable writes, durable batches, randomized memtable and warm
  SSTable reads with the application cache both enabled and disabled, WAL
  recovery, overlapping compaction, and real TCP GET/PUT at several concurrency
  levels.

Current load suite (all passed in `17.73 s` on the machine described below):

| Scenario | Workload | What is verified |
|---|---:|---|
| Bulk persistence | 100k SET + 25k DELETE, ~100 flushes | Every value/tombstone after reopen with 90+ SSTables |
| Concurrent HTTP | 200 distinct-key writers + 200 hot-key writers | No lost/corrupt writes; valid last-writer-wins result |
| Deterministic mixed | 100k ops, 10k keys, 60% SET / 25% DELETE / 15% GET | Exact match with `BTreeMap` across periodic flush, compaction, and 10 reopens |
| MVCC + retention | 30k commits over 2k keys + 4k historical probes | Historical reads before GC and retained reads after GC/reopen |

### End-to-end methodology

Run the quick profile while iterating and the standard profile for a report:

```sh
cargo bench --locked --bench kvdb_bench -- --profile quick
cargo bench --locked --bench kvdb_bench -- --profile standard

# Put data on a specific device and retain it for inspection:
cargo bench --locked --bench kvdb_bench -- \
  --profile standard --dir /path/on/the/device --keep
```

The default benchmark root is `target/kvdb-bench`, on the same filesystem as
the repository. On Linux the harness detects the filesystem type and refuses
`tmpfs`/`ramfs`, because `sync_data` there cannot measure stable-storage
latency. `--allow-memory-fs` is available only for an explicitly CPU-only run.

Every result reports median throughput across independent samples plus
p50/p95/p99/max latency. Library-operation latency is sampled to limit timer
overhead; every TCP request is timed. The output records the profile, path,
filesystem, Rust version, value size, throughput unit, and latency unit so a
batch-commit latency cannot be mistaken for per-record latency.

SSTable and recovery scenarios are deliberately labeled `warm`: portable cache
eviction cannot be guaranteed without privileged, platform-specific support.
CI runs the quick profile only as an informational regression signal. Shared
runner timings are retained as artifacts but never treated as an SLA or a
performance gate.

## Run

Server (defaults to `0.0.0.0:6380`, WAL at `kvdb.wal`):

```sh
KVDB_USER=admin KVDB_PASSWORD=secret cargo run --bin kvdb-server
# optional: kvdb-server <BIND_ADDR> <WAL_PATH>
```

### Logging

The server logs to **stderr** and, if `KVDB_LOG_FILE` is set, also appends to
that file. The minimum level is controlled by `KVDB_LOG` (`error`, `warn`,
`info` (default), `debug`):

```sh
KVDB_LOG=debug KVDB_LOG_FILE=kvdb.log \
KVDB_USER=admin KVDB_PASSWORD=secret cargo run --bin kvdb-server
# [2026-07-01T12:00:00Z INFO  kvdb::server] listening on 0.0.0.0:6380
```

### Storage layout

Alongside `kvdb.wal`, the store may create **SSTable** generations named
`kvdb-000000.sst`, `kvdb-000001.sst`, and so on. Records inside each SSTable
are sorted by key. The **manifest** is `kvdb.manifest`; it lists live tables
from oldest to newest and records the latest durable commit sequence and
retained-history boundary. A sibling lock file, `kvdb.wal.lock`, is held with
an operating-system advisory lock for the `Store` lifetime, so a second writer
cannot open the same WAL.

The memtable is flushed when the first configured limit is reached:

| Environment variable | Default | Meaning |
|---|---:|---|
| `KVDB_MEMTABLE_LIMIT` | 1024 | distinct memtable keys |
| `KVDB_MEMTABLE_BYTES_LIMIT` | 64 MiB | approximate memtable payload bytes |
| `KVDB_MEMTABLE_VERSIONS_LIMIT` | 16384 | retained memtable versions |
| `KVDB_WAL_BYTES_LIMIT` | 128 MiB | WAL file size |
| `KVDB_COMPACTION_THRESHOLD` | 8 | SSTable count; `0` disables automatic compaction |
| `KVDB_SSTABLE_FILE_CACHE_CAPACITY` | 64 | open SSTable file handles; `0` disables |
| `KVDB_SSTABLE_BLOCK_CACHE_BYTES` | 64 MiB | decoded block payload budget; `0` disables |

The HTTP storage worker has separate backpressure and group-commit controls:

| Environment variable | Default | Meaning |
|---|---:|---|
| `KVDB_STORAGE_QUEUE_CAPACITY` | 1024 | queued storage commands before HTTP returns `503` |
| `KVDB_GROUP_COMMIT_MAX` | 64 | maximum adjacent writes sharing one WAL flush/fsync |
| `KVDB_GROUP_COMMIT_DELAY_US` | 1000 | durable-write collection window in microseconds |

After a successful flush, the manifest is updated and the WAL is truncated.
Each SSTable key record contains its versions in ascending commit-sequence
order. Tables group records into 64-entry blocks with a persisted **sparse
index** (one first key and byte range per block) and a **Bloom filter**.
Opening a table does not load every key into memory; a Bloom negative skips the
file entirely, while a possible hit binary-searches the index and searches one
decoded block. Open files and decoded blocks share bounded LRU caches across
all live tables in a Store. Compaction invalidates retired table entries before
deleting their files; hit/miss/eviction/residency counters are available from
`Store::sstable_cache_metrics()`.

Once `KVDB_COMPACTION_THRESHOLD` SSTables accumulate (default `8`), the Store
automatically performs a full compaction: it deduplicates identical commit
sequences, preserves historical values and tombstones for MVCC, and atomically
replaces the manifest. Setting the threshold to `0` disables automation;
`Store::compact()` remains available for an explicit run. Automatic and
ordinary manual compaction preserve every currently retained version.

### Atomic batches

Every standalone `SET`/`DELETE` receives a monotonically increasing commit
sequence. Library users can group mutations into a `WriteBatch`; the whole
batch is encoded as one WAL record and consumes one sequence number, so recovery
applies either every operation or none of them if the trailing record is torn:

```rust
use kvdb::{Store, WriteBatch};

fn main() -> std::io::Result<()> {
    let mut store = Store::open("kvdb.wal")?;
    let mut batch = WriteBatch::new();
    batch
        .set(b"user:1".to_vec(), b"Alice".to_vec())
        .delete(b"user:old".to_vec());
    let sequence = store.write_batch(batch)?;
    println!("committed at sequence {sequence}");
    Ok(())
}
```

`Store::snapshot()` returns an immutable read-only copy of all currently visible
values together with its commit sequence. `snapshot_at(sequence)` reconstructs
the state at an older commit. Later writes, flushes, and compactions do not
affect either snapshot. Creating one costs O(visible keys + values) memory; the
underlying memtable and SSTables retain ordered per-key versions for historical
reads.

### History retention

History is retained indefinitely by default. To reclaim old versions, call
`compact_with_retention(history_start)`: it flushes pending writes, fully merges
the SSTables, and retains exact reads and snapshots from `history_start` onward.
The newest value at or before that sequence is kept as an anchor for each key;
an obsolete tombstone is removed once no older SSTable can resurface behind it.
The boundary is persisted in the manifest and can only advance. Historical
`get_at` and `snapshot_at` calls before the boundary return `InvalidInput`
rather than a partial answer.

```rust
store.compact_with_retention(10_000)?;
assert_eq!(store.history_start_sequence(), 10_000);
```

`Store::begin_transaction()` builds on that snapshot with a private write
overlay. Reads see the transaction's own SET/DELETE operations first. Commit is
optimistic: `commit_transaction()` writes the overlay as one atomic batch only
if none of the transaction's read or written keys changed after its snapshot.
Independent transactions touching different keys can both commit; dropping a
transaction aborts it without WAL I/O.

### Talk to it with curl

```sh
curl -u admin:secret -X PUT  localhost:6380/v1/keys/city -d Berlin   # -> OK
curl -u admin:secret         localhost:6380/v1/keys/city             # -> Berlin
curl -u admin:secret -X DELETE localhost:6380/v1/keys/city           # -> OK
curl                         localhost:6380/health                   # -> PONG
```

Because it's plain HTTP, any language or tool works (browser, Python `requests`,
JS `fetch`, etc.) — no special client required.

### Talk to it with the bundled client

Interactive REPL:

```sh
KVDB_USER=admin KVDB_PASSWORD=secret cargo run --bin kvdb-client
kvdb> SET greeting hello world
OK
kvdb> GET greeting
hello world
kvdb> DEL greeting
OK
kvdb> PING
PONG
```

One-shot invocation; credentials can come from the environment or from
`--user`/`--password`:

```sh
cargo run --bin kvdb-client -- http://127.0.0.1:6380 --user admin --password secret SET counter 42
cargo run --bin kvdb-client -- http://127.0.0.1:6380 --user admin --password secret GET counter
```

## Docker

```sh
docker build -t kvdb .
docker run -d \
  -e KVDB_USER=admin -e KVDB_PASSWORD=secret \
  -p 6380:6380 \
  -v kvdb-data:/data \
  kvdb

curl -u admin:secret -X PUT localhost:6380/v1/keys/city -d Berlin
```

Credentials are passed at run time and are never baked into the image. The WAL
lives on the `/data` volume, so data survives container restarts. The server
runs as a non-root user.

## Roadmap

The learning-oriented LSM foundation is complete: WAL recovery, versioned
memtables, indexed and Bloom-filtered SSTables, atomic batches, snapshots,
optimistic transactions, MVCC retention, compaction, HTTP access, load tests,
and a local performance baseline are implemented.

The first persistence-hardening pass is complete: durable mode calls
`sync_data`, recovery allocations are bounded, storage read failures propagate,
memory/WAL flush limits are enforced, and a second writer is rejected. The
benchmark-driven performance pass is active: bounded worker/group commit is in
place, with SSTable caching and background streaming compaction next. See the
[prioritized roadmap](ROADMAP.md) for current status and acceptance criteria.
