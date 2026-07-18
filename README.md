# kvdb

A small **networked key-value database** written in Rust, exposed over an
**HTTP/REST API** with HTTP Basic authentication. It's a learning project that
mirrors the starting architecture of LSM-tree engines (RocksDB, LevelDB): data
lives in memory and is durably journaled to disk through a write-ahead log
(WAL), which is replayed on startup to rebuild state.

## Architecture

```
src/
├── lib.rs        — module exports
├── store.rs      — storage engine: memtable (BTreeMap) + WAL + recovery
├── http.rs       — axum router, REST handlers, Basic-auth middleware
└── bin/
    ├── server.rs — HTTP server (axum + tokio)
    └── client.rs — HTTP client: interactive REPL and one-shot mode
tests/
├── store.rs      — storage engine integration tests
└── http.rs       — HTTP router tests (auth, CRUD) via tower oneshot
```

**Durability guarantee:** every mutation is first appended to the WAL and flushed
to disk, and only then applied to the memtable. A record left torn by a crash
mid-write is treated as uncommitted and dropped during recovery.

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
cargo build --release
cargo test           # fast suite: storage engine + HTTP router tests

# Heavier suites are opt-in (kept out of the default run):
cargo test --release --test load -- --ignored              # stress / concurrency
cargo test --release --test perf -- --ignored --nocapture  # throughput numbers
```

CI builds the Docker image and smoke-tests the running container on every push,
so the primary (containerized) way of running kvdb is exercised end-to-end.

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

Alongside the WAL (`kvdb.wal`), a full store keeps sorted **SSTable** files
(`kvdb-000001.sst`, …) and a **manifest** (`kvdb.manifest`) that lists them in
order together with the latest durable commit sequence. When the memtable grows
past `KVDB_MEMTABLE_LIMIT` entries (default `1024`) it is flushed to a new
SSTable, the manifest is updated, and the WAL is truncated. Each new SSTable
stores records in 64-entry blocks with a persisted
**sparse index** (one first-key + byte range per block) and a **Bloom filter**.
Opening a table does not load every key into memory; a Bloom negative skips the
file entirely, while a possible hit binary-searches the index and scans one
block. Once `KVDB_COMPACTION_THRESHOLD` SSTables accumulate (default `8`), the
Store automatically performs a full compaction: it retains only the newest
value for each key, drops tombstones, and atomically replaces the manifest.
Setting the threshold to `0` disables automation; `Store::compact()` remains
available for an explicit run.

### Atomic batches

Every standalone `SET`/`DELETE` receives a monotonically increasing commit
sequence. Library users can group mutations into a `WriteBatch`; the whole
batch is encoded as one WAL record and consumes one sequence number, so recovery
applies either every operation or none of them if the trailing record is torn:

```rust
use kvdb::{Store, WriteBatch};

let mut store = Store::open("kvdb.wal")?;
let mut batch = WriteBatch::new();
batch
    .set(b"user:1".to_vec(), b"Alice".to_vec())
    .delete(b"user:old".to_vec());
let sequence = store.write_batch(batch)?;
# Ok::<(), std::io::Error>(())
```

`Store::snapshot()` returns an immutable read-only copy of all currently visible
values together with its commit sequence. Later writes, flushes, and compactions
do not affect it. This first snapshot implementation deliberately copies the
logical state, so creating one costs O(keys + values) memory; it does not yet
retain multiple versions inside SSTables.

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

One-shot (handy for scripts); credentials via env or `--user`/`--password`:

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

## Roadmap (LSM tree)

- [x] memtable + WAL + recovery
- [x] HTTP/REST API with Basic auth
- [x] logging to console **and** file (no external crates)
- [x] represent deletes as **tombstones** in the memtable — a delete must leave a
  trace so it can shadow an older on-disk value instead of resurrecting it
- [x] flush the memtable into an immutable sorted **SSTable** once it crosses a
  threshold; seal (truncate) the WAL and record the file in a **manifest**
- [x] read path: memtable → newest-to-oldest SSTables, first hit (incl. tombstone) wins
- [x] SSTable **block index** + persisted sparse index (64 records per block;
  one first-key + byte range per block stays resident in memory)
- [x] **compaction** — full k-way merge of SSTables; drop shadowed entries and
  tombstones, then atomically publish the replacement manifest
- [x] automatic compaction trigger by SSTable count (default `8`, configurable)
- [x] **bloom filters** persisted with each new SSTable to skip files that
  definitely cannot contain a key (false positives still use the normal lookup)
- [x] durable **sequence numbers** + atomic WAL **write batches**
- [x] immutable read-only **snapshots** at a fixed sequence (copy-on-snapshot)
- [x] optimistic **write transactions** with read-your-writes, atomic commit, and
  key-level read/write conflict detection
- [ ] full MVCC with per-key versions and key-level conflict detection
