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
cargo test           # storage engine + HTTP router tests
```

## Run

Server (defaults to `0.0.0.0:6380`, WAL at `kvdb.wal`):

```sh
KVDB_USER=admin KVDB_PASSWORD=secret cargo run --bin kvdb-server
# optional: kvdb-server <BIND_ADDR> <WAL_PATH>
```

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
- [ ] flush the memtable into sorted **SSTables** once it crosses a threshold
- [ ] SSTable lookups + block index
- [ ] **compaction** — merge SSTables, drop tombstones
- [ ] **bloom filters** to skip files that can't contain a key
- [ ] batches / transactions / MVCC
