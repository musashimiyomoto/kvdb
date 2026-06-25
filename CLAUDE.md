# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`kvdb` is a learning-oriented networked key-value database in Rust (edition 2024). It implements the starting point of an LSM-tree engine: an in-memory memtable (`BTreeMap`) made durable by a write-ahead log, exposed over an HTTP/REST API with HTTP Basic auth. See the README's Roadmap for the intended trajectory (SSTables → compaction → bloom filters); the code is structured to grow into those stages.

## Commands

```sh
cargo build --release
cargo test                          # all tests (lib + integration)
cargo test --test store             # only the storage-engine integration tests
cargo test --test http              # only the HTTP router tests
cargo test set_get_delete           # a single test by name
cargo clippy --all-targets          # lint
cargo fmt                           # format

# Run the server (credentials are MANDATORY or it refuses to start)
KVDB_USER=admin KVDB_PASSWORD=secret cargo run --bin kvdb-server
# optional positional args: kvdb-server <BIND_ADDR> <WAL_PATH>  (defaults 0.0.0.0:6380, kvdb.wal)

# Run the client (REPL, or one-shot)
KVDB_USER=admin KVDB_PASSWORD=secret cargo run --bin kvdb-client
cargo run --bin kvdb-client -- http://127.0.0.1:6380 --user admin --password secret SET k v
```

## Architecture

Three modules, layered bottom-up (`src/lib.rs` re-exports `Store`, `AppState`, `router`):

- **`src/store.rs` — storage engine.** `Store` is single-threaded and not internally synchronized; callers serialize access (the HTTP layer wraps it in a `Mutex`). The WAL is a custom binary format: `[op:u8][key_len:u32 BE][key][val_len:u32 BE][value]` for SET, `[op][key_len][key]` for DELETE. Durability ordering is the core invariant: **every mutation appends to the WAL and flushes to disk *before* the memtable is updated.** Recovery (`replay`) is forgiving by design — a truncated trailing record (crash mid-write) hits `UnexpectedEof` and is dropped as uncommitted rather than failing startup. An unknown op code, by contrast, is a hard `InvalidData` error.

- **`src/http.rs` — REST layer.** `router(state)` builds the axum 0.8 router; the router is constructed here (not in the binary) specifically so integration tests can drive it via `tower`'s `oneshot` without binding a socket. `/health` is public; all `/v1/*` routes sit behind `auth` middleware (`route_layer`). `AppState` holds `Arc<Mutex<Store>>` plus credentials. Note: handlers return `lock_error()` (500) if the mutex is poisoned — a panic while holding the store lock degrades the whole server. Credential comparison uses `constant_time_eq` to avoid timing leaks.

- **`src/bin/{server,client}.rs` — thin binaries.** Server reads `KVDB_USER`/`KVDB_PASSWORD` from env (empty = unset = refuse to start), opens the store, serves the router. Client uses `reqwest` (built with `default-features = false` — **no TLS**, plain HTTP only; keeps the build/image lean).

## Tests

Integration tests live in `tests/` (not unit tests in `src`). `tests/http.rs` exercises the real router through `oneshot` and includes hand-rolled base64 for the auth header. Tests create isolated WAL files via per-test `tmp_path(tag)` helpers — when adding tests, give each a unique tag so WAL files don't collide.

## Conventions

- Keys and values are arbitrary bytes end-to-end (`Vec<u8>`); the HTTP layer carries values as the raw request/response body.
- When extending the WAL format, preserve backward-compatible replay (old logs must still recover) and keep the "torn tail is dropped, bad data is an error" distinction in `read_record`.
