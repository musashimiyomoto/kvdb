# kvdb roadmap

Last reviewed: 2026-07-19.

This roadmap starts from the repository as it exists today. The LSM-tree
learning milestones are implemented and well covered by logical correctness
tests. The next goal is to turn that foundation into a trustworthy single-node
service before adding more database features.

## Audit baseline

The review covered every source, test, workflow, container, and documentation
file in the repository. On Rust 1.97.1 the following checks pass:

- `cargo fmt --all -- --check`
- `cargo clippy --all-targets --locked -- -D warnings`
- `cargo test --all --locked` (53 passing tests, 19 intentionally ignored)
- release load suite (4 passing tests)
- release performance suite (15 passing informational scenarios)

The Dockerfile's Rust 1.96 build could not be reproduced during this review
because Docker Hub was unavailable. CI and local builds use the lockfile, while
the current Docker build does not.

## Current risk register

The order below reflects correctness and operational impact, not implementation
convenience.

| ID | Priority | Current issue | Impact |
|---|---|---|---|
| R1 | P0 | `Store::set`, `delete`, and `write_batch` acknowledge after `BufWriter::flush`, without `File::sync_data` | An OS crash or power loss can lose acknowledged writes; the current durability wording is stronger than the implementation |
| R2 | P0 | A partial WAL write leaves the same `Store` usable and WAL records have no frame checksum | Continuing after disk-full or short-write errors can append behind a torn record and make later recovery fail |
| R3 | P0 | `Store::lookup_at` logs SSTable read errors and returns `None` | Corruption or I/O failure can be reported as a legitimate missing key; DELETE can also make the wrong existence decision |
| R4 | P0 | The memtable limit counts distinct keys, not bytes or versions | Repeated writes to one hot key can grow memory and the WAL without triggering a flush |
| R5 | P0 | WAL and SSTable data decoders allocate from untrusted length/count fields before enforcing configured bounds | A small malformed file can cause excessive allocation or recovery-time denial of service |
| R6 | P0 | There is no exclusive data-directory/process lock | Two server processes can open the same WAL and corrupt sequence, manifest, and SSTable state |
| R7 | P1 | SSTable data, WAL records, and the text manifest have no integrity checksum; rename operations do not fsync the parent directory | Bit rot can become silent wrong data, and metadata persistence across a machine crash is not fully specified |
| R8 | P1 | Axum handlers hold `std::sync::Mutex<Store>` while doing blocking filesystem I/O | A slow disk blocks Tokio workers and all requests; there is no queue bound or write backpressure |
| R9 | P1 | Basic credentials are sent over plain HTTP; the client accepts an `https://` URL but reqwest is built without TLS | Credentials are exposed off-host and the advertised client URL scheme is inconsistent with its build |
| R10 | P1 | The client treats HTTP error statuses as a successful command and converts all values to text | Scripts receive exit code 0 for 401/404/500, and the bundled client does not preserve arbitrary binary values |
| R11 | P1 | The HTTP API uses UTF-8 path keys and an implicit Axum body limit while library docs promise arbitrary bytes | Limits and binary-key behavior are inconsistent across the library, REST API, and client |
| R12 | P1 | Docker omits `Cargo.lock` and does not use `--locked`; no repository toolchain/MSRV file exists | The image can resolve a dependency graph different from CI and may drift beyond Rust 1.96 compatibility |
| R13 | P2 | Compaction materializes every live SSTable in memory and runs inline with the triggering write | Large compactions can multiply peak memory and create long write/request stalls |
| R14 | P2 | Liveness does not distinguish readiness; there are no timeouts, graceful shutdown, metrics, backup, verify, or repair commands | Operators cannot reliably detect degradation, drain the server, or recover data |

## Milestone 0: trustworthy persistence (P0)

Complete this milestone before treating kvdb as durable storage.

### 0.1 Define and enforce the durability contract

- Add explicit durability modes, with `sync_data` before acknowledging a write
  in the default `durable` mode. If a buffered mode remains, name and document
  its data-loss window rather than calling it durable.
- Fsync the containing directory after publishing or replacing SSTables and the
  manifest. Document the supported filesystem and rename assumptions.
- Put the store into a fail-stop/poisoned state after any uncertain WAL, flush,
  manifest, or compaction write. Only reopen/recovery may make it writable again.
- Return structured storage errors that distinguish invalid input, corruption,
  unavailable I/O, conflicts, and internal invariant failures.

Acceptance: after every acknowledged durable write, forced process/machine-crash
tests recover that write; after injected short-write/disk-full errors, no later
write is accepted by the same store instance.

### 0.2 Make recovery bounded and corruption explicit

- Introduce a versioned, length-delimited WAL frame with maximum record, key,
  value, operation-count, and version-count limits plus a checksum.
- Apply the same configured bounds to SSTable records, indexes, Bloom metadata,
  manifests, snapshots, batches, and HTTP bodies before allocating.
- Change current reads to a fallible API (`io::Result<Option<Vec<u8>>>` or a
  domain error) and propagate SSTable failures to HTTP as unavailable/corrupt,
  never as `404`.
- Add checksums per SSTable block and for manifest metadata. Define a migration
  path that can read version 1 files and writes the new format atomically.
- Validate manifest filenames, table ordering, sequence bounds, duplicate
  entries, and table metadata against the manifest during open.

Acceptance: fuzzed/truncated/corrupted files never panic or allocate beyond the
configured budget, and every corruption is either rejected during open or
surfaced by the read that encounters it.

### 0.3 Bound memory and enforce single ownership

- Track memtable resident bytes, version count, and WAL bytes in addition to
  distinct keys. Flush/backpressure on the first configured limit reached.
- Bound or replace the ever-growing `key_sequences` transaction-conflict map.
- Acquire an exclusive lock for the store directory/WAL lifetime; a second
  writer must fail with a clear startup error.
- Clean stale temporary/orphan files only after proving they are not referenced
  by the durable manifest.

Acceptance: a hot-key workload stays within its memory/WAL budget, and a second
process cannot mutate the same store.

## Milestone 1: production-safe service boundary (P1)

### 1.1 Move storage I/O off async request workers

- Give one dedicated blocking storage worker ownership of `Store`; communicate
  through a bounded request queue with cancellation and overload responses.
- Add optional group commit so concurrent durable writes can share an fsync
  without weakening the selected durability contract.
- Separate `/live` from `/ready`; readiness must fail when storage is poisoned,
  the queue is saturated for too long, or recovery has not completed.
- Add connection, request-body, key/value, concurrency, and request-timeout
  limits. Implement graceful SIGTERM/SIGINT drain and final state reporting.

Acceptance: slow-disk tests do not block unrelated Tokio tasks, overload is
bounded and observable, and shutdown either completes queued acknowledged work
or clearly rejects it.

### 1.2 Make the API and client contracts precise

- Choose one binary-key representation for HTTP (for example base64url) or
  explicitly define the REST API as UTF-8-only. Keep raw response bodies for
  binary values and publish exact size limits.
- Add versioned machine-readable errors and stable mappings for not-found,
  conflict, invalid input, too-large, unavailable, and corruption cases.
- Expose atomic batch writes and compare-and-set before exposing the current
  in-process transaction object over the network.
- Make one-shot client commands return non-zero for authentication, transport,
  server, and unexpected not-found errors; add strict argument validation,
  timeouts, raw file/stdin input, and raw stdout output.
- Either add rustls HTTPS support to the client/server or formally require a TLS
  reverse proxy and reject/document plaintext Basic Auth outside trusted local
  deployments. Add authentication throttling.

Acceptance: TCP-level integration tests cover binary data, status/exit-code
mapping, limits, timeouts, TLS policy, and server restart behavior.

### 1.3 Reproducible and secure delivery

- Copy `Cargo.lock` into the Docker build and use `cargo build --locked`; add a
  pinned `rust-toolchain.toml` or documented MSRV tested in CI.
- Pin GitHub Actions by commit SHA, add dependency/license policy (`cargo-deny`)
  and vulnerability scanning, and schedule dependency updates.
- Add OCI labels, a container `HEALTHCHECK`, read-only-root-filesystem guidance,
  an SBOM, and a non-root persistence smoke test using the exact release image.
- Split correctness benchmarks from historical machine-specific numbers in the
  README; retain raw benchmark artifacts and track regressions on a controlled
  runner before introducing gates.

Acceptance: local, CI, and Docker builds use the same locked dependency graph
and supported Rust version; image and dependency scans have no untriaged high
severity findings.

## Milestone 2: predictable storage at scale (P2)

- Replace all-in-memory full compaction with a streaming k-way merge and bounded
  buffers. Run compaction in the storage worker/background executor with write
  backpressure and crash-safe publication.
- Move from an SSTable-count trigger to size-aware L0/tiered or leveled
  compaction with explicit write-amplification and space-amplification targets.
- Add a bounded block cache and file-handle cache; measure Bloom false-positive
  rate, read amplification, cache hit rate, and p50/p95/p99 latency.
- Make snapshots/transactions lazy or structurally shared so beginning a
  transaction does not copy the whole visible database. Preserve retention
  correctness with active snapshots before changing the representation.
- Add prefix/range iteration primitives needed by backup and future API work.

Acceptance: compaction peak memory is bounded independently of database size,
foreground latency remains within a documented budget, and model-based tests
still match a reference MVCC map across compaction/restart cycles.

## Milestone 3: operations and observability (P2)

- Adopt structured tracing with request IDs and redaction. Export metrics for
  request latency/status, queue depth, WAL/fsync latency, memtable bytes,
  SSTable count/bytes, compaction, recovery, and corruption.
- Add `kvdb verify`, consistent backup/export, restore/import, and explicit
  offline repair/salvage tooling. Never make automatic repair silently discard
  data.
- Add typed configuration with precedence (CLI, environment, defaults), startup
  validation, and a redacted effective-configuration log.
- Document capacity planning, upgrade/format migration, backup recovery,
  disk-full response, corruption response, and graceful shutdown runbooks.

Acceptance: a fresh operator can detect an unhealthy store, create and restore
a verified backup, and follow a tested runbook for disk-full and corruption.

## Test strategy that spans all milestones

- Add failpoints around WAL write/sync, SSTable sync/rename, manifest
  sync/rename, WAL truncation, and old-table deletion; kill a child process at
  each point and compare recovery with an acknowledged-operation journal.
- Add property/model tests for batches, MVCC, retention, snapshots, transaction
  conflicts, and arbitrary reopen/flush/compact sequences.
- Fuzz every WAL/SSTable/manifest decoder with allocation limits; run Miri on
  focused codec/state-machine tests where practical.
- Add real TCP server/client tests, multi-process lock tests, slow/disk-full I/O
  tests, and platform coverage for Linux plus any explicitly supported targets.
- Keep load tests correctness-only. Move statistical performance work to a
  benchmark harness that reports distributions, warmup, dataset shape, and
  storage durability mode.

## Feature work after hardening (P3)

Once Milestones 0-2 are complete, prioritize features that build on the proven
single-node engine: range/prefix scans, conditional writes, network batch APIs,
online backup, and optional TTL with explicitly defined MVCC semantics.

Replication, clustering, distributed transactions, and automatic sharding are
deliberately out of scope until single-node durability, bounded recovery,
backpressure, and operational recovery are demonstrated.

