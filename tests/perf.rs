//! Informational component microbenchmarks built on [`std::time::Instant`].
//!
//! These are **not** pass/fail assertions on timing (which would be flaky in
//! CI); they print throughput and latency so you can eyeball regressions and
//! understand the cost of each operation and of the LSM read path (memtable hit
//! vs SSTable hit vs miss). They do not replace the end-to-end benchmark in
//! `benches/kvdb_bench.rs`. They are `#[ignore]`d so `cargo test` stays fast.
//!
//! Run them, in release mode, with output shown:
//!
//! ```sh
//! cargo test --release --locked --test perf -- --ignored --nocapture --test-threads=1
//! ```

use std::hint::black_box;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use kvdb::http::{AppState, router};
use kvdb::store::{Store, WriteBatch};
use tower::ServiceExt;

fn tmp_dir(tag: &str) -> PathBuf {
    let mut p = std::env::var_os("KVDB_BENCH_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("target")
                .join("kvdb-microbench")
        });
    std::fs::create_dir_all(&p).unwrap();
    p.push(format!("kvdb-perf-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn key(i: usize) -> Vec<u8> {
    format!("key-{i:08}").into_bytes()
}

fn value(i: usize) -> Vec<u8> {
    format!("value-number-{i}").into_bytes()
}

/// Prints `label`, the number of ops, total wall time, throughput and mean
/// per-op latency in a fixed, greppable format.
fn report(label: &str, ops: usize, elapsed: Duration) {
    let secs = elapsed.as_secs_f64();
    let per_sec = ops as f64 / secs;
    let per_op_ns = elapsed.as_nanos() as f64 / ops as f64;
    println!(
        "{label:<28} {ops:>9} ops  {secs:>8.3}s  {per_sec:>12.0} ops/sec  {per_op_ns:>10.1} ns/op"
    );
}

fn report_duration(label: &str, elapsed: Duration) {
    println!(
        "DURATION {label:<28} {:>10.3} ms",
        elapsed.as_secs_f64() * 1_000.0
    );
}

fn report_storage(label: &str, bytes: u64, records: usize) {
    println!(
        "STORAGE  {label:<28} {bytes:>12} bytes  {:>10.1} bytes/record",
        bytes as f64 / records as f64
    );
}

fn storage_bytes(dir: &std::path::Path) -> u64 {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().metadata().unwrap().len())
        .sum()
}

fn keep_all_writes_in_memtable(store: &mut Store) {
    store.set_memtable_limit(usize::MAX);
    store.set_memtable_bytes_limit(usize::MAX);
    store.set_memtable_versions_limit(usize::MAX);
    store.set_wal_bytes_limit(u64::MAX);
}

/// Write throughput in the durability mode selected by `KVDB_DURABILITY`.
#[test]
#[ignore = "informational benchmark; run with --release --ignored --nocapture"]
fn perf_set_throughput() {
    let dir = tmp_dir("set");
    let wal = dir.join("kvdb.wal");
    let mut s = Store::open(&wal).unwrap();
    keep_all_writes_in_memtable(&mut s);

    const N: usize = 200_000;
    let start = Instant::now();
    for i in 0..N {
        s.set(key(i), value(i)).unwrap();
    }
    report("set (WAL record, no flush)", N, start.elapsed());
    report_storage("WAL after individual SET", storage_bytes(&dir), N);

    std::fs::remove_dir_all(&dir).ok();
}

/// Batched write throughput, amortizing one WAL flush over 100 SET operations.
#[test]
#[ignore = "informational benchmark; run with --release --ignored --nocapture"]
fn perf_batch_set_throughput() {
    let dir = tmp_dir("batch-set");
    let wal = dir.join("kvdb.wal");
    let mut s = Store::open(&wal).unwrap();
    keep_all_writes_in_memtable(&mut s);

    const N: usize = 200_000;
    const BATCH_SIZE: usize = 100;
    let start = Instant::now();
    for batch_start in (0..N).step_by(BATCH_SIZE) {
        let mut batch = WriteBatch::new();
        for i in batch_start..batch_start + BATCH_SIZE {
            batch.set(key(i), value(i));
        }
        s.write_batch(batch).unwrap();
    }
    report("set (batch=100, WAL)", N, start.elapsed());
    report_storage("WAL after batch SET", storage_bytes(&dir), N);

    std::fs::remove_dir_all(&dir).ok();
}

/// Durable tombstone throughput after preparing live keys in one atomic batch.
#[test]
#[ignore = "informational benchmark; run with --release --ignored --nocapture"]
fn perf_delete_throughput() {
    let dir = tmp_dir("delete");
    let wal = dir.join("kvdb.wal");
    let mut s = Store::open(&wal).unwrap();
    keep_all_writes_in_memtable(&mut s);

    const N: usize = 50_000;
    let mut batch = WriteBatch::new();
    for i in 0..N {
        batch.set(key(i), value(i));
    }
    s.write_batch(batch).unwrap();

    let start = Instant::now();
    for i in 0..N {
        assert!(s.delete(&key(i)).unwrap());
    }
    report("delete (durable tombstone)", N, start.elapsed());

    std::fs::remove_dir_all(&dir).ok();
}

/// Read throughput from the in-memory memtable (no SSTable involved).
#[test]
#[ignore = "informational benchmark; run with --release --ignored --nocapture"]
fn perf_get_memtable_hit() {
    let dir = tmp_dir("get-mem");
    let wal = dir.join("kvdb.wal");
    let mut s = Store::open(&wal).unwrap();
    keep_all_writes_in_memtable(&mut s);

    const N: usize = 100_000;
    for i in 0..N {
        s.set(key(i), value(i)).unwrap();
    }

    let start = Instant::now();
    for i in 0..N {
        black_box(s.get(&key(i)).unwrap());
    }
    report("get hit (memtable)", N, start.elapsed());

    std::fs::remove_dir_all(&dir).ok();
}

/// Read throughput when every key lives on disk in a single flushed SSTable
/// (index lookup + seek + read), vs the memtable numbers above.
#[test]
#[ignore = "informational benchmark; run with --release --ignored --nocapture"]
fn perf_get_sstable_hit() {
    let dir = tmp_dir("get-sst");
    let wal = dir.join("kvdb.wal");
    let mut s = Store::open(&wal).unwrap();
    keep_all_writes_in_memtable(&mut s);

    const N: usize = 100_000;
    for i in 0..N {
        s.set(key(i), value(i)).unwrap();
    }
    s.flush().unwrap(); // push everything to one SSTable; memtable now empty

    let start = Instant::now();
    for i in 0..N {
        black_box(s.get(&key(i)).unwrap());
    }
    report("get hit (sstable)", N, start.elapsed());

    std::fs::remove_dir_all(&dir).ok();
}

/// Historical read throughput from a compacted SSTable with five versions per
/// key, selecting a non-current version from each record.
#[test]
#[ignore = "informational benchmark; run with --release --ignored --nocapture"]
fn perf_get_historical_sstable() {
    let dir = tmp_dir("get-historical");
    let wal = dir.join("kvdb.wal");
    let mut s = Store::open(&wal).unwrap();
    keep_all_writes_in_memtable(&mut s);
    s.set_compaction_threshold(0);

    const KEYS: usize = 20_000;
    const VERSIONS: usize = 5;
    let mut sequences = Vec::new();
    for version in 0..VERSIONS {
        let mut batch = WriteBatch::new();
        for i in 0..KEYS {
            batch.set(key(i), format!("version-{version}-{i}").into_bytes());
        }
        sequences.push(s.write_batch(batch).unwrap());
        s.flush().unwrap();
    }
    s.compact().unwrap();

    let start = Instant::now();
    for i in 0..KEYS {
        black_box(s.get_at(&key(i), sequences[1]).unwrap());
    }
    report("get_at (5-version SSTable)", KEYS, start.elapsed());

    std::fs::remove_dir_all(&dir).ok();
}

/// Read throughput for keys that are absent (worst case: must consult every
/// level before concluding "not found").
#[test]
#[ignore = "informational benchmark; run with --release --ignored --nocapture"]
fn perf_get_miss() {
    let dir = tmp_dir("get-miss");
    let wal = dir.join("kvdb.wal");
    let mut s = Store::open(&wal).unwrap();
    s.set_memtable_limit(10_000); // several SSTables to scan through on a miss
    s.set_compaction_threshold(0); // retain all levels for the worst-case miss

    const N: usize = 100_000;
    for i in 0..N {
        s.set(key(i), value(i)).unwrap();
    }

    let start = Instant::now();
    for i in 0..N {
        black_box(s.get(format!("missing-{i}").as_bytes()).unwrap());
    }
    report("get miss (10 Bloom checks)", N, start.elapsed());

    std::fs::remove_dir_all(&dir).ok();
}

/// Cost of flushing a full memtable to an SSTable, per entry.
#[test]
#[ignore = "informational benchmark; run with --release --ignored --nocapture"]
fn perf_flush_cost() {
    let dir = tmp_dir("flush");
    let wal = dir.join("kvdb.wal");
    let mut s = Store::open(&wal).unwrap();
    keep_all_writes_in_memtable(&mut s);

    const N: usize = 200_000;
    for i in 0..N {
        s.set(key(i), value(i)).unwrap();
    }

    let start = Instant::now();
    s.flush().unwrap();
    report("flush (write sstable)", N, start.elapsed());
    report_storage("single SSTable", storage_bytes(&dir), N);

    std::fs::remove_dir_all(&dir).ok();
}

/// Recovery time: reopening a store and replaying a large WAL from scratch.
#[test]
#[ignore = "informational benchmark; run with --release --ignored --nocapture"]
fn perf_recovery_from_wal() {
    let dir = tmp_dir("recovery");
    let wal = dir.join("kvdb.wal");

    const N: usize = 200_000;
    {
        let mut s = Store::open(&wal).unwrap();
        keep_all_writes_in_memtable(&mut s);
        for i in 0..N {
            s.set(key(i), value(i)).unwrap();
        }
    }

    let start = Instant::now();
    let s = Store::open(&wal).unwrap();
    let elapsed = start.elapsed();
    assert_eq!(s.len().unwrap(), N);
    report("recovery (replay wal)", N, elapsed);

    std::fs::remove_dir_all(&dir).ok();
}

/// Recovery when the same dataset is already represented by one SSTable.
#[test]
#[ignore = "informational benchmark; run with --release --ignored --nocapture"]
fn perf_recovery_from_sstable() {
    let dir = tmp_dir("recovery-sstable");
    let wal = dir.join("kvdb.wal");

    const N: usize = 200_000;
    {
        let mut s = Store::open(&wal).unwrap();
        keep_all_writes_in_memtable(&mut s);
        for chunk_start in (0..N).step_by(100_000) {
            let mut batch = WriteBatch::new();
            for i in chunk_start..(chunk_start + 100_000).min(N) {
                batch.set(key(i), value(i));
            }
            s.write_batch(batch).unwrap();
        }
        s.flush().unwrap();
    }

    let start = Instant::now();
    let s = Store::open(&wal).unwrap();
    let elapsed = start.elapsed();
    assert_eq!(s.sstable_count(), 1);
    report_duration("recovery (open SSTable)", elapsed);
    black_box(s);

    std::fs::remove_dir_all(&dir).ok();
}

/// Full compaction of ten SSTables containing disjoint keys.
#[test]
#[ignore = "informational benchmark; run with --release --ignored --nocapture"]
fn perf_full_compaction() {
    let dir = tmp_dir("compaction");
    let wal = dir.join("kvdb.wal");
    let mut s = Store::open(&wal).unwrap();
    keep_all_writes_in_memtable(&mut s);
    s.set_compaction_threshold(0);

    const N: usize = 100_000;
    const TABLES: usize = 10;
    for table in 0..TABLES {
        let mut batch = WriteBatch::new();
        for i in table * (N / TABLES)..(table + 1) * (N / TABLES) {
            batch.set(key(i), value(i));
        }
        s.write_batch(batch).unwrap();
        s.flush().unwrap();
    }
    let before = storage_bytes(&dir);

    let start = Instant::now();
    s.compact().unwrap();
    let elapsed = start.elapsed();
    assert_eq!(s.sstable_count(), 1);
    report("compaction (10 to 1)", N, elapsed);
    report_storage("before compaction", before, N);
    report_storage("after compaction", storage_bytes(&dir), N);

    std::fs::remove_dir_all(&dir).ok();
}

/// MVCC GC cost and disk reduction for five versions of every key.
#[test]
#[ignore = "informational benchmark; run with --release --ignored --nocapture"]
fn perf_mvcc_retention_compaction() {
    let dir = tmp_dir("retention");
    let wal = dir.join("kvdb.wal");
    let mut s = Store::open(&wal).unwrap();
    keep_all_writes_in_memtable(&mut s);
    s.set_compaction_threshold(0);

    const KEYS: usize = 20_000;
    const VERSIONS: usize = 5;
    let mut boundaries = Vec::new();
    for version in 0..VERSIONS {
        let mut batch = WriteBatch::new();
        for i in 0..KEYS {
            batch.set(key(i), format!("version-{version}-{i}").into_bytes());
        }
        boundaries.push(s.write_batch(batch).unwrap());
        s.flush().unwrap();
    }
    let before = storage_bytes(&dir);

    let start = Instant::now();
    s.compact_with_retention(boundaries[VERSIONS - 2]).unwrap();
    let elapsed = start.elapsed();
    assert_eq!(s.sstable_count(), 1);
    report("retention GC (100k versions)", KEYS * VERSIONS, elapsed);
    report_storage("MVCC before retention", before, KEYS * VERSIONS);
    report_storage("MVCC after retention", storage_bytes(&dir), KEYS * VERSIONS);

    std::fs::remove_dir_all(&dir).ok();
}

/// Cost of materializing an immutable copy-on-snapshot from one SSTable.
#[test]
#[ignore = "informational benchmark; run with --release --ignored --nocapture"]
fn perf_snapshot_materialization() {
    let dir = tmp_dir("snapshot");
    let wal = dir.join("kvdb.wal");
    let mut s = Store::open(&wal).unwrap();
    keep_all_writes_in_memtable(&mut s);

    const N: usize = 100_000;
    let mut batch = WriteBatch::new();
    for i in 0..N {
        batch.set(key(i), value(i));
    }
    s.write_batch(batch).unwrap();
    s.flush().unwrap();

    let start = Instant::now();
    let snapshot = s.snapshot().unwrap();
    let elapsed = start.elapsed();
    assert_eq!(snapshot.len(), N);
    report("snapshot (copy 100k keys)", N, elapsed);
    black_box(snapshot);

    std::fs::remove_dir_all(&dir).ok();
}

/// Axum API-layer throughput without a TCP socket. This includes routing, Basic
/// auth, body handling, mutex locking, and Store lookup.
#[tokio::test(flavor = "current_thread")]
#[ignore = "informational benchmark; run with --release --ignored --nocapture"]
async fn perf_http_router_get() {
    let dir = tmp_dir("http-get");
    let wal = dir.join("kvdb.wal");
    let mut store = Store::open(&wal).unwrap();
    keep_all_writes_in_memtable(&mut store);

    const KEYS: usize = 10_000;
    const REQUESTS: usize = 50_000;
    let mut batch = WriteBatch::new();
    for i in 0..KEYS {
        batch.set(key(i), value(i));
    }
    store.write_batch(batch).unwrap();
    let app = router(AppState::new(store, "admin", "secret"));

    let start = Instant::now();
    for i in 0..REQUESTS {
        let request = Request::builder()
            .uri(format!("/v1/keys/key-{:08}", i % KEYS))
            .header("authorization", "Basic YWRtaW46c2VjcmV0")
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        black_box(response.into_body().collect().await.unwrap());
    }
    report("HTTP GET (router, no TCP)", REQUESTS, start.elapsed());

    std::fs::remove_dir_all(&dir).ok();
}

/// Axum PUT throughput with the default memtable and compaction thresholds.
/// Like the GET benchmark, this excludes TCP but includes routing, auth, body
/// handling, mutex locking, WAL writes, flushes, and automatic compaction.
#[tokio::test(flavor = "current_thread")]
#[ignore = "informational benchmark; run with --release --ignored --nocapture"]
async fn perf_http_router_put() {
    let dir = tmp_dir("http-put");
    let wal = dir.join("kvdb.wal");
    let app = router(AppState::new(Store::open(&wal).unwrap(), "admin", "secret"));

    const REQUESTS: usize = 20_000;
    let start = Instant::now();
    for i in 0..REQUESTS {
        let request = Request::builder()
            .method("PUT")
            .uri(format!("/v1/keys/key-{i:08}"))
            .header("authorization", "Basic YWRtaW46c2VjcmV0")
            .body(Body::from(format!("value-{i}")))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        black_box(response.into_body().collect().await.unwrap());
    }
    report("HTTP PUT (router, no TCP)", REQUESTS, start.elapsed());

    std::fs::remove_dir_all(&dir).ok();
}
