//! Informational performance tests — a zero-dependency micro-benchmark harness
//! built on [`std::time::Instant`].
//!
//! These are **not** pass/fail assertions on timing (which would be flaky in
//! CI); they print throughput and latency so you can eyeball regressions and
//! understand the cost of each operation and of the LSM read path (memtable hit
//! vs SSTable hit vs miss). They are `#[ignore]`d so `cargo test` stays fast.
//!
//! Run them, in release mode, with output shown:
//!
//! ```sh
//! cargo test --release --test perf -- --ignored --nocapture
//! ```

use std::hint::black_box;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use kvdb::store::Store;

fn tmp_dir(tag: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
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

/// Write throughput: durable `set` (each does a WAL append + flush-to-OS).
#[test]
#[ignore = "informational benchmark; run with --release --ignored --nocapture"]
fn perf_set_throughput() {
    let dir = tmp_dir("set");
    let wal = dir.join("kvdb.wal");
    let mut s = Store::open(&wal).unwrap();
    s.set_memtable_limit(usize::MAX); // isolate the write path from flushing

    const N: usize = 200_000;
    let start = Instant::now();
    for i in 0..N {
        s.set(key(i), value(i)).unwrap();
    }
    report("set (durable, no flush)", N, start.elapsed());

    std::fs::remove_dir_all(&dir).ok();
}

/// Read throughput from the in-memory memtable (no SSTable involved).
#[test]
#[ignore = "informational benchmark; run with --release --ignored --nocapture"]
fn perf_get_memtable_hit() {
    let dir = tmp_dir("get-mem");
    let wal = dir.join("kvdb.wal");
    let mut s = Store::open(&wal).unwrap();
    s.set_memtable_limit(usize::MAX);

    const N: usize = 100_000;
    for i in 0..N {
        s.set(key(i), value(i)).unwrap();
    }

    let start = Instant::now();
    for i in 0..N {
        black_box(s.get(&key(i)));
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
    s.set_memtable_limit(usize::MAX);

    const N: usize = 100_000;
    for i in 0..N {
        s.set(key(i), value(i)).unwrap();
    }
    s.flush().unwrap(); // push everything to one SSTable; memtable now empty

    let start = Instant::now();
    for i in 0..N {
        black_box(s.get(&key(i)));
    }
    report("get hit (sstable)", N, start.elapsed());

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

    const N: usize = 100_000;
    for i in 0..N {
        s.set(key(i), value(i)).unwrap();
    }

    let start = Instant::now();
    for i in 0..N {
        black_box(s.get(format!("missing-{i}").as_bytes()));
    }
    report("get miss (all levels)", N, start.elapsed());

    std::fs::remove_dir_all(&dir).ok();
}

/// Cost of flushing a full memtable to an SSTable, per entry.
#[test]
#[ignore = "informational benchmark; run with --release --ignored --nocapture"]
fn perf_flush_cost() {
    let dir = tmp_dir("flush");
    let wal = dir.join("kvdb.wal");
    let mut s = Store::open(&wal).unwrap();
    s.set_memtable_limit(usize::MAX);

    const N: usize = 200_000;
    for i in 0..N {
        s.set(key(i), value(i)).unwrap();
    }

    let start = Instant::now();
    s.flush().unwrap();
    report("flush (write sstable)", N, start.elapsed());

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
        s.set_memtable_limit(usize::MAX); // keep it all in the WAL (no flush)
        for i in 0..N {
            s.set(key(i), value(i)).unwrap();
        }
    }

    let start = Instant::now();
    let s = Store::open(&wal).unwrap();
    let elapsed = start.elapsed();
    assert_eq!(s.len(), N);
    report("recovery (replay wal)", N, elapsed);

    std::fs::remove_dir_all(&dir).ok();
}
