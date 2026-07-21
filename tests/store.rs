//! Integration tests for the storage engine.

use std::path::PathBuf;

use kvdb::limits::MAX_KEY_BYTES;
use kvdb::store::{Durability, Store, TransactionError, WriteBatch};

/// Returns a fresh, unique temp WAL path and removes any leftover from a
/// previous run with the same tag.
fn tmp_path(tag: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("kvdb-test-{tag}-{}.wal", std::process::id()));
    let _ = std::fs::remove_file(&p);
    p
}

/// A fresh, isolated directory holding one store's WAL + SSTables + manifest.
/// Using a per-test directory keeps flushed sibling files from colliding and
/// makes cleanup a single `remove_dir_all`.
fn tmp_dir(tag: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("kvdb-store-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn crc32(parts: &[&[u8]]) -> u32 {
    let mut crc = u32::MAX;
    for part in parts {
        for &byte in *part {
            crc ^= u32::from(byte);
            for _ in 0..8 {
                let mask = 0u32.wrapping_sub(crc & 1);
                crc = (crc >> 1) ^ (0xedb8_8320 & mask);
            }
        }
    }
    !crc
}

fn checksummed_manifest(body: &[u8]) -> Vec<u8> {
    let mut bytes = body.to_vec();
    bytes.extend_from_slice(format!("checksum={:08x}\n", crc32(&[body])).as_bytes());
    bytes
}

#[test]
fn set_get_delete() {
    let path = tmp_path("basic");
    let mut s = Store::open(&path).unwrap();
    s.set(b"a".to_vec(), b"1".to_vec()).unwrap();
    assert_eq!(s.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert!(s.delete(b"a").unwrap());
    assert_eq!(s.get(b"a").unwrap(), None);
    std::fs::remove_file(&path).unwrap();
}

#[test]
fn recovers_after_reopen() {
    let path = tmp_path("recover");
    {
        let mut s = Store::open(&path).unwrap();
        s.set(b"x".to_vec(), b"hello".to_vec()).unwrap();
        s.set(b"y".to_vec(), b"world".to_vec()).unwrap();
        s.delete(b"x").unwrap();
    }
    let s = Store::open(&path).unwrap();
    assert_eq!(s.get(b"x").unwrap(), None);
    assert_eq!(s.get(b"y").unwrap(), Some(b"world".to_vec()));
    assert_eq!(s.len().unwrap(), 1);
    std::fs::remove_file(&path).unwrap();
}

#[test]
fn flush_creates_sstable_and_truncates_wal() {
    let dir = tmp_dir("flush");
    let wal = dir.join("kvdb.wal");

    let mut s = Store::open(&wal).unwrap();
    s.set_memtable_limit(3); // flush once the memtable holds 3 entries

    s.set(b"a".to_vec(), b"1".to_vec()).unwrap();
    s.set(b"b".to_vec(), b"2".to_vec()).unwrap();
    assert_eq!(s.sstable_count(), 0); // not yet at the limit
    s.set(b"c".to_vec(), b"3".to_vec()).unwrap(); // hits the limit -> flush

    assert_eq!(s.sstable_count(), 1, "third insert should trigger a flush");
    // WAL was sealed (truncated) by the flush.
    assert_eq!(std::fs::metadata(&wal).unwrap().len(), 0);
    // A manifest now records the table.
    assert!(s.manifest_path().exists());

    // Values are served from the SSTable now that the memtable is empty.
    assert_eq!(s.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(s.get(b"b").unwrap(), Some(b"2".to_vec()));
    assert_eq!(s.get(b"c").unwrap(), Some(b"3".to_vec()));

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn tombstone_in_sstable_does_not_resurrect() {
    let dir = tmp_dir("tombstone");
    let wal = dir.join("kvdb.wal");

    let mut s = Store::open(&wal).unwrap();

    // First generation: write a key, then flush it into SSTable #1.
    s.set(b"k".to_vec(), b"v1".to_vec()).unwrap();
    s.flush().unwrap();
    assert_eq!(s.sstable_count(), 1);

    // Delete it (tombstone lands in the memtable), then flush into SSTable #2.
    assert!(s.delete(b"k").unwrap(), "delete should see the live value");
    s.flush().unwrap();
    assert_eq!(s.sstable_count(), 2);

    // The newer tombstone must shadow the older value, not resurrect it.
    assert_eq!(s.get(b"k").unwrap(), None);

    // And it must survive a reopen (tombstone persisted on disk).
    drop(s);
    let s2 = Store::open(&wal).unwrap();
    assert_eq!(s2.get(b"k").unwrap(), None);
    assert_eq!(s2.len().unwrap(), 0);

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn newer_value_shadows_older_sstable() {
    let dir = tmp_dir("shadow");
    let wal = dir.join("kvdb.wal");

    let mut s = Store::open(&wal).unwrap();
    s.set(b"k".to_vec(), b"old".to_vec()).unwrap();
    s.flush().unwrap();
    s.set(b"k".to_vec(), b"new".to_vec()).unwrap();
    s.flush().unwrap();

    assert_eq!(s.sstable_count(), 2);
    assert_eq!(s.get(b"k").unwrap(), Some(b"new".to_vec()));
    assert_eq!(s.len().unwrap(), 1, "duplicate key counts once");

    drop(s);
    let s2 = Store::open(&wal).unwrap();
    assert_eq!(s2.get(b"k").unwrap(), Some(b"new".to_vec()));

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn recovers_flushed_data_and_unflushed_wal_tail() {
    let dir = tmp_dir("mixed-recover");
    let wal = dir.join("kvdb.wal");

    {
        let mut s = Store::open(&wal).unwrap();
        s.set(b"flushed".to_vec(), b"on-disk".to_vec()).unwrap();
        s.flush().unwrap(); // -> SSTable, WAL truncated
        // This one stays only in the WAL (no flush).
        s.set(b"pending".to_vec(), b"in-wal".to_vec()).unwrap();
        assert!(std::fs::metadata(&wal).unwrap().len() > 0);
    }

    // Reopen: SSTable data + replayed WAL tail must both be present.
    let s = Store::open(&wal).unwrap();
    assert_eq!(s.get(b"flushed").unwrap(), Some(b"on-disk".to_vec()));
    assert_eq!(s.get(b"pending").unwrap(), Some(b"in-wal".to_vec()));
    assert_eq!(s.len().unwrap(), 2);

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn orphan_sstable_absent_from_manifest_is_ignored() {
    let dir = tmp_dir("orphan");
    let wal = dir.join("kvdb.wal");

    {
        let mut s = Store::open(&wal).unwrap();
        s.set(b"real".to_vec(), b"yes".to_vec()).unwrap();
        s.flush().unwrap(); // creates kvdb-000000.sst + manifest
    }

    // Simulate a crash mid-flush: an SSTable file exists on disk but was never
    // recorded in the manifest. Recovery must ignore it entirely.
    std::fs::write(dir.join("kvdb-000009.sst"), b"garbage not in manifest").unwrap();

    let s = Store::open(&wal).unwrap();
    assert_eq!(s.get(b"real").unwrap(), Some(b"yes".to_vec()));
    assert_eq!(s.len().unwrap(), 1);
    assert_eq!(s.sstable_count(), 1, "orphan table must not be loaded");

    std::fs::remove_dir_all(&dir).ok();
}

// ---- Edge cases: values, keys, tombstone accounting ------------------------

#[test]
fn empty_value_is_distinct_from_absent() {
    let path = tmp_path("empty-value");
    let mut s = Store::open(&path).unwrap();

    // An empty value is a real, present value — not the same as "missing".
    s.set(b"k".to_vec(), Vec::new()).unwrap();
    assert_eq!(s.get(b"k").unwrap(), Some(Vec::new()));
    assert_eq!(s.get(b"absent").unwrap(), None);
    assert_eq!(s.len().unwrap(), 1);

    std::fs::remove_file(&path).ok();
}

#[test]
fn binary_keys_and_values_roundtrip_through_flush() {
    let dir = tmp_dir("binary");
    let wal = dir.join("kvdb.wal");

    // Keys/values are arbitrary bytes: NUL, 0xFF, newlines, the length-prefix
    // sentinel bytes — nothing should be special-cased.
    let key = vec![0u8, 0xFF, b'\n', 1, 2, 3];
    let val = vec![0xDE, 0xAD, 0x00, 0xBE, 0xEF];

    let mut s = Store::open(&wal).unwrap();
    s.set(key.clone(), val.clone()).unwrap();
    s.flush().unwrap();
    assert_eq!(s.get(&key).unwrap(), Some(val.clone()));

    // Survives reopen (re-read from the SSTable).
    drop(s);
    let s2 = Store::open(&wal).unwrap();
    assert_eq!(s2.get(&key).unwrap(), Some(val));

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn overwrite_updates_value_and_keeps_count() {
    let path = tmp_path("overwrite");
    let mut s = Store::open(&path).unwrap();

    s.set(b"k".to_vec(), b"v1".to_vec()).unwrap();
    s.set(b"k".to_vec(), b"v2".to_vec()).unwrap();
    s.set(b"k".to_vec(), b"v3".to_vec()).unwrap();

    assert_eq!(s.get(b"k").unwrap(), Some(b"v3".to_vec()));
    assert_eq!(s.len().unwrap(), 1, "overwrites don't add keys");

    std::fs::remove_file(&path).ok();
}

#[test]
fn delete_absent_key_reports_false() {
    let path = tmp_path("del-absent");
    let mut s = Store::open(&path).unwrap();

    assert!(
        !s.delete(b"never").unwrap(),
        "deleting a missing key => false"
    );
    // A redundant delete of an already-deleted key is also false.
    s.set(b"k".to_vec(), b"v".to_vec()).unwrap();
    assert!(s.delete(b"k").unwrap());
    assert!(!s.delete(b"k").unwrap(), "second delete => false");

    std::fs::remove_file(&path).ok();
}

#[test]
fn delete_of_key_living_only_in_sstable_reports_true() {
    let dir = tmp_dir("del-sstable");
    let wal = dir.join("kvdb.wal");

    let mut s = Store::open(&wal).unwrap();
    s.set(b"k".to_vec(), b"v".to_vec()).unwrap();
    s.flush().unwrap(); // k now lives only in the SSTable; memtable is empty

    // delete must see the live value on disk and report true.
    assert!(s.delete(b"k").unwrap(), "delete of an on-disk key => true");
    assert_eq!(s.get(b"k").unwrap(), None);
    assert_eq!(s.len().unwrap(), 0);

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn large_value_roundtrips() {
    let dir = tmp_dir("large");
    let wal = dir.join("kvdb.wal");

    let big = vec![0xABu8; 1 << 20]; // 1 MiB
    let mut s = Store::open(&wal).unwrap();
    s.set(b"blob".to_vec(), big.clone()).unwrap();
    s.flush().unwrap();
    assert_eq!(s.get(b"blob").unwrap(), Some(big.clone()));

    drop(s);
    let s2 = Store::open(&wal).unwrap();
    assert_eq!(s2.get(b"blob").unwrap(), Some(big));

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn many_keys_across_many_flushes_all_recover() {
    let dir = tmp_dir("many-flush");
    let wal = dir.join("kvdb.wal");

    const N: usize = 500;
    {
        let mut s = Store::open(&wal).unwrap();
        s.set_memtable_limit(16); // force many flushes
        for i in 0..N {
            s.set(
                format!("key-{i:04}").into_bytes(),
                format!("val-{i}").into_bytes(),
            )
            .unwrap();
        }
        // Delete every 10th key to spread tombstones across tables.
        for i in (0..N).step_by(10) {
            s.delete(format!("key-{i:04}").as_bytes()).unwrap();
        }
        assert!(s.sstable_count() > 1, "should have flushed multiple tables");
    }

    // Reopen from disk only and verify the full logical state.
    let s = Store::open(&wal).unwrap();
    for i in 0..N {
        let key = format!("key-{i:04}");
        if i % 10 == 0 {
            assert_eq!(
                s.get(key.as_bytes()).unwrap(),
                None,
                "{key} should be deleted"
            );
        } else {
            assert_eq!(
                s.get(key.as_bytes()).unwrap(),
                Some(format!("val-{i}").into_bytes()),
                "{key} should survive"
            );
        }
    }
    assert_eq!(s.len().unwrap(), N - N.div_ceil(10));

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn sparse_index_reads_large_sstable_after_reopen() {
    let dir = tmp_dir("sparse-index");
    let wal = dir.join("kvdb.wal");

    const N: usize = 150; // spans three 64-record SSTable blocks
    {
        let mut s = Store::open(&wal).unwrap();
        s.set_memtable_limit(usize::MAX);
        for i in 0..N {
            s.set(
                format!("key-{i:04}").into_bytes(),
                format!("value-{i}").into_bytes(),
            )
            .unwrap();
        }
        s.flush().unwrap();
    }

    let s = Store::open(&wal).unwrap();
    for i in [0, 63, 64, 127, 128, N - 1] {
        assert_eq!(
            s.get(format!("key-{i:04}").as_bytes()).unwrap(),
            Some(format!("value-{i}").into_bytes())
        );
    }
    assert_eq!(s.get(b"key-0063x").unwrap(), None);
    assert_eq!(s.len().unwrap(), N);

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn compaction_keeps_current_values_and_historical_versions() {
    let dir = tmp_dir("compact");
    let wal = dir.join("kvdb.wal");

    let mut s = Store::open(&wal).unwrap();
    s.set(b"updated".to_vec(), b"old".to_vec()).unwrap();
    s.set(b"deleted".to_vec(), b"present".to_vec()).unwrap();
    s.set(b"stable".to_vec(), b"kept".to_vec()).unwrap();
    s.flush().unwrap();

    s.set(b"updated".to_vec(), b"new".to_vec()).unwrap();
    assert!(s.delete(b"deleted").unwrap());
    s.set(b"added".to_vec(), b"fresh".to_vec()).unwrap();
    s.flush().unwrap();
    assert_eq!(s.sstable_count(), 2);

    assert_eq!(s.get(b"updated").unwrap(), Some(b"new".to_vec()));
    assert_eq!(s.get(b"stable").unwrap(), Some(b"kept".to_vec()));
    assert!(s.sstable_cache_metrics().resident_blocks >= 2);

    s.compact().unwrap();
    assert_eq!(s.sstable_count(), 1);
    let cache = s.sstable_cache_metrics();
    assert_eq!(cache.open_files, 1);
    assert_eq!(cache.resident_blocks, 0);
    assert_eq!(cache.resident_bytes, 0);
    assert_eq!(s.get(b"updated").unwrap(), Some(b"new".to_vec()));
    assert_eq!(s.get(b"deleted").unwrap(), None);
    assert_eq!(s.get(b"stable").unwrap(), Some(b"kept".to_vec()));
    assert_eq!(s.get(b"added").unwrap(), Some(b"fresh".to_vec()));
    assert_eq!(s.len().unwrap(), 3);
    assert_eq!(s.get_at(b"updated", 1).unwrap(), Some(b"old".to_vec()));
    assert_eq!(s.get_at(b"updated", 4).unwrap(), Some(b"new".to_vec()));
    assert_eq!(s.get_at(b"deleted", 2).unwrap(), Some(b"present".to_vec()));
    assert_eq!(s.get_at(b"deleted", 5).unwrap(), None);

    // The compacted table survives a restart, and subsequent flushes preserve
    // its sequence naming and read precedence.
    s.set(b"later".to_vec(), b"tail".to_vec()).unwrap();
    s.flush().unwrap();
    assert_eq!(s.sstable_count(), 2);
    drop(s);

    let s = Store::open(&wal).unwrap();
    assert_eq!(s.get(b"updated").unwrap(), Some(b"new".to_vec()));
    assert_eq!(s.get(b"deleted").unwrap(), None);
    assert_eq!(s.get(b"later").unwrap(), Some(b"tail".to_vec()));
    assert_eq!(s.len().unwrap(), 4);
    assert_eq!(s.get_at(b"updated", 1).unwrap(), Some(b"old".to_vec()));
    assert_eq!(s.get_at(b"deleted", 2).unwrap(), Some(b"present".to_vec()));

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn compaction_retains_history_behind_a_current_tombstone() {
    let dir = tmp_dir("compact-empty");
    let wal = dir.join("kvdb.wal");

    let mut s = Store::open(&wal).unwrap();
    s.set(b"gone".to_vec(), b"value".to_vec()).unwrap();
    s.flush().unwrap();
    assert!(s.delete(b"gone").unwrap());
    s.flush().unwrap();

    s.compact().unwrap();
    assert_eq!(s.sstable_count(), 1);
    assert_eq!(s.get(b"gone").unwrap(), None);
    assert_eq!(s.get_at(b"gone", 1).unwrap(), Some(b"value".to_vec()));
    drop(s);

    let s = Store::open(&wal).unwrap();
    assert_eq!(s.sstable_count(), 1);
    assert_eq!(s.get(b"gone").unwrap(), None);
    assert_eq!(s.get_at(b"gone", 1).unwrap(), Some(b"value".to_vec()));
    let historical = s.snapshot_at(1).unwrap();
    assert_eq!(historical.sequence(), 1);
    assert_eq!(historical.get(b"gone"), Some(b"value".as_slice()));
    assert!(s.snapshot_at(3).is_err());

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn compaction_retention_keeps_boundary_state_and_survives_reopen() {
    let dir = tmp_dir("history-retention");
    let wal = dir.join("kvdb.wal");

    {
        let mut s = Store::open(&wal).unwrap();
        s.set(b"unchanged".to_vec(), b"first".to_vec()).unwrap(); // 1
        s.set(b"changed".to_vec(), b"old".to_vec()).unwrap(); // 2
        s.delete(b"deleted").unwrap(); // 3
        s.flush().unwrap();
        s.set(b"changed".to_vec(), b"new".to_vec()).unwrap(); // 4
        s.set(b"later".to_vec(), b"value".to_vec()).unwrap(); // 5

        s.compact_with_retention(3).unwrap();
        assert_eq!(s.history_start_sequence(), 3);
        assert_eq!(s.get_at(b"changed", 3).unwrap(), Some(b"old".to_vec()));
        assert_eq!(s.get_at(b"changed", 4).unwrap(), Some(b"new".to_vec()));
        assert_eq!(s.get_at(b"unchanged", 3).unwrap(), Some(b"first".to_vec()));
        assert_eq!(s.get_at(b"deleted", 3).unwrap(), None);
        assert_eq!(s.get(b"deleted").unwrap(), None);
        assert_eq!(
            s.get_at(b"changed", 2).unwrap_err().kind(),
            std::io::ErrorKind::InvalidInput
        );
        assert_eq!(
            s.snapshot_at(2).unwrap_err().kind(),
            std::io::ErrorKind::InvalidInput
        );
        assert!(s.compact_with_retention(2).is_err());
        assert!(s.compact_with_retention(6).is_err());
    }

    let s = Store::open(&wal).unwrap();
    assert_eq!(s.history_start_sequence(), 3);
    assert_eq!(s.get_at(b"changed", 3).unwrap(), Some(b"old".to_vec()));
    assert_eq!(s.get_at(b"changed", 4).unwrap(), Some(b"new".to_vec()));
    assert_eq!(s.get_at(b"later", 5).unwrap(), Some(b"value".to_vec()));
    assert!(s.get_at(b"unchanged", 2).is_err());

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn retention_boundary_persists_when_compaction_removes_every_key() {
    let dir = tmp_dir("empty-history-retention");
    let wal = dir.join("kvdb.wal");

    {
        let mut s = Store::open(&wal).unwrap();
        s.set(b"gone".to_vec(), b"value".to_vec()).unwrap();
        s.delete(b"gone").unwrap();
        s.compact_with_retention(2).unwrap();
        assert_eq!(s.sstable_count(), 0);
        assert_eq!(s.history_start_sequence(), 2);
    }

    let s = Store::open(&wal).unwrap();
    assert_eq!(s.sstable_count(), 0);
    assert_eq!(s.current_sequence(), 2);
    assert_eq!(s.history_start_sequence(), 2);
    assert!(s.snapshot_at(1).is_err());
    assert!(s.is_empty().unwrap());

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn later_flush_and_automatic_compaction_preserve_retention_boundary() {
    let dir = tmp_dir("retention-auto-compact");
    let wal = dir.join("kvdb.wal");

    {
        let mut s = Store::open(&wal).unwrap();
        s.set_memtable_limit(usize::MAX);
        s.set(b"key".to_vec(), b"v1".to_vec()).unwrap();
        s.flush().unwrap();
        s.set(b"key".to_vec(), b"v2".to_vec()).unwrap();
        s.flush().unwrap();
        s.compact_with_retention(2).unwrap();

        s.set_compaction_threshold(2);
        s.set_memtable_limit(1);
        s.set(b"key".to_vec(), b"v3".to_vec()).unwrap();
        assert!(s.compaction_metrics().in_progress);
        s.wait_for_background_compaction().unwrap();
        assert_eq!(s.sstable_count(), 1);
        assert_eq!(s.history_start_sequence(), 2);
        assert_eq!(s.get_at(b"key", 2).unwrap(), Some(b"v2".to_vec()));
        assert_eq!(s.get(b"key").unwrap(), Some(b"v3".to_vec()));
        assert!(s.get_at(b"key", 1).is_err());
    }

    let s = Store::open(&wal).unwrap();
    assert_eq!(s.history_start_sequence(), 2);
    assert_eq!(s.get_at(b"key", 2).unwrap(), Some(b"v2".to_vec()));
    assert_eq!(s.get(b"key").unwrap(), Some(b"v3".to_vec()));

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn automatic_compaction_runs_at_threshold_and_survives_reopen() {
    let dir = tmp_dir("compact-auto");
    let wal = dir.join("kvdb.wal");

    let mut s = Store::open(&wal).unwrap();
    s.set_memtable_limit(1);
    s.set_compaction_threshold(3);

    s.set(b"a".to_vec(), b"one".to_vec()).unwrap();
    s.set(b"b".to_vec(), b"two".to_vec()).unwrap();
    assert_eq!(s.sstable_count(), 2);
    s.set(b"c".to_vec(), b"three".to_vec()).unwrap();
    assert!(s.compaction_metrics().in_progress);
    s.wait_for_background_compaction().unwrap();
    assert_eq!(s.sstable_count(), 1, "third table triggers compaction");

    assert!(s.delete(b"a").unwrap());
    assert_eq!(s.sstable_count(), 2);
    s.set(b"d".to_vec(), b"four".to_vec()).unwrap();
    s.wait_for_background_compaction().unwrap();
    assert_eq!(s.sstable_count(), 1, "threshold triggers again");
    assert_eq!(s.get(b"a").unwrap(), None);
    assert_eq!(s.get_at(b"a", 1).unwrap(), Some(b"one".to_vec()));
    assert_eq!(s.get(b"b").unwrap(), Some(b"two".to_vec()));
    assert_eq!(s.get(b"c").unwrap(), Some(b"three".to_vec()));
    assert_eq!(s.get(b"d").unwrap(), Some(b"four".to_vec()));
    drop(s);

    let s = Store::open(&wal).unwrap();
    assert_eq!(s.sstable_count(), 1);
    assert_eq!(s.current_sequence(), 5);
    assert_eq!(s.get(b"a").unwrap(), None);
    assert_eq!(s.get_at(b"a", 1).unwrap(), Some(b"one".to_vec()));
    assert_eq!(s.get(b"d").unwrap(), Some(b"four".to_vec()));

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn background_compaction_streams_while_new_writes_continue() {
    let dir = tmp_dir("compact-background");
    let wal = dir.join("kvdb.wal");
    let mut s = Store::open(&wal).unwrap();
    s.set_memtable_limit(usize::MAX);
    s.set_compaction_threshold(3);

    const KEYS: usize = 200;
    const VALUE_BYTES: usize = 2 * 1024;
    for generation in 0..3u8 {
        for key in 0..KEYS {
            s.set(
                format!("key-{key:04}").into_bytes(),
                vec![generation; VALUE_BYTES],
            )
            .unwrap();
        }
        s.flush().unwrap();
    }

    assert_eq!(s.sstable_count(), 3);
    assert!(s.compaction_metrics().in_progress);

    // This foreground flush is newer than every compaction input table and must
    // remain as a suffix regardless of when the replacement is published.
    s.set_memtable_limit(1);
    s.set(b"key-0000".to_vec(), b"foreground-tail".to_vec())
        .unwrap();
    s.wait_for_background_compaction().unwrap();
    assert_eq!(s.sstable_count(), 2);
    assert_eq!(
        s.get(b"key-0000").unwrap(),
        Some(b"foreground-tail".to_vec())
    );
    assert_eq!(
        s.get_at(b"key-0000", 1).unwrap(),
        Some(vec![0; VALUE_BYTES])
    );
    assert_eq!(
        s.get_at(b"key-0000", 201).unwrap(),
        Some(vec![1; VALUE_BYTES])
    );

    let metrics = s.compaction_metrics();
    assert_eq!(metrics.runs_started, 1);
    assert_eq!(metrics.runs_completed, 1);
    assert_eq!(metrics.runs_failed, 0);
    assert_eq!(metrics.input_tables, 3);
    assert_eq!(metrics.input_versions, (KEYS * 3) as u64);
    assert_eq!(metrics.output_versions, (KEYS * 3) as u64);
    assert!(metrics.input_bytes > metrics.output_bytes);
    assert!(metrics.peak_buffer_bytes < 128 * 1024);

    drop(s);
    let s = Store::open(&wal).unwrap();
    assert_eq!(
        s.get(b"key-0000").unwrap(),
        Some(b"foreground-tail".to_vec())
    );
    assert_eq!(s.get(b"key-0199").unwrap(), Some(vec![2; VALUE_BYTES]));

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn automatic_compaction_can_be_disabled() {
    let dir = tmp_dir("compact-disabled");
    let wal = dir.join("kvdb.wal");

    let mut s = Store::open(&wal).unwrap();
    s.set_memtable_limit(1);
    s.set_compaction_threshold(0);
    for i in 0..10 {
        s.set(format!("key-{i}").into_bytes(), b"value".to_vec())
            .unwrap();
    }
    assert_eq!(s.sstable_count(), 10);

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn atomic_batch_applies_in_order_with_one_sequence_number() {
    let path = tmp_path("batch");
    let mut s = Store::open(&path).unwrap();
    let mut batch = WriteBatch::new();
    batch
        .set(b"a".to_vec(), b"first".to_vec())
        .set(b"b".to_vec(), b"second".to_vec())
        .delete(b"a".to_vec())
        .set(b"a".to_vec(), b"last".to_vec());

    assert_eq!(s.write_batch(batch).unwrap(), 1);
    assert_eq!(s.current_sequence(), 1);
    assert_eq!(s.get(b"a").unwrap(), Some(b"last".to_vec()));
    assert_eq!(s.get(b"b").unwrap(), Some(b"second".to_vec()));

    // An empty batch is a no-op and does not consume a sequence number.
    assert_eq!(s.write_batch(WriteBatch::new()).unwrap(), 1);
    assert_eq!(s.current_sequence(), 1);

    drop(s);
    let s = Store::open(&path).unwrap();
    assert_eq!(s.current_sequence(), 1);
    assert_eq!(s.get(b"a").unwrap(), Some(b"last".to_vec()));
    assert_eq!(s.get(b"b").unwrap(), Some(b"second".to_vec()));

    std::fs::remove_file(&path).ok();
}

#[test]
fn write_group_assigns_one_sequence_per_logical_batch() {
    let path = tmp_path("write-group");
    let mut store = Store::open(&path).unwrap();
    let mut first = WriteBatch::new();
    first.set(b"key".to_vec(), b"one".to_vec());
    let mut second = WriteBatch::new();
    second.set(b"key".to_vec(), b"two".to_vec());
    let empty = WriteBatch::new();

    assert_eq!(
        store.write_group(vec![first, empty, second]).unwrap(),
        vec![1, 1, 2]
    );
    assert_eq!(store.current_sequence(), 2);
    assert_eq!(store.get_at(b"key", 1).unwrap(), Some(b"one".to_vec()));
    assert_eq!(store.get(b"key").unwrap(), Some(b"two".to_vec()));

    drop(store);
    let store = Store::open(&path).unwrap();
    assert_eq!(store.current_sequence(), 2);
    assert_eq!(store.get_at(b"key", 1).unwrap(), Some(b"one".to_vec()));
    assert_eq!(store.get(b"key").unwrap(), Some(b"two".to_vec()));

    std::fs::remove_file(&path).ok();
}

#[test]
fn write_group_validates_every_batch_before_writing() {
    let path = tmp_path("write-group-invalid");
    let mut store = Store::open(&path).unwrap();
    let mut valid = WriteBatch::new();
    valid.set(b"valid".to_vec(), b"value".to_vec());
    let mut invalid = WriteBatch::new();
    invalid.set(vec![b'x'; MAX_KEY_BYTES + 1], b"value".to_vec());

    let error = store.write_group(vec![valid, invalid]).unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert_eq!(store.current_sequence(), 0);
    assert_eq!(store.get(b"valid").unwrap(), None);
    assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
    assert!(!store.is_poisoned());

    std::fs::remove_file(&path).ok();
}

#[test]
fn torn_batch_is_discarded_without_partial_application() {
    let path = tmp_path("batch-torn");
    {
        let mut s = Store::open(&path).unwrap();
        let mut batch = WriteBatch::new();
        batch
            .set(b"a".to_vec(), b"one".to_vec())
            .set(b"b".to_vec(), b"two".to_vec());
        s.write_batch(batch).unwrap();
    }

    let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    let len = file.metadata().unwrap().len();
    file.set_len(len - 1).unwrap();

    let s = Store::open(&path).unwrap();
    assert_eq!(s.current_sequence(), 0);
    assert_eq!(s.get(b"a").unwrap(), None);
    assert_eq!(s.get(b"b").unwrap(), None);

    std::fs::remove_file(&path).ok();
}

#[test]
fn sequence_survives_flush_and_duplicate_wal_replay() {
    let dir = tmp_dir("sequence");
    let wal = dir.join("kvdb.wal");

    let wal_before_flush = {
        let mut s = Store::open(&wal).unwrap();
        let mut batch = WriteBatch::new();
        batch.set(b"persisted".to_vec(), b"value".to_vec());
        assert_eq!(s.write_batch(batch).unwrap(), 1);
        let wal_bytes = std::fs::read(&wal).unwrap();
        s.flush().unwrap();
        assert_eq!(s.current_sequence(), 1);
        wal_bytes
    };

    // Simulate a crash after publishing the manifest but before truncating the
    // WAL. The duplicate sequence is already durable in the SSTable and skipped.
    std::fs::write(&wal, wal_before_flush).unwrap();
    let mut s = Store::open(&wal).unwrap();
    assert_eq!(s.current_sequence(), 1);
    assert_eq!(s.get(b"persisted").unwrap(), Some(b"value".to_vec()));
    s.set(b"next".to_vec(), b"two".to_vec()).unwrap();
    assert_eq!(s.current_sequence(), 2);

    drop(s);
    let s = Store::open(&wal).unwrap();
    assert_eq!(s.current_sequence(), 2);
    assert_eq!(s.get(b"next").unwrap(), Some(b"two".to_vec()));

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn compaction_does_not_mark_unflushed_wal_as_durable() {
    let dir = tmp_dir("compact-wal-tail");
    let wal = dir.join("kvdb.wal");

    {
        let mut s = Store::open(&wal).unwrap();
        s.set(b"flushed".to_vec(), b"sstable".to_vec()).unwrap();
        s.flush().unwrap();
        s.set(b"pending".to_vec(), b"wal".to_vec()).unwrap();
        assert_eq!(s.current_sequence(), 2);
        s.compact().unwrap();
    }

    let s = Store::open(&wal).unwrap();
    assert_eq!(s.current_sequence(), 2);
    assert_eq!(s.get(b"flushed").unwrap(), Some(b"sstable".to_vec()));
    assert_eq!(s.get(b"pending").unwrap(), Some(b"wal".to_vec()));

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn snapshot_remains_stable_after_writes_flush_and_compaction() {
    let dir = tmp_dir("snapshot");
    let wal = dir.join("kvdb.wal");

    let mut s = Store::open(&wal).unwrap();
    s.set(b"changing".to_vec(), b"old".to_vec()).unwrap();
    s.set(b"kept".to_vec(), b"visible".to_vec()).unwrap();
    s.set(b"gone".to_vec(), b"temporary".to_vec()).unwrap();
    assert!(s.delete(b"gone").unwrap());
    s.flush().unwrap();

    let snapshot = s.snapshot().unwrap();
    assert_eq!(snapshot.sequence(), 4);
    assert_eq!(snapshot.len(), 2);
    assert_eq!(snapshot.get(b"changing"), Some(b"old".as_slice()));
    assert_eq!(snapshot.get(b"kept"), Some(b"visible".as_slice()));
    assert_eq!(snapshot.get(b"gone"), None);

    s.set(b"changing".to_vec(), b"new".to_vec()).unwrap();
    assert!(s.delete(b"kept").unwrap());
    s.set(b"added".to_vec(), b"later".to_vec()).unwrap();
    s.flush().unwrap();
    s.compact().unwrap();

    assert_eq!(snapshot.sequence(), 4);
    assert_eq!(snapshot.get(b"changing"), Some(b"old".as_slice()));
    assert_eq!(snapshot.get(b"kept"), Some(b"visible".as_slice()));
    assert_eq!(snapshot.get(b"added"), None);

    assert_eq!(s.get(b"changing").unwrap(), Some(b"new".to_vec()));
    assert_eq!(s.get(b"kept").unwrap(), None);
    assert_eq!(s.get(b"added").unwrap(), Some(b"later".to_vec()));

    drop(s);
    let s = Store::open(&wal).unwrap();
    let current = s.snapshot().unwrap();
    assert_eq!(current.sequence(), 7);
    assert_eq!(current.get(b"changing"), Some(b"new".as_slice()));
    assert_eq!(current.get(b"kept"), None);
    assert_eq!(current.get(b"added"), Some(b"later".as_slice()));

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn optimistic_transaction_reads_its_writes_and_commits_atomically() {
    let path = tmp_path("transaction");
    let mut s = Store::open(&path).unwrap();
    s.set(b"balance".to_vec(), b"100".to_vec()).unwrap();

    let mut transaction = s.begin_transaction().unwrap();
    assert_eq!(transaction.base_sequence(), 1);
    assert_eq!(transaction.get(b"balance"), Some(b"100".as_slice()));
    transaction
        .set(b"balance".to_vec(), b"90".to_vec())
        .set(b"audit".to_vec(), b"withdraw 10".to_vec())
        .delete(b"temporary".to_vec());
    assert_eq!(transaction.get(b"balance"), Some(b"90".as_slice()));
    assert_eq!(transaction.get(b"audit"), Some(b"withdraw 10".as_slice()));
    assert_eq!(transaction.get(b"temporary"), None);

    // Uncommitted writes are private to the transaction.
    assert_eq!(s.get(b"balance").unwrap(), Some(b"100".to_vec()));
    assert_eq!(s.get(b"audit").unwrap(), None);

    assert_eq!(s.commit_transaction(transaction).unwrap(), 2);
    assert_eq!(s.get(b"balance").unwrap(), Some(b"90".to_vec()));
    assert_eq!(s.get(b"audit").unwrap(), Some(b"withdraw 10".to_vec()));
    drop(s);

    let s = Store::open(&path).unwrap();
    assert_eq!(s.current_sequence(), 2);
    assert_eq!(s.get(b"balance").unwrap(), Some(b"90".to_vec()));
    assert_eq!(s.get(b"audit").unwrap(), Some(b"withdraw 10".to_vec()));

    std::fs::remove_file(&path).ok();
}

#[test]
fn optimistic_transaction_conflict_has_no_partial_writes() {
    let path = tmp_path("transaction-conflict");
    let mut s = Store::open(&path).unwrap();
    s.set(b"key".to_vec(), b"snapshot".to_vec()).unwrap();

    let mut transaction = s.begin_transaction().unwrap();
    transaction
        .set(b"key".to_vec(), b"transaction".to_vec())
        .set(b"only-in-transaction".to_vec(), b"hidden".to_vec());

    s.set(b"key".to_vec(), b"concurrent".to_vec()).unwrap();
    assert_eq!(transaction.get(b"key"), Some(b"transaction".as_slice()));

    let error = s.commit_transaction(transaction).unwrap_err();
    assert!(matches!(
        error,
        TransactionError::Conflict {
            expected: 1,
            actual: 2,
            ..
        }
    ));
    assert_eq!(s.current_sequence(), 2);
    assert_eq!(s.get(b"key").unwrap(), Some(b"concurrent".to_vec()));
    assert_eq!(s.get(b"only-in-transaction").unwrap(), None);

    drop(s);
    let s = Store::open(&path).unwrap();
    assert_eq!(s.current_sequence(), 2);
    assert_eq!(s.get(b"key").unwrap(), Some(b"concurrent".to_vec()));
    assert_eq!(s.get(b"only-in-transaction").unwrap(), None);

    std::fs::remove_file(&path).ok();
}

#[test]
fn independent_transactions_can_both_commit() {
    let path = tmp_path("transaction-independent");
    let mut s = Store::open(&path).unwrap();
    let mut left = s.begin_transaction().unwrap();
    let mut right = s.begin_transaction().unwrap();
    left.set(b"left".to_vec(), b"one".to_vec());
    right.set(b"right".to_vec(), b"two".to_vec());

    assert_eq!(s.commit_transaction(left).unwrap(), 1);
    assert_eq!(s.commit_transaction(right).unwrap(), 2);
    assert_eq!(s.get(b"left").unwrap(), Some(b"one".to_vec()));
    assert_eq!(s.get(b"right").unwrap(), Some(b"two".to_vec()));

    std::fs::remove_file(&path).ok();
}

#[test]
fn transaction_read_conflict_detects_a_key_added_after_snapshot() {
    let path = tmp_path("transaction-read-conflict");
    let mut s = Store::open(&path).unwrap();
    let mut transaction = s.begin_transaction().unwrap();
    assert_eq!(transaction.get(b"missing"), None);
    transaction.set(b"derived".to_vec(), b"value".to_vec());

    s.set(b"missing".to_vec(), b"now-present".to_vec()).unwrap();
    let error = s.commit_transaction(transaction).unwrap_err();
    assert!(matches!(
        error,
        TransactionError::Conflict { key, .. } if key == b"missing"
    ));
    assert_eq!(s.get(b"derived").unwrap(), None);

    std::fs::remove_file(&path).ok();
}

#[test]
fn transaction_conflicts_when_a_read_value_changes_and_changes_back() {
    let path = tmp_path("transaction-aba-conflict");
    let mut s = Store::open(&path).unwrap();
    s.set(b"key".to_vec(), b"original".to_vec()).unwrap();

    let mut transaction = s.begin_transaction().unwrap();
    assert_eq!(transaction.get(b"key"), Some(b"original".as_slice()));
    transaction.set(b"derived".to_vec(), b"value".to_vec());

    s.set(b"key".to_vec(), b"temporary".to_vec()).unwrap();
    s.set(b"key".to_vec(), b"original".to_vec()).unwrap();
    let error = s.commit_transaction(transaction).unwrap_err();
    assert!(matches!(
        error,
        TransactionError::Conflict {
            expected: 1,
            actual: 3,
            ..
        }
    ));
    assert_eq!(s.get(b"derived").unwrap(), None);

    std::fs::remove_file(&path).ok();
}

#[test]
fn manifest_rejects_history_boundary_newer_than_durable_sequence() {
    let dir = tmp_dir("bad-history-boundary");
    let wal = dir.join("kvdb.wal");
    std::fs::write(
        dir.join("kvdb.manifest"),
        checksummed_manifest(b"sequence=4\nhistory_start=5\n"),
    )
    .unwrap();

    let error = match Store::open(&wal) {
        Ok(_) => panic!("expected invalid history boundary to fail"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn manifest_checksum_corruption_is_rejected() {
    let dir = tmp_dir("manifest-checksum");
    let wal = dir.join("kvdb.wal");
    {
        let mut store = Store::open(&wal).unwrap();
        store.set(b"key".to_vec(), b"value".to_vec()).unwrap();
        store.flush().unwrap();
    }

    let manifest = dir.join("kvdb.manifest");
    let mut bytes = std::fs::read(&manifest).unwrap();
    let sequence = bytes
        .windows(b"sequence=1".len())
        .position(|window| window == b"sequence=1")
        .unwrap();
    bytes[sequence + b"sequence=".len()] = b'2';
    std::fs::write(&manifest, bytes).unwrap();

    let error = match Store::open(&wal) {
        Ok(_) => panic!("expected manifest checksum corruption to fail"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    assert!(error.to_string().contains("checksum mismatch"));
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn corrupted_live_sstable_is_rejected_on_reopen() {
    use std::io::{Seek, SeekFrom, Write};

    let dir = tmp_dir("corrupt-sstable");
    let wal = dir.join("kvdb.wal");
    {
        let mut s = Store::open(&wal).unwrap();
        s.set(b"key".to_vec(), b"value".to_vec()).unwrap();
        s.flush().unwrap();
    }

    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(dir.join("kvdb-000000.sst"))
        .unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();
    file.write_all(b"BROKEN!!").unwrap();
    drop(file);

    let error = match Store::open(&wal) {
        Ok(_) => panic!("expected corrupted SSTable to fail"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn unknown_wal_opcode_is_a_hard_error() {
    let path = tmp_path("bad-opcode");
    let payload = [0x07u8];
    let length = (payload.len() as u32).to_be_bytes();
    let mut frame = b"KVWL".to_vec();
    frame.extend_from_slice(&length);
    frame.extend_from_slice(&payload);
    frame.extend_from_slice(&crc32(&[b"KVWL", &length, &payload]).to_be_bytes());
    std::fs::write(&path, frame).unwrap();

    let err = match Store::open(&path) {
        Ok(_) => panic!("expected an error for an unknown WAL op code"),
        Err(e) => e,
    };
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);

    std::fs::remove_file(&path).ok();
}

#[test]
fn torn_wal_tail_is_dropped_but_prior_records_survive() {
    let path = tmp_path("torn-tail");

    // Write one clean frame, then append a partial frame header to simulate a
    // crash before the next record's payload was written.
    {
        let mut s = Store::open(&path).unwrap();
        s.set(b"good".to_vec(), b"kept".to_vec()).unwrap();
    }
    {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        f.write_all(b"KVWL\x00").unwrap();
    }

    // Recovery physically drops the torn tail, so a later append remains
    // reachable when the WAL is reopened a second time.
    let mut s = Store::open(&path).unwrap();
    assert_eq!(s.get(b"good").unwrap(), Some(b"kept".to_vec()));
    s.set(b"later".to_vec(), b"survives".to_vec()).unwrap();
    drop(s);

    let s = Store::open(&path).unwrap();
    assert_eq!(s.get(b"good").unwrap(), Some(b"kept".to_vec()));
    assert_eq!(s.get(b"later").unwrap(), Some(b"survives".to_vec()));
    assert_eq!(s.len().unwrap(), 2);

    std::fs::remove_file(&path).ok();
}

#[test]
fn second_writer_is_rejected_until_first_store_closes() {
    let dir = tmp_dir("single-writer");
    let wal = dir.join("kvdb.wal");
    let first = Store::open(&wal).unwrap();

    let error = match Store::open(&wal) {
        Ok(_) => panic!("a second writer unexpectedly acquired the store"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);

    drop(first);
    Store::open(&wal).unwrap();
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn hot_key_versions_trigger_a_flush() {
    let dir = tmp_dir("hot-key-limit");
    let wal = dir.join("kvdb.wal");
    let mut store = Store::open(&wal).unwrap();
    store.set_durability(Durability::Buffered);
    store.set_memtable_limit(usize::MAX);
    store.set_memtable_bytes_limit(usize::MAX);
    store.set_memtable_versions_limit(3);
    store.set_wal_bytes_limit(u64::MAX);

    for value in [b"one".as_slice(), b"two", b"three"] {
        store.set(b"hot".to_vec(), value.to_vec()).unwrap();
    }

    assert_eq!(store.sstable_count(), 1);
    assert_eq!(store.get(b"hot").unwrap(), Some(b"three".to_vec()));
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn failed_flush_poisons_writes_but_wal_recovers_after_reopen() {
    let dir = tmp_dir("poisoned-flush");
    let wal = dir.join("kvdb.wal");
    let mut store = Store::open(&wal).unwrap();
    store.set(b"persisted".to_vec(), b"value".to_vec()).unwrap();

    let manifest_tmp = dir.join("kvdb.manifest.tmp");
    std::fs::create_dir(&manifest_tmp).unwrap();
    assert!(store.flush().is_err());
    assert!(store.is_poisoned());
    let error = store
        .set(b"rejected".to_vec(), b"value".to_vec())
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::Other);

    drop(store);
    std::fs::remove_dir(&manifest_tmp).unwrap();
    let store = Store::open(&wal).unwrap();
    assert_eq!(store.get(b"persisted").unwrap(), Some(b"value".to_vec()));
    assert_eq!(store.get(b"rejected").unwrap(), None);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn corruption_encountered_after_open_is_returned_by_get() {
    use std::io::{Seek, SeekFrom, Write};

    let dir = tmp_dir("late-corruption");
    let wal = dir.join("kvdb.wal");
    let mut store = Store::open(&wal).unwrap();
    store.set(b"key".to_vec(), b"value".to_vec()).unwrap();
    store.flush().unwrap();

    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(dir.join("kvdb-000000.sst"))
        .unwrap();
    file.seek(SeekFrom::Start(8)).unwrap();
    file.write_all(&u32::MAX.to_be_bytes()).unwrap();
    drop(file);

    let error = store.get(b"key").unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn oversized_input_is_rejected_without_poisoning_the_store() {
    let path = tmp_path("oversized-input");
    let mut store = Store::open(&path).unwrap();
    let error = store
        .set(vec![0; MAX_KEY_BYTES + 1], b"value".to_vec())
        .unwrap_err();

    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert!(!store.is_poisoned());
    store.set(b"valid".to_vec(), b"value".to_vec()).unwrap();
    std::fs::remove_file(&path).ok();
}

#[test]
fn obsolete_unframed_wal_is_rejected() {
    let path = tmp_path("unframed-wal");
    let mut bytes = vec![1u8];
    bytes.extend_from_slice(&1u64.to_be_bytes());
    bytes.extend_from_slice(&u32::MAX.to_be_bytes());
    std::fs::write(&path, bytes).unwrap();

    let error = match Store::open(&path) {
        Ok(_) => panic!("expected obsolete unframed WAL to fail"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    std::fs::remove_file(&path).ok();
}

#[test]
fn wal_checksum_corruption_is_a_hard_error() {
    let path = tmp_path("wal-checksum");
    {
        let mut store = Store::open(&path).unwrap();
        store.set(b"key".to_vec(), b"value".to_vec()).unwrap();
    }
    let mut bytes = std::fs::read(&path).unwrap();
    let payload_byte = bytes.len() - 5;
    bytes[payload_byte] ^= 1;
    std::fs::write(&path, bytes).unwrap();

    let error = match Store::open(&path) {
        Ok(_) => panic!("expected WAL checksum corruption to fail"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    std::fs::remove_file(&path).ok();
}

#[test]
fn invalid_wal_frame_magic_is_a_hard_error() {
    let path = tmp_path("wal-magic");
    {
        let mut store = Store::open(&path).unwrap();
        store.set(b"key".to_vec(), b"value".to_vec()).unwrap();
    }
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[3] = b'X';
    std::fs::write(&path, bytes).unwrap();

    let error = match Store::open(&path) {
        Ok(_) => panic!("expected invalid WAL frame magic to fail"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    std::fs::remove_file(&path).ok();
}

#[test]
fn oversized_framed_wal_length_is_rejected_before_allocation() {
    let path = tmp_path("oversized-framed-wal");
    let mut bytes = b"KVWL".to_vec();
    bytes.extend_from_slice(&u32::MAX.to_be_bytes());
    std::fs::write(&path, bytes).unwrap();

    let error = match Store::open(&path) {
        Ok(_) => panic!("expected oversized WAL frame to fail"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    std::fs::remove_file(&path).ok();
}

#[test]
fn torn_second_wal_frame_is_discarded_and_future_appends_survive() {
    let path = tmp_path("torn-second-frame");
    {
        let mut store = Store::open(&path).unwrap();
        store.set(b"first".to_vec(), b"kept".to_vec()).unwrap();
        store.set(b"second".to_vec(), b"lost".to_vec()).unwrap();
    }
    let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    let length = file.metadata().unwrap().len();
    file.set_len(length - 2).unwrap();

    let mut store = Store::open(&path).unwrap();
    assert_eq!(store.current_sequence(), 1);
    assert_eq!(store.get(b"first").unwrap(), Some(b"kept".to_vec()));
    assert_eq!(store.get(b"second").unwrap(), None);
    store.set(b"third".to_vec(), b"reachable".to_vec()).unwrap();
    drop(store);

    let store = Store::open(&path).unwrap();
    assert_eq!(store.current_sequence(), 2);
    assert_eq!(store.get(b"first").unwrap(), Some(b"kept".to_vec()));
    assert_eq!(store.get(b"second").unwrap(), None);
    assert_eq!(store.get(b"third").unwrap(), Some(b"reachable".to_vec()));
    std::fs::remove_file(&path).ok();
}
