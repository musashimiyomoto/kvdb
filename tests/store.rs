//! Integration tests for the storage engine.

use std::path::PathBuf;

use kvdb::store::{Store, TransactionError, WriteBatch};

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

#[test]
fn set_get_delete() {
    let path = tmp_path("basic");
    let mut s = Store::open(&path).unwrap();
    s.set(b"a".to_vec(), b"1".to_vec()).unwrap();
    assert_eq!(s.get(b"a"), Some(b"1".to_vec()));
    assert!(s.delete(b"a").unwrap());
    assert_eq!(s.get(b"a"), None);
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
    assert_eq!(s.get(b"x"), None);
    assert_eq!(s.get(b"y"), Some(b"world".to_vec()));
    assert_eq!(s.len(), 1);
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
    assert_eq!(s.get(b"a"), Some(b"1".to_vec()));
    assert_eq!(s.get(b"b"), Some(b"2".to_vec()));
    assert_eq!(s.get(b"c"), Some(b"3".to_vec()));

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
    assert_eq!(s.get(b"k"), None);

    // And it must survive a reopen (tombstone persisted on disk).
    let s2 = Store::open(&wal).unwrap();
    assert_eq!(s2.get(b"k"), None);
    assert_eq!(s2.len(), 0);

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
    assert_eq!(s.get(b"k"), Some(b"new".to_vec()));
    assert_eq!(s.len(), 1, "duplicate key counts once");

    let s2 = Store::open(&wal).unwrap();
    assert_eq!(s2.get(b"k"), Some(b"new".to_vec()));

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
    assert_eq!(s.get(b"flushed"), Some(b"on-disk".to_vec()));
    assert_eq!(s.get(b"pending"), Some(b"in-wal".to_vec()));
    assert_eq!(s.len(), 2);

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
    assert_eq!(s.get(b"real"), Some(b"yes".to_vec()));
    assert_eq!(s.len(), 1);
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
    assert_eq!(s.get(b"k"), Some(Vec::new()));
    assert_eq!(s.get(b"absent"), None);
    assert_eq!(s.len(), 1);

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
    assert_eq!(s.get(&key), Some(val.clone()));

    // Survives reopen (re-read from the SSTable).
    let s2 = Store::open(&wal).unwrap();
    assert_eq!(s2.get(&key), Some(val));

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn overwrite_updates_value_and_keeps_count() {
    let path = tmp_path("overwrite");
    let mut s = Store::open(&path).unwrap();

    s.set(b"k".to_vec(), b"v1".to_vec()).unwrap();
    s.set(b"k".to_vec(), b"v2".to_vec()).unwrap();
    s.set(b"k".to_vec(), b"v3".to_vec()).unwrap();

    assert_eq!(s.get(b"k"), Some(b"v3".to_vec()));
    assert_eq!(s.len(), 1, "overwrites don't add keys");

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
    assert_eq!(s.get(b"k"), None);
    assert_eq!(s.len(), 0);

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
    assert_eq!(s.get(b"blob"), Some(big.clone()));

    let s2 = Store::open(&wal).unwrap();
    assert_eq!(s2.get(b"blob"), Some(big));

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
            assert_eq!(s.get(key.as_bytes()), None, "{key} should be deleted");
        } else {
            assert_eq!(
                s.get(key.as_bytes()),
                Some(format!("val-{i}").into_bytes()),
                "{key} should survive"
            );
        }
    }
    assert_eq!(s.len(), N - N.div_ceil(10));

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
            s.get(format!("key-{i:04}").as_bytes()),
            Some(format!("value-{i}").into_bytes())
        );
    }
    assert_eq!(s.get(b"key-0063x"), None);
    assert_eq!(s.len(), N);

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn compaction_keeps_newest_values_and_discards_tombstones() {
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

    s.compact().unwrap();
    assert_eq!(s.sstable_count(), 1);
    assert_eq!(s.get(b"updated"), Some(b"new".to_vec()));
    assert_eq!(s.get(b"deleted"), None);
    assert_eq!(s.get(b"stable"), Some(b"kept".to_vec()));
    assert_eq!(s.get(b"added"), Some(b"fresh".to_vec()));
    assert_eq!(s.len(), 3);

    // The compacted table survives a restart, and subsequent flushes preserve
    // its sequence naming and read precedence.
    s.set(b"later".to_vec(), b"tail".to_vec()).unwrap();
    s.flush().unwrap();
    assert_eq!(s.sstable_count(), 2);
    drop(s);

    let s = Store::open(&wal).unwrap();
    assert_eq!(s.get(b"updated"), Some(b"new".to_vec()));
    assert_eq!(s.get(b"deleted"), None);
    assert_eq!(s.get(b"later"), Some(b"tail".to_vec()));
    assert_eq!(s.len(), 4);

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn compaction_of_only_tombstones_publishes_an_empty_manifest() {
    let dir = tmp_dir("compact-empty");
    let wal = dir.join("kvdb.wal");

    let mut s = Store::open(&wal).unwrap();
    s.set(b"gone".to_vec(), b"value".to_vec()).unwrap();
    s.flush().unwrap();
    assert!(s.delete(b"gone").unwrap());
    s.flush().unwrap();

    s.compact().unwrap();
    assert_eq!(s.sstable_count(), 0);
    assert_eq!(s.get(b"gone"), None);
    assert_eq!(
        std::fs::read_to_string(s.manifest_path()).unwrap(),
        format!("sequence={}\n", s.current_sequence())
    );
    drop(s);

    let s = Store::open(&wal).unwrap();
    assert_eq!(s.sstable_count(), 0);
    assert_eq!(s.get(b"gone"), None);

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
    assert_eq!(s.get(b"a"), Some(b"last".to_vec()));
    assert_eq!(s.get(b"b"), Some(b"second".to_vec()));

    // An empty batch is a no-op and does not consume a sequence number.
    assert_eq!(s.write_batch(WriteBatch::new()).unwrap(), 1);
    assert_eq!(s.current_sequence(), 1);

    let s = Store::open(&path).unwrap();
    assert_eq!(s.current_sequence(), 1);
    assert_eq!(s.get(b"a"), Some(b"last".to_vec()));
    assert_eq!(s.get(b"b"), Some(b"second".to_vec()));

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
    assert_eq!(s.get(b"a"), None);
    assert_eq!(s.get(b"b"), None);

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
    assert_eq!(s.get(b"persisted"), Some(b"value".to_vec()));
    s.set(b"next".to_vec(), b"two".to_vec()).unwrap();
    assert_eq!(s.current_sequence(), 2);

    drop(s);
    let s = Store::open(&wal).unwrap();
    assert_eq!(s.current_sequence(), 2);
    assert_eq!(s.get(b"next"), Some(b"two".to_vec()));

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
    assert_eq!(s.get(b"flushed"), Some(b"sstable".to_vec()));
    assert_eq!(s.get(b"pending"), Some(b"wal".to_vec()));

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

    assert_eq!(s.get(b"changing"), Some(b"new".to_vec()));
    assert_eq!(s.get(b"kept"), None);
    assert_eq!(s.get(b"added"), Some(b"later".to_vec()));

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
    assert_eq!(s.get(b"balance"), Some(b"100".to_vec()));
    assert_eq!(s.get(b"audit"), None);

    assert_eq!(s.commit_transaction(transaction).unwrap(), 2);
    assert_eq!(s.get(b"balance"), Some(b"90".to_vec()));
    assert_eq!(s.get(b"audit"), Some(b"withdraw 10".to_vec()));
    drop(s);

    let s = Store::open(&path).unwrap();
    assert_eq!(s.current_sequence(), 2);
    assert_eq!(s.get(b"balance"), Some(b"90".to_vec()));
    assert_eq!(s.get(b"audit"), Some(b"withdraw 10".to_vec()));

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
    assert_eq!(s.get(b"key"), Some(b"concurrent".to_vec()));
    assert_eq!(s.get(b"only-in-transaction"), None);

    drop(s);
    let s = Store::open(&path).unwrap();
    assert_eq!(s.current_sequence(), 2);
    assert_eq!(s.get(b"key"), Some(b"concurrent".to_vec()));
    assert_eq!(s.get(b"only-in-transaction"), None);

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
    assert_eq!(s.get(b"left"), Some(b"one".to_vec()));
    assert_eq!(s.get(b"right"), Some(b"two".to_vec()));

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
    assert_eq!(s.get(b"derived"), None);

    std::fs::remove_file(&path).ok();
}

#[test]
fn unknown_wal_opcode_is_a_hard_error() {
    let path = tmp_path("bad-opcode");
    // A single byte 0x07 is not a valid op code; recovery must refuse it
    // (unlike a torn tail, which is tolerated).
    std::fs::write(&path, [0x07u8]).unwrap();

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

    // Write one clean record, then reopen and append a truncated SET header
    // (op byte + partial key length) to simulate a crash mid-write.
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
        f.write_all(&[1u8, 0x00, 0x00]).unwrap(); // OP_SET + 2 of 4 length bytes
    }

    // The torn tail is dropped; the earlier committed record remains.
    let s = Store::open(&path).unwrap();
    assert_eq!(s.get(b"good"), Some(b"kept".to_vec()));
    assert_eq!(s.len(), 1);

    std::fs::remove_file(&path).ok();
}
