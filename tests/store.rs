//! Integration tests for the storage engine.

use std::path::PathBuf;

use kvdb::store::Store;

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
