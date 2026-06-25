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

#[test]
fn set_get_delete() {
    let path = tmp_path("basic");
    let mut s = Store::open(&path).unwrap();
    s.set(b"a".to_vec(), b"1".to_vec()).unwrap();
    assert_eq!(s.get(b"a"), Some(b"1".to_vec()));
    assert_eq!(s.delete(b"a").unwrap(), true);
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
