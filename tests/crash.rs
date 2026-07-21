//! Child-process crash matrix for storage publication ordering.
#![cfg(debug_assertions)]

use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;

use kvdb::store::Store;

const CHILD_EXIT: i32 = 86;

fn crash_dir(point: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!("kvdb-crash-{point}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).unwrap();
    path
}

fn acknowledge(dir: &Path, key: &str) {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(dir.join("acknowledged"))
        .unwrap();
    writeln!(file, "{key}").unwrap();
    file.sync_all().unwrap();
}

#[test]
fn crash_failpoint_child() {
    let Ok(dir) = std::env::var("KVDB_CRASH_DIR") else {
        return;
    };
    let scenario = std::env::var("KVDB_CRASH_SCENARIO").unwrap();
    let dir = PathBuf::from(dir);
    let wal = dir.join("kvdb.wal");
    let mut store = Store::open(&wal).unwrap();

    match scenario.as_str() {
        "wal" => {
            store
                .set(b"target".to_vec(), b"wal-value".to_vec())
                .unwrap();
            acknowledge(&dir, "target");
        }
        "flush" => {
            store
                .set(b"target".to_vec(), b"flush-value".to_vec())
                .unwrap();
            acknowledge(&dir, "target");
            store.flush().unwrap();
        }
        "obsolete" => {
            store
                .set(b"target".to_vec(), b"compact-value".to_vec())
                .unwrap();
            acknowledge(&dir, "target");
            store.flush().unwrap();
            store.compact().unwrap();
        }
        other => panic!("unknown crash scenario: {other}"),
    }

    panic!("selected failpoint did not terminate the child");
}

#[test]
fn acknowledged_writes_survive_the_crash_matrix() {
    let points = [
        ("wal_after_write", "wal"),
        ("wal_before_sync", "wal"),
        ("wal_after_sync", "wal"),
        ("sstable_before_sync", "flush"),
        ("sstable_after_sync", "flush"),
        ("sstable_after_rename", "flush"),
        ("manifest_before_sync", "flush"),
        ("manifest_after_sync", "flush"),
        ("manifest_after_rename", "flush"),
        ("wal_before_truncate", "flush"),
        ("wal_after_set_len", "flush"),
        ("wal_after_truncate", "flush"),
        ("obsolete_before_delete", "obsolete"),
        ("obsolete_after_delete", "obsolete"),
    ];

    for (point, scenario) in points {
        let dir = crash_dir(point);
        let wal = dir.join("kvdb.wal");
        {
            let mut store = Store::open(&wal).unwrap();
            store
                .set(b"baseline".to_vec(), b"durable".to_vec())
                .unwrap();
            store.flush().unwrap();
        }

        let output = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "crash_failpoint_child", "--nocapture"])
            .env("KVDB_ENABLE_FAILPOINTS", "1")
            .env("KVDB_FAILPOINT", point)
            .env("KVDB_CRASH_DIR", &dir)
            .env("KVDB_CRASH_SCENARIO", scenario)
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(CHILD_EXIT),
            "failpoint {point} did not terminate as expected:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            String::from_utf8_lossy(&output.stderr).contains(point),
            "child did not report failpoint {point}"
        );

        let acknowledged = std::fs::read_to_string(dir.join("acknowledged"))
            .unwrap_or_default()
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        let mut store = Store::open(&wal).unwrap();
        assert_eq!(
            store.get(b"baseline").unwrap(),
            Some(b"durable".to_vec()),
            "baseline was lost at {point}"
        );
        if acknowledged.iter().any(|key| key == "target") {
            assert!(
                store.get(b"target").unwrap().is_some(),
                "acknowledged target was lost at {point}"
            );
        }
        store
            .set(b"post-recovery".to_vec(), point.as_bytes().to_vec())
            .unwrap();
        store.flush().unwrap();
        drop(store);

        let store = Store::open(&wal).unwrap();
        assert_eq!(store.get(b"baseline").unwrap(), Some(b"durable".to_vec()));
        assert_eq!(
            store.get(b"post-recovery").unwrap(),
            Some(point.as_bytes().to_vec())
        );
        if acknowledged.iter().any(|key| key == "target") {
            assert!(store.get(b"target").unwrap().is_some());
        }
        drop(store);
        std::fs::remove_dir_all(&dir).ok();
    }
}
