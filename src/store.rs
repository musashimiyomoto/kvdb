//! Storage engine: an in-memory memtable backed by a write-ahead log (WAL).
//!
//! Every mutation (`set` / `delete`) is first appended to the WAL on disk and
//! flushed, then applied to the in-memory `BTreeMap`. On startup we replay the
//! WAL to rebuild the memtable, so committed writes survive a restart or crash.
//!
//! This is the classic LSM starting point. Later stages (flushing the memtable
//! into sorted SSTables, compaction, bloom filters) build on top of this file.

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

/// WAL operation codes.
const OP_SET: u8 = 1;
const OP_DELETE: u8 = 2;

/// A single-file, crash-safe key-value store.
pub struct Store {
    /// Sorted in-memory view of the current state.
    memtable: BTreeMap<Vec<u8>, Vec<u8>>,
    /// Buffered append-only handle to the WAL file.
    wal: BufWriter<File>,
    /// Path of the WAL (kept for diagnostics / future compaction).
    wal_path: PathBuf,
}

impl Store {
    /// Opens (or creates) a store whose WAL lives at `wal_path`, replaying any
    /// existing log to reconstruct the in-memory state.
    pub fn open<P: AsRef<Path>>(wal_path: P) -> io::Result<Self> {
        let wal_path = wal_path.as_ref().to_path_buf();
        let memtable = Self::replay(&wal_path)?;

        // Reopen in append mode for subsequent writes.
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&wal_path)?;

        Ok(Store {
            memtable,
            wal: BufWriter::new(file),
            wal_path,
        })
    }

    /// Replays the WAL at `path` and returns the reconstructed memtable.
    ///
    /// A truncated trailing record (e.g. from a crash mid-write) is treated as
    /// "not committed" and silently dropped, rather than failing recovery.
    fn replay(path: &Path) -> io::Result<BTreeMap<Vec<u8>, Vec<u8>>> {
        let mut map = BTreeMap::new();

        let file = match File::open(path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(map),
            Err(e) => return Err(e),
        };
        let mut reader = BufReader::new(file);

        loop {
            match read_record(&mut reader) {
                Ok(Some(Record::Set { key, value })) => {
                    map.insert(key, value);
                }
                Ok(Some(Record::Delete { key })) => {
                    map.remove(&key);
                }
                Ok(None) => break, // clean end of log
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break, // torn tail write
                Err(e) => return Err(e),
            }
        }

        Ok(map)
    }

    /// Returns the value for `key`, if present.
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.memtable.get(key).cloned()
    }

    /// Inserts or overwrites `key` with `value`, durably.
    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> io::Result<()> {
        write_set(&mut self.wal, &key, &value)?;
        self.wal.flush()?;
        self.memtable.insert(key, value);
        Ok(())
    }

    /// Removes `key`. Returns whether it existed. Durable regardless.
    pub fn delete(&mut self, key: &[u8]) -> io::Result<bool> {
        write_delete(&mut self.wal, key)?;
        self.wal.flush()?;
        Ok(self.memtable.remove(key).is_some())
    }

    /// Number of live keys currently in the store.
    pub fn len(&self) -> usize {
        self.memtable.len()
    }

    pub fn is_empty(&self) -> bool {
        self.memtable.is_empty()
    }

    /// Path to the backing WAL file.
    pub fn wal_path(&self) -> &Path {
        &self.wal_path
    }
}

/// A decoded WAL record.
enum Record {
    Set { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

/// Encodes and appends a SET record: `[OP_SET][key_len][key][val_len][value]`.
fn write_set<W: Write>(w: &mut W, key: &[u8], value: &[u8]) -> io::Result<()> {
    w.write_all(&[OP_SET])?;
    w.write_all(&(key.len() as u32).to_be_bytes())?;
    w.write_all(key)?;
    w.write_all(&(value.len() as u32).to_be_bytes())?;
    w.write_all(value)?;
    Ok(())
}

/// Encodes and appends a DELETE record: `[OP_DELETE][key_len][key]`.
fn write_delete<W: Write>(w: &mut W, key: &[u8]) -> io::Result<()> {
    w.write_all(&[OP_DELETE])?;
    w.write_all(&(key.len() as u32).to_be_bytes())?;
    w.write_all(key)?;
    Ok(())
}

/// Reads one record from `r`. Returns `Ok(None)` at a clean end of stream.
fn read_record<R: Read>(r: &mut R) -> io::Result<Option<Record>> {
    let mut op = [0u8; 1];
    match r.read_exact(&mut op) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }

    match op[0] {
        OP_SET => {
            let key = read_chunk(r)?;
            let value = read_chunk(r)?;
            Ok(Some(Record::Set { key, value }))
        }
        OP_DELETE => {
            let key = read_chunk(r)?;
            Ok(Some(Record::Delete { key }))
        }
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown WAL op code: {other}"),
        )),
    }
}

/// Reads a length-prefixed byte chunk (`[u32 len][bytes]`).
fn read_chunk<R: Read>(r: &mut R) -> io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf)?;
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)?;
    Ok(buf)
}
