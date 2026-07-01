//! Immutable, key-sorted on-disk tables — the "SSTable" of an LSM tree.
//!
//! A [`Store`](crate::store::Store) flushes its memtable to one of these files
//! when it grows too large. Each file is written once, in ascending key order,
//! and never mutated afterwards; newer state lives in newer files (or in the
//! memtable) and shadows older files during a read.
//!
//! ## On-disk format
//!
//! A flat sequence of records, sorted by key. Each record is:
//!
//! ```text
//!   [flag:u8][key_len:u32 BE][key]            (flag = 1: Tombstone — stops here)
//!   [flag:u8][key_len:u32 BE][key][val_len:u32 BE][value]   (flag = 0: Set)
//! ```
//!
//! A tombstone carries no value: its presence *is* the information ("this key is
//! deleted at this level"). Keeping tombstones on disk is what lets a delete
//! shadow an older value in an older SSTable instead of resurrecting it.
//!
//! ## Index
//!
//! On [`open`](SsTable::open) the whole file is scanned once to build a dense
//! in-memory `key → file-offset` index, so a later lookup is a map hit plus a
//! single seek+read rather than a full scan. A sparse/block index (so the index
//! itself need not be fully resident) is a later roadmap step.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// Record flags on disk.
const FLAG_SET: u8 = 0;
const FLAG_TOMBSTONE: u8 = 1;

/// A value stored for a key: either live bytes, or a tombstone marking a delete.
///
/// This is the memtable's value type as well as the SSTable record payload, so
/// deletes have a first-class on-disk representation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value {
    /// A live value.
    Set(Vec<u8>),
    /// A deletion marker that shadows any older value for the same key.
    Tombstone,
}

impl Value {
    /// The number of bytes this record occupies on disk, including its header.
    fn encoded_len(&self, key: &[u8]) -> u64 {
        let head = 1 + 4 + key.len() as u64; // flag + key_len + key
        match self {
            Value::Set(v) => head + 4 + v.len() as u64, // + val_len + value
            Value::Tombstone => head,
        }
    }
}

/// A handle to one on-disk SSTable: its path plus the resident key→offset index.
pub struct SsTable {
    path: PathBuf,
    index: BTreeMap<Vec<u8>, u64>,
}

impl SsTable {
    /// Writes `entries` (which MUST be in ascending key order) to a new SSTable
    /// at `path`, atomically: the data is streamed to a temp file, fsynced, and
    /// renamed into place, so a crash never leaves a half-written `path`.
    pub fn write<'a, I>(path: &Path, entries: I) -> io::Result<()>
    where
        I: IntoIterator<Item = (&'a [u8], &'a Value)>,
    {
        let tmp = tmp_path(path);
        let file = File::create(&tmp)?;
        let mut w = BufWriter::new(file);
        for (key, value) in entries {
            write_record(&mut w, key, value)?;
        }
        w.flush()?;
        // Durability: get the bytes to disk before we publish the file by rename.
        w.get_ref().sync_all()?;
        std::fs::rename(&tmp, path)?;
        Ok(())
    }

    /// Opens an existing SSTable and builds its in-memory index by scanning once.
    pub fn open(path: &Path) -> io::Result<SsTable> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut index = BTreeMap::new();
        let mut offset = 0u64;

        while let Some((key, value)) = read_record(&mut reader)? {
            let len = value.encoded_len(&key);
            index.insert(key, offset);
            offset += len;
        }

        Ok(SsTable {
            path: path.to_path_buf(),
            index,
        })
    }

    /// Looks up `key`. Returns:
    /// * `Ok(Some(Value::Set(_)))` — a live value;
    /// * `Ok(Some(Value::Tombstone))` — the key is deleted at this level;
    /// * `Ok(None)` — this table has no record for the key (check older tables).
    pub fn get(&self, key: &[u8]) -> io::Result<Option<Value>> {
        let Some(&offset) = self.index.get(key) else {
            return Ok(None);
        };
        let mut file = File::open(&self.path)?;
        file.seek(SeekFrom::Start(offset))?;
        let mut reader = BufReader::new(file);
        match read_record(&mut reader)? {
            Some((_key, value)) => Ok(Some(value)),
            None => Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "sstable index points past end of file",
            )),
        }
    }

    /// Iterates the keys held by this table (live values and tombstones), in
    /// ascending order.
    pub fn keys(&self) -> impl Iterator<Item = &Vec<u8>> {
        self.index.keys()
    }

    /// Number of records (live values and tombstones) in this table.
    pub fn len(&self) -> usize {
        self.index.len()
    }

    /// Whether the table holds no records.
    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }
}

/// The temp path a table is staged at before being renamed into place.
fn tmp_path(path: &Path) -> PathBuf {
    let mut s = path.as_os_str().to_os_string();
    s.push(".tmp");
    PathBuf::from(s)
}

/// Encodes one record (see the module-level format docs).
fn write_record<W: Write>(w: &mut W, key: &[u8], value: &Value) -> io::Result<()> {
    match value {
        Value::Set(v) => {
            w.write_all(&[FLAG_SET])?;
            w.write_all(&(key.len() as u32).to_be_bytes())?;
            w.write_all(key)?;
            w.write_all(&(v.len() as u32).to_be_bytes())?;
            w.write_all(v)?;
        }
        Value::Tombstone => {
            w.write_all(&[FLAG_TOMBSTONE])?;
            w.write_all(&(key.len() as u32).to_be_bytes())?;
            w.write_all(key)?;
        }
    }
    Ok(())
}

/// Reads one record. Returns `Ok(None)` at a clean end of file. Unlike the WAL,
/// an SSTable is written atomically, so a torn record here is genuine
/// corruption and surfaces as an error rather than being tolerated.
fn read_record<R: Read>(r: &mut R) -> io::Result<Option<(Vec<u8>, Value)>> {
    let mut flag = [0u8; 1];
    match r.read_exact(&mut flag) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }

    let key = read_chunk(r)?;
    match flag[0] {
        FLAG_SET => {
            let value = read_chunk(r)?;
            Ok(Some((key, Value::Set(value))))
        }
        FLAG_TOMBSTONE => Ok(Some((key, Value::Tombstone))),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown SSTable record flag: {other}"),
        )),
    }
}

/// Reads a length-prefixed byte chunk (`[u32 BE len][bytes]`).
fn read_chunk<R: Read>(r: &mut R) -> io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf)?;
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(tag: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("kvdb-sst-unit-{tag}-{}.sst", std::process::id()));
        let _ = std::fs::remove_file(&p);
        p
    }

    #[test]
    fn roundtrips_values_and_tombstones() {
        let path = tmp("roundtrip");
        let entries: Vec<(Vec<u8>, Value)> = vec![
            (b"a".to_vec(), Value::Set(b"1".to_vec())),
            (b"b".to_vec(), Value::Tombstone),
            (b"c".to_vec(), Value::Set(b"three".to_vec())),
        ];
        SsTable::write(&path, entries.iter().map(|(k, v)| (k.as_slice(), v))).unwrap();

        let sst = SsTable::open(&path).unwrap();
        assert_eq!(sst.len(), 3);
        assert_eq!(sst.get(b"a").unwrap(), Some(Value::Set(b"1".to_vec())));
        assert_eq!(sst.get(b"b").unwrap(), Some(Value::Tombstone));
        assert_eq!(sst.get(b"c").unwrap(), Some(Value::Set(b"three".to_vec())));
        assert_eq!(sst.get(b"missing").unwrap(), None);

        std::fs::remove_file(&path).ok();
    }
}
