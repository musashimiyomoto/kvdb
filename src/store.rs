//! Storage engine: an in-memory memtable backed by a write-ahead log (WAL),
//! with overflow flushed to immutable sorted [`SsTable`]s.
//!
//! Every mutation (`set` / `delete`) is first appended to the WAL on disk and
//! flushed, then applied to the in-memory `BTreeMap`. A delete is recorded as a
//! [`Value::Tombstone`] rather than a removal, so it can shadow an older value
//! that already lives in an SSTable instead of resurrecting it.
//!
//! When the memtable grows past a threshold it is flushed to a new SSTable and
//! the WAL is truncated; a [manifest](Self::manifest_path) records the live
//! tables. On startup we load the manifest's SSTables and replay the (post-last-
//! flush) WAL on top, so the memtable always holds the newest state.
//!
//! Read precedence, newest to oldest: **memtable → SSTables (newest first)**.
//! The first record found wins, and a tombstone found first means "not present".

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use crate::sstable::{SsTable, Value};
use crate::{log_debug, log_info, log_warn};

/// WAL operation codes.
const OP_SET: u8 = 1;
const OP_DELETE: u8 = 2;

/// Default memtable size (live + tombstone entries) that triggers a flush.
const DEFAULT_MEMTABLE_LIMIT: usize = 1024;

const TARGET: &str = "kvdb::store";

/// A single-node, crash-safe key-value store: WAL + memtable + SSTables.
pub struct Store {
    /// Sorted in-memory view of the most recent mutations (since the last flush).
    memtable: BTreeMap<Vec<u8>, Value>,
    /// Buffered append-only handle to the WAL file.
    wal: BufWriter<File>,
    /// Path of the WAL; also the anchor for SSTable / manifest paths.
    wal_path: PathBuf,
    /// Live SSTables, oldest first. Reads scan this in reverse (newest first).
    sstables: Vec<SsTable>,
    /// Sequence number for the next SSTable file.
    next_seq: u64,
    /// Flush the memtable once it reaches this many entries.
    memtable_limit: usize,
}

impl Store {
    /// Opens (or creates) a store whose WAL lives at `wal_path`. Loads the
    /// SSTables named in the manifest, then replays any WAL records left over
    /// from writes that had not yet been flushed, rebuilding the memtable.
    pub fn open<P: AsRef<Path>>(wal_path: P) -> io::Result<Self> {
        let wal_path = wal_path.as_ref().to_path_buf();

        // 1. Load previously-flushed SSTables from the manifest.
        let manifest_path = manifest_path(&wal_path);
        let sstable_names = read_manifest(&manifest_path)?;
        let dir = parent_dir(&wal_path);
        let mut sstables = Vec::with_capacity(sstable_names.len());
        for name in &sstable_names {
            let sst = SsTable::open(&dir.join(name))?;
            sstables.push(sst);
        }
        let next_seq = next_seq_from(&sstable_names);

        // 2. Replay the WAL (writes since the last flush) on top of the SSTables.
        let memtable = Self::replay(&wal_path)?;

        // 3. Reopen the WAL in append mode for subsequent writes.
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&wal_path)?;

        log_debug!(
            TARGET,
            "opened store: {} sstable(s), {} memtable entr(ies)",
            sstables.len(),
            memtable.len()
        );

        Ok(Store {
            memtable,
            wal: BufWriter::new(file),
            wal_path,
            sstables,
            next_seq,
            memtable_limit: memtable_limit_from_env(),
        })
    }

    /// Replays the WAL at `path` and returns the reconstructed memtable.
    ///
    /// A truncated trailing record (e.g. from a crash mid-write) is treated as
    /// "not committed" and silently dropped, rather than failing recovery. A
    /// DELETE replays as a tombstone so it still shadows any older SSTable value.
    fn replay(path: &Path) -> io::Result<BTreeMap<Vec<u8>, Value>> {
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
                    map.insert(key, Value::Set(value));
                }
                Ok(Some(Record::Delete { key })) => {
                    map.insert(key, Value::Tombstone);
                }
                Ok(None) => break, // clean end of log
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break, // torn tail write
                Err(e) => return Err(e),
            }
        }

        Ok(map)
    }

    /// Returns the value for `key`, if present and not deleted.
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        match self.lookup(key) {
            Some(Value::Set(v)) => Some(v),
            _ => None, // tombstone or genuinely absent
        }
    }

    /// Resolves a key across every level, newest to oldest, returning the first
    /// record found (which may be a tombstone). `None` means no level knows it.
    fn lookup(&self, key: &[u8]) -> Option<Value> {
        // Memtable is newest.
        if let Some(v) = self.memtable.get(key) {
            return Some(v.clone());
        }
        // Then SSTables, newest (last pushed) first.
        for sst in self.sstables.iter().rev() {
            match sst.get(key) {
                Ok(Some(v)) => return Some(v),
                Ok(None) => continue,
                Err(e) => {
                    log_warn!(TARGET, "sstable read failed for a key: {e}");
                    continue;
                }
            }
        }
        None
    }

    /// Inserts or overwrites `key` with `value`, durably. May trigger a flush.
    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> io::Result<()> {
        write_set(&mut self.wal, &key, &value)?;
        self.wal.flush()?;
        self.memtable.insert(key, Value::Set(value));
        self.maybe_flush()
    }

    /// Records a delete for `key`, durably. Returns whether a *live* value
    /// existed beforehand (preserving the HTTP layer's 404 semantics). The
    /// delete is stored as a tombstone regardless. May trigger a flush.
    pub fn delete(&mut self, key: &[u8]) -> io::Result<bool> {
        // Existed = there is a live value at some level right now.
        let existed = matches!(self.lookup(key), Some(Value::Set(_)));

        write_delete(&mut self.wal, key)?;
        self.wal.flush()?;
        self.memtable.insert(key.to_vec(), Value::Tombstone);
        self.maybe_flush()?;
        Ok(existed)
    }

    /// Flushes the memtable to a new SSTable if it has reached the limit.
    fn maybe_flush(&mut self) -> io::Result<()> {
        if self.memtable.len() >= self.memtable_limit {
            self.flush()?;
        }
        Ok(())
    }

    /// Flushes the current memtable to a new immutable SSTable and seals the WAL.
    ///
    /// Ordering is the durability invariant: **SSTable (fsync+rename) → manifest
    /// (fsync+rename) → truncate WAL → clear memtable.** A crash between any two
    /// steps degrades safely — an SSTable absent from the manifest is an orphan
    /// and ignored, and un-truncated WAL records simply replay (newest wins).
    ///
    /// A no-op on an empty memtable.
    pub fn flush(&mut self) -> io::Result<()> {
        if self.memtable.is_empty() {
            return Ok(());
        }

        let dir = parent_dir(&self.wal_path);
        let name = sstable_name(&self.wal_path, self.next_seq);
        let sst_path = dir.join(&name);

        // 1. Write the sorted memtable to a fresh SSTable, durably.
        SsTable::write(
            &sst_path,
            self.memtable.iter().map(|(k, v)| (k.as_slice(), v)),
        )?;

        // 2. Publish it by appending to the manifest (temp + fsync + rename).
        let mut names: Vec<String> = self.sstable_names();
        names.push(name.clone());
        write_manifest(&manifest_path(&self.wal_path), &names)?;

        // 3. Seal the WAL: everything in it is now durable in the SSTable.
        self.truncate_wal()?;

        // 4. Adopt the new table and reset the memtable.
        let flushed = self.memtable.len();
        self.sstables.push(SsTable::open(&sst_path)?);
        self.next_seq += 1;
        self.memtable.clear();

        log_info!(
            TARGET,
            "flushed {flushed} entr(ies) to {} ({} sstable(s) live)",
            name,
            self.sstables.len()
        );
        Ok(())
    }

    /// Merges every live SSTable into one table, retaining only the newest
    /// record for each key and dropping tombstones. Tombstones are safe to drop
    /// because this is a full compaction: there are no older tables left that
    /// could otherwise resurrect their values.
    ///
    /// Publication follows the same crash-safe order as [`Self::flush`]: write
    /// and fsync the replacement table, atomically publish its manifest, then
    /// remove superseded files. A crash before the manifest update leaves the
    /// old set live; a crash after it leaves harmless orphaned old files.
    pub fn compact(&mut self) -> io::Result<()> {
        if self.sstables.is_empty() {
            return Ok(());
        }

        let merged = merge_sstables(&self.sstables)?;
        let old_names = self.sstable_names();
        let dir = parent_dir(&self.wal_path);
        let manifest = manifest_path(&self.wal_path);

        let replacement = if merged.is_empty() {
            None
        } else {
            let name = sstable_name(&self.wal_path, self.next_seq);
            let path = dir.join(&name);
            SsTable::write(
                &path,
                merged.iter().map(|(key, value)| (key.as_slice(), value)),
            )?;
            let table = SsTable::open(&path)?;
            Some((name, table))
        };

        let replacement_names: Vec<String> = replacement
            .as_ref()
            .map(|(name, _)| vec![name.clone()])
            .unwrap_or_default();
        write_manifest(&manifest, &replacement_names)?;

        let replacement_name = replacement.as_ref().map(|(name, _)| name.as_str());
        for old_name in &old_names {
            if Some(old_name.as_str()) == replacement_name {
                continue;
            }
            if let Err(e) = std::fs::remove_file(dir.join(old_name)) {
                log_warn!(
                    TARGET,
                    "could not remove superseded SSTable {old_name}: {e}"
                );
            }
        }

        self.sstables = replacement.into_iter().map(|(_, table)| table).collect();
        if !self.sstables.is_empty() {
            self.next_seq += 1;
        }

        log_info!(
            TARGET,
            "compacted {} SSTable(s) into {} table(s)",
            old_names.len(),
            self.sstables.len()
        );
        Ok(())
    }

    /// Empties the WAL file after its contents have been captured in an SSTable.
    /// The buffer is flushed first, then the file is truncated to zero length;
    /// the append handle keeps writing new records from the (now empty) start.
    fn truncate_wal(&mut self) -> io::Result<()> {
        self.wal.flush()?;
        let file = self.wal.get_mut();
        file.set_len(0)?;
        file.sync_all()?;
        Ok(())
    }

    /// The file names of the currently-live SSTables, oldest first.
    fn sstable_names(&self) -> Vec<String> {
        (0..self.sstables.len())
            .map(|i| sstable_name(&self.wal_path, self.first_seq() + i as u64))
            .collect()
    }

    /// Sequence number of the oldest live SSTable (0 if there are none).
    fn first_seq(&self) -> u64 {
        self.next_seq - self.sstables.len() as u64
    }

    /// Number of live keys currently visible across every level.
    ///
    /// This walks the union of keys (memtable + on-disk SSTable scans) and counts
    /// those resolving to a live value -- O(keys), used for the startup summary
    /// and tests rather than a hot path. SSTables deliberately keep only sparse
    /// block indexes in memory.
    pub fn len(&self) -> usize {
        let mut keys: std::collections::BTreeSet<Vec<u8>> = std::collections::BTreeSet::new();
        keys.extend(self.memtable.keys().cloned());
        for sst in &self.sstables {
            match sst.keys() {
                Ok(sstable_keys) => keys.extend(sstable_keys),
                Err(e) => log_warn!(TARGET, "sstable key scan failed: {e}"),
            }
        }
        keys.iter()
            .filter(|k| matches!(self.lookup(k), Some(Value::Set(_))))
            .count()
    }

    /// Whether the store currently exposes no live keys.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Overrides the memtable flush threshold (entries) for this store. Mainly
    /// for tests, which need to force a flush deterministically without relying
    /// on the process-global `KVDB_MEMTABLE_LIMIT` env var. A zero is ignored.
    pub fn set_memtable_limit(&mut self, limit: usize) {
        if limit > 0 {
            self.memtable_limit = limit;
        }
    }

    /// Number of SSTables currently live (flushed and recorded in the manifest).
    pub fn sstable_count(&self) -> usize {
        self.sstables.len()
    }

    /// Path to the backing WAL file.
    pub fn wal_path(&self) -> &Path {
        &self.wal_path
    }

    /// Path to the manifest that lists this store's live SSTables.
    pub fn manifest_path(&self) -> PathBuf {
        manifest_path(&self.wal_path)
    }
}

/// K-way merge sorted SSTables, oldest to newest. When more than one source
/// has the same key, the last source wins; tombstones are omitted because a
/// full compaction leaves no older level for them to shadow.
fn merge_sstables(sstables: &[SsTable]) -> io::Result<Vec<(Vec<u8>, Value)>> {
    let sources: Vec<Vec<(Vec<u8>, Value)>> = sstables
        .iter()
        .map(SsTable::entries)
        .collect::<io::Result<_>>()?;
    let mut positions = vec![0usize; sources.len()];
    let mut merged = Vec::new();

    while let Some((_, key)) = sources
        .iter()
        .enumerate()
        .filter_map(|(source, entries)| {
            entries.get(positions[source]).map(|(key, _)| (source, key))
        })
        .min_by(|(_, left), (_, right)| left.cmp(right))
    {
        let key = key.clone();
        let mut newest = None;

        for (source, entries) in sources.iter().enumerate() {
            if entries
                .get(positions[source])
                .is_some_and(|(candidate, _)| candidate == &key)
            {
                newest = Some(entries[positions[source]].1.clone());
                positions[source] += 1;
            }
        }

        if let Some(Value::Set(value)) = newest {
            merged.push((key, Value::Set(value)));
        }
    }

    Ok(merged)
}

// ---- Paths -----------------------------------------------------------------

/// Directory containing the WAL (and thus the SSTables + manifest). An empty
/// parent (e.g. a bare `kvdb.wal`) means the current directory.
fn parent_dir(wal_path: &Path) -> PathBuf {
    match wal_path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
        _ => PathBuf::from("."),
    }
}

/// File stem used to name sibling files, e.g. `kvdb` for `kvdb.wal`.
fn stem(wal_path: &Path) -> String {
    wal_path
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| "kvdb".to_string())
}

/// The manifest path, `<dir>/<stem>.manifest`.
fn manifest_path(wal_path: &Path) -> PathBuf {
    parent_dir(wal_path).join(format!("{}.manifest", stem(wal_path)))
}

/// The SSTable file name for a sequence number, e.g. `kvdb-000007.sst`.
fn sstable_name(wal_path: &Path, seq: u64) -> String {
    format!("{}-{seq:06}.sst", stem(wal_path))
}

/// Next sequence number = one past the highest seq encoded in existing names.
fn next_seq_from(names: &[String]) -> u64 {
    names
        .iter()
        .filter_map(|n| seq_of(n))
        .max()
        .map(|m| m + 1)
        .unwrap_or(0)
}

/// Extracts the sequence number from a `<stem>-NNNNNN.sst` file name.
fn seq_of(name: &str) -> Option<u64> {
    let digits = name.rsplit_once('-')?.1.strip_suffix(".sst")?;
    digits.parse().ok()
}

// ---- Manifest --------------------------------------------------------------

/// Reads the manifest: one SSTable file name per line, oldest first. A missing
/// manifest means a fresh store (no SSTables yet).
fn read_manifest(path: &Path) -> io::Result<Vec<String>> {
    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    let mut names = Vec::new();
    for line in BufReader::new(file).lines() {
        let line = line?;
        let trimmed = line.trim();
        if !trimmed.is_empty() {
            names.push(trimmed.to_string());
        }
    }
    Ok(names)
}

/// Writes the manifest atomically (temp file + fsync + rename) so a crash never
/// leaves a partially-written catalog of live SSTables.
fn write_manifest(path: &Path, names: &[String]) -> io::Result<()> {
    let mut tmp = path.as_os_str().to_os_string();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);

    let mut file = File::create(&tmp)?;
    for name in names {
        file.write_all(name.as_bytes())?;
        file.write_all(b"\n")?;
    }
    file.sync_all()?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

// ---- WAL codec -------------------------------------------------------------

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

// ---- Config ----------------------------------------------------------------

/// Reads the memtable flush threshold from `KVDB_MEMTABLE_LIMIT`, falling back
/// to [`DEFAULT_MEMTABLE_LIMIT`]. A zero or unparseable value uses the default.
fn memtable_limit_from_env() -> usize {
    std::env::var("KVDB_MEMTABLE_LIMIT")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_MEMTABLE_LIMIT)
}
