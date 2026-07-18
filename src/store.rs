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

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use crate::sstable::{SsTable, Value, VersionedValue};
use crate::{log_debug, log_info, log_warn};

/// WAL operation codes.
const OP_SET: u8 = 1;
const OP_DELETE: u8 = 2;
const OP_BATCH: u8 = 3;

/// Default memtable size (live + tombstone entries) that triggers a flush.
const DEFAULT_MEMTABLE_LIMIT: usize = 1024;
const DEFAULT_COMPACTION_THRESHOLD: usize = 8;

const TARGET: &str = "kvdb::store";

type MemTable = BTreeMap<Vec<u8>, Vec<VersionedValue>>;

/// One mutation inside an atomic [`WriteBatch`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BatchOperation {
    Set { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

impl BatchOperation {
    fn key(&self) -> &[u8] {
        match self {
            BatchOperation::Set { key, .. } | BatchOperation::Delete { key } => key,
        }
    }
}

/// A group of mutations committed as one WAL record and one sequence number.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WriteBatch {
    operations: Vec<BatchOperation>,
}

/// An immutable, read-only copy of the store's visible state at one sequence.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Snapshot {
    sequence: u64,
    values: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl Snapshot {
    /// Returns the value visible when this snapshot was created.
    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        self.values.get(key).map(Vec::as_slice)
    }

    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.values.contains_key(key)
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Commit sequence captured by this snapshot.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
}

/// An optimistic transaction backed by an immutable snapshot and a write
/// overlay. Dropping it aborts without touching the WAL.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Transaction {
    base_sequence: u64,
    snapshot: Snapshot,
    batch: WriteBatch,
    overlay: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    read_set: BTreeSet<Vec<u8>>,
}

impl Transaction {
    /// Reads from the transaction's own writes first, then its fixed snapshot.
    pub fn get(&mut self, key: &[u8]) -> Option<&[u8]> {
        self.read_set.insert(key.to_vec());
        match self.overlay.get(key) {
            Some(Some(value)) => Some(value),
            Some(None) => None,
            None => self.snapshot.get(key),
        }
    }

    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> &mut Self {
        self.overlay.insert(key.clone(), Some(value.clone()));
        self.batch.set(key, value);
        self
    }

    pub fn delete(&mut self, key: Vec<u8>) -> &mut Self {
        self.overlay.insert(key.clone(), None);
        self.batch.delete(key);
        self
    }

    pub fn base_sequence(&self) -> u64 {
        self.base_sequence
    }

    pub fn is_empty(&self) -> bool {
        self.batch.is_empty()
    }
}

/// Failure to commit an optimistic [`Transaction`].
#[derive(Debug)]
pub enum TransactionError {
    /// Another commit changed the Store after this transaction's snapshot.
    Conflict {
        key: Vec<u8>,
        expected: u64,
        actual: u64,
    },
    /// The atomic WAL batch could not be committed.
    Io(io::Error),
}

impl std::fmt::Display for TransactionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransactionError::Conflict {
                key,
                expected,
                actual,
            } => write!(
                f,
                "transaction conflict for key {key:?}: expected at most sequence {expected}, actual {actual}"
            ),
            TransactionError::Io(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for TransactionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            TransactionError::Conflict { .. } => None,
            TransactionError::Io(error) => Some(error),
        }
    }
}

impl From<io::Error> for TransactionError {
    fn from(error: io::Error) -> Self {
        TransactionError::Io(error)
    }
}

impl WriteBatch {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> &mut Self {
        self.operations.push(BatchOperation::Set { key, value });
        self
    }

    pub fn delete(&mut self, key: Vec<u8>) -> &mut Self {
        self.operations.push(BatchOperation::Delete { key });
        self
    }

    pub fn len(&self) -> usize {
        self.operations.len()
    }

    pub fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }
}

/// A single-node, crash-safe key-value store: WAL + memtable + SSTables.
pub struct Store {
    /// Sorted in-memory view of the most recent mutations (since the last flush).
    memtable: MemTable,
    /// Buffered append-only handle to the WAL file.
    wal: BufWriter<File>,
    /// Path of the WAL; also the anchor for SSTable / manifest paths.
    wal_path: PathBuf,
    /// Live SSTables, oldest first. Reads scan this in reverse (newest first).
    sstables: Vec<SsTable>,
    /// Sequence number for the next SSTable file.
    next_seq: u64,
    /// Sequence number of the latest committed mutation or batch.
    sequence: u64,
    /// Latest sequence already represented by the manifest's SSTables.
    durable_sequence: u64,
    /// Oldest sequence for which historical reads remain complete.
    history_start: u64,
    /// Last commit sequence for keys changed since this Store was opened.
    key_sequences: BTreeMap<Vec<u8>, u64>,
    /// Flush the memtable once it reaches this many entries.
    memtable_limit: usize,
    /// Compact after reaching this many SSTables; `None` disables automation.
    compaction_threshold: Option<usize>,
}

impl Store {
    /// Opens (or creates) a store whose WAL lives at `wal_path`. Loads the
    /// SSTables named in the manifest, then replays any WAL records left over
    /// from writes that had not yet been flushed, rebuilding the memtable.
    pub fn open<P: AsRef<Path>>(wal_path: P) -> io::Result<Self> {
        let wal_path = wal_path.as_ref().to_path_buf();

        // 1. Load previously-flushed SSTables from the manifest.
        let manifest_path = manifest_path(&wal_path);
        let manifest = read_manifest(&manifest_path)?;
        let sstable_names = manifest.sstables;
        let dir = parent_dir(&wal_path);
        let mut sstables = Vec::with_capacity(sstable_names.len());
        for name in &sstable_names {
            let sst = SsTable::open(&dir.join(name))?;
            sstables.push(sst);
        }
        let next_seq = next_seq_from(&sstable_names);

        // 2. Replay the WAL (writes since the last flush) on top of the SSTables.
        let (memtable, sequence) = Self::replay(&wal_path, manifest.sequence)?;

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
            sequence,
            durable_sequence: manifest.sequence,
            history_start: manifest.history_start,
            key_sequences: BTreeMap::new(),
            memtable_limit: memtable_limit_from_env(),
            compaction_threshold: compaction_threshold_from_env(),
        })
    }

    /// Replays the WAL at `path` and returns the reconstructed memtable.
    ///
    /// A truncated trailing record (e.g. from a crash mid-write) is treated as
    /// "not committed" and silently dropped, rather than failing recovery. A
    /// DELETE replays as a tombstone so it still shadows any older SSTable value.
    fn replay(path: &Path, durable_sequence: u64) -> io::Result<(MemTable, u64)> {
        let mut map = BTreeMap::new();
        let mut sequence = durable_sequence;

        let file = match File::open(path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok((map, sequence)),
            Err(e) => return Err(e),
        };
        let mut reader = BufReader::new(file);

        loop {
            match read_record(&mut reader) {
                Ok(Some(record)) if record.sequence() <= durable_sequence => continue,
                Ok(Some(record)) if record.sequence() <= sequence => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "WAL sequence numbers are not strictly increasing",
                    ));
                }
                Ok(Some(record)) => {
                    sequence = record.sequence();
                    record.apply_to(&mut map);
                }
                Ok(None) => break, // clean end of log
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break, // torn tail write
                Err(e) => return Err(e),
            }
        }

        Ok((map, sequence))
    }

    /// Returns the value for `key`, if present and not deleted.
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        match self.lookup(key) {
            Some(Value::Set(value)) => Some(value),
            _ => None,
        }
    }

    /// Returns the value visible at `sequence`, if it existed and was live.
    ///
    /// Returns an error if the requested sequence predates retained history or
    /// is newer than this Store. Use [`Self::get`] for the current value.
    pub fn get_at(&self, key: &[u8], sequence: u64) -> io::Result<Option<Vec<u8>>> {
        self.validate_historical_sequence(sequence)?;
        Ok(match self.lookup_at_result(key, sequence)? {
            Some(Value::Set(value)) => Some(value),
            _ => None, // tombstone or genuinely absent
        })
    }

    /// Resolves a key across every level, newest to oldest, returning the first
    /// record found (which may be a tombstone). `None` means no level knows it.
    fn lookup(&self, key: &[u8]) -> Option<Value> {
        self.lookup_at(key, u64::MAX)
    }

    fn lookup_at(&self, key: &[u8], sequence: u64) -> Option<Value> {
        match self.lookup_at_result(key, sequence) {
            Ok(value) => value,
            Err(e) => {
                log_warn!(TARGET, "sstable read failed for a key: {e}");
                None
            }
        }
    }

    fn lookup_at_result(&self, key: &[u8], sequence: u64) -> io::Result<Option<Value>> {
        // Memtable is newest.
        if let Some(versions) = self.memtable.get(key)
            && let Some(version) = versions
                .iter()
                .rev()
                .find(|version| version.sequence <= sequence)
        {
            return Ok(Some(version.value.clone()));
        }
        // Then SSTables, newest (last pushed) first.
        for sst in self.sstables.iter().rev() {
            match sst.get_at(key, sequence) {
                Ok(Some(v)) => return Ok(Some(v)),
                Ok(None) => continue,
                Err(e) => return Err(e),
            }
        }
        Ok(None)
    }

    /// Inserts or overwrites `key` with `value`, durably. May trigger a flush.
    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> io::Result<()> {
        let sequence = self.next_sequence()?;
        write_set(&mut self.wal, sequence, &key, &value)?;
        self.wal.flush()?;
        self.key_sequences.insert(key.clone(), sequence);
        insert_version(&mut self.memtable, key, sequence, Value::Set(value));
        self.sequence = sequence;
        self.maybe_flush()
    }

    /// Records a delete for `key`, durably. Returns whether a *live* value
    /// existed beforehand (preserving the HTTP layer's 404 semantics). The
    /// delete is stored as a tombstone regardless. May trigger a flush.
    pub fn delete(&mut self, key: &[u8]) -> io::Result<bool> {
        // Existed = there is a live value at some level right now.
        let existed = matches!(self.lookup(key), Some(Value::Set(_)));

        let sequence = self.next_sequence()?;
        write_delete(&mut self.wal, sequence, key)?;
        self.wal.flush()?;
        let key = key.to_vec();
        self.key_sequences.insert(key.clone(), sequence);
        insert_version(&mut self.memtable, key, sequence, Value::Tombstone);
        self.sequence = sequence;
        self.maybe_flush()?;
        Ok(existed)
    }

    /// Commits all operations atomically as one WAL record. Recovery applies
    /// either the complete batch or none of it if the trailing record is torn.
    /// Operations are applied in order, so the last operation for a key wins.
    pub fn write_batch(&mut self, batch: WriteBatch) -> io::Result<u64> {
        if batch.is_empty() {
            return Ok(self.sequence);
        }

        let sequence = self.next_sequence()?;
        write_batch(&mut self.wal, sequence, &batch.operations)?;
        self.wal.flush()?;
        for operation in batch.operations {
            self.key_sequences
                .insert(operation.key().to_vec(), sequence);
            apply_operation(&mut self.memtable, sequence, operation);
        }
        self.sequence = sequence;
        self.maybe_flush()?;
        Ok(sequence)
    }

    fn next_sequence(&self) -> io::Result<u64> {
        self.sequence
            .checked_add(1)
            .ok_or_else(|| io::Error::other("commit sequence exhausted"))
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
            self.memtable
                .iter()
                .map(|(key, versions)| (key.as_slice(), versions.as_slice())),
        )?;

        // 2. Publish it by appending to the manifest (temp + fsync + rename).
        let mut names: Vec<String> = self.sstable_names();
        names.push(name.clone());
        write_manifest(
            &manifest_path(&self.wal_path),
            self.sequence,
            self.history_start,
            &names,
        )?;
        self.durable_sequence = self.sequence;

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
        self.maybe_compact()
    }

    fn maybe_compact(&mut self) -> io::Result<()> {
        if self
            .compaction_threshold
            .is_some_and(|threshold| self.sstables.len() >= threshold)
        {
            self.compact()?;
        }
        Ok(())
    }

    /// Merges every live SSTable into one table, retaining all ordered versions
    /// of each key. Historical values and tombstones remain available to MVCC
    /// reads; version garbage collection is a separate concern.
    ///
    /// Publication follows the same crash-safe order as [`Self::flush`]: write
    /// and fsync the replacement table, atomically publish its manifest, then
    /// remove superseded files. A crash before the manifest update leaves the
    /// old set live; a crash after it leaves harmless orphaned old files.
    pub fn compact(&mut self) -> io::Result<()> {
        self.compact_sstables(self.history_start)
    }

    /// Flushes pending writes and compacts all versions while discarding
    /// history older than `history_start`.
    ///
    /// For each key, the newest value at or before the boundary is retained as
    /// an anchor so every snapshot from the boundary onward stays correct. An
    /// anchor tombstone can be removed because a full compaction eliminates all
    /// older tables it would otherwise need to shadow. The boundary is durable
    /// and can only move forward.
    pub fn compact_with_retention(&mut self, history_start: u64) -> io::Result<()> {
        if history_start < self.history_start {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "history retention boundary cannot move backwards",
            ));
        }
        if history_start > self.sequence {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "history retention boundary is newer than the Store",
            ));
        }

        // GC covers the whole Store. Flushing first ensures old versions do not
        // remain in the WAL or memtable and reappear in a later SSTable.
        self.flush()?;
        self.compact_sstables(history_start)
    }

    fn compact_sstables(&mut self, history_start: u64) -> io::Result<()> {
        if self.sstables.is_empty() {
            if history_start != self.history_start {
                write_manifest(
                    &manifest_path(&self.wal_path),
                    self.durable_sequence,
                    history_start,
                    &[],
                )?;
            }
            self.history_start = history_start;
            return Ok(());
        }

        let merged = retain_history_from(merge_sstables(&self.sstables)?, history_start);
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
                merged
                    .iter()
                    .map(|(key, versions)| (key.as_slice(), versions.as_slice())),
            )?;
            let table = SsTable::open(&path)?;
            Some((name, table))
        };

        let replacement_names: Vec<String> = replacement
            .as_ref()
            .map(|(name, _)| vec![name.clone()])
            .unwrap_or_default();
        write_manifest(
            &manifest,
            self.durable_sequence,
            history_start,
            &replacement_names,
        )?;

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
        self.history_start = history_start;
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

    /// Overrides the automatic compaction threshold. Values greater than zero
    /// are clamped to at least 2 SSTables; zero disables automatic compaction.
    pub fn set_compaction_threshold(&mut self, threshold: usize) {
        self.compaction_threshold = match threshold {
            0 => None,
            value => Some(value.max(2)),
        };
    }

    /// Number of SSTables currently live (flushed and recorded in the manifest).
    pub fn sstable_count(&self) -> usize {
        self.sstables.len()
    }

    /// Sequence number of the latest committed mutation or atomic batch.
    pub fn current_sequence(&self) -> u64 {
        self.sequence
    }

    /// Oldest sequence for which [`Self::get_at`] and [`Self::snapshot_at`]
    /// can reconstruct a complete historical view.
    pub fn history_start_sequence(&self) -> u64 {
        self.history_start
    }

    /// Captures all currently visible live values in an immutable read-only
    /// snapshot. Later writes, flushes, and compactions do not change it.
    pub fn snapshot(&self) -> io::Result<Snapshot> {
        self.snapshot_at(self.sequence)
    }

    /// Reconstructs an immutable snapshot at a historical commit sequence.
    pub fn snapshot_at(&self, sequence: u64) -> io::Result<Snapshot> {
        self.validate_historical_sequence(sequence)?;
        let mut keys = std::collections::BTreeSet::new();
        keys.extend(self.memtable.keys().cloned());
        for sst in &self.sstables {
            keys.extend(sst.keys()?);
        }

        let mut values = BTreeMap::new();
        for key in keys {
            if let Some(Value::Set(value)) = self.lookup_at_result(&key, sequence)? {
                values.insert(key, value);
            }
        }
        Ok(Snapshot { sequence, values })
    }

    fn validate_historical_sequence(&self, sequence: u64) -> io::Result<()> {
        if sequence < self.history_start {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "sequence {sequence} predates retained history starting at {}",
                    self.history_start
                ),
            ));
        }
        if sequence > self.sequence {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "sequence is newer than the Store",
            ));
        }
        Ok(())
    }

    /// Starts an optimistic transaction over a fixed read-only snapshot.
    pub fn begin_transaction(&self) -> io::Result<Transaction> {
        let snapshot = self.snapshot()?;
        Ok(Transaction {
            base_sequence: snapshot.sequence(),
            snapshot,
            batch: WriteBatch::new(),
            overlay: BTreeMap::new(),
            read_set: BTreeSet::new(),
        })
    }

    /// Commits a transaction if no mutation has advanced the Store since its
    /// snapshot. All writes are persisted atomically through one [`WriteBatch`].
    pub fn commit_transaction(
        &mut self,
        transaction: Transaction,
    ) -> Result<u64, TransactionError> {
        let Transaction {
            base_sequence,
            batch,
            overlay,
            mut read_set,
            ..
        } = transaction;
        read_set.extend(overlay.into_keys());
        for key in read_set {
            let actual = self.key_sequences.get(&key).copied().unwrap_or(0);
            if actual > base_sequence {
                return Err(TransactionError::Conflict {
                    key,
                    expected: base_sequence,
                    actual,
                });
            }
        }
        Ok(self.write_batch(batch)?)
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

/// K-way merge sorted SSTables, oldest to newest. All unique commit sequences
/// are retained; if a sequence is duplicated, the newest source wins.
fn merge_sstables(sstables: &[SsTable]) -> io::Result<Vec<(Vec<u8>, Vec<VersionedValue>)>> {
    let sources: Vec<Vec<(Vec<u8>, Vec<VersionedValue>)>> = sstables
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
        let mut versions = BTreeMap::new();

        for (source, entries) in sources.iter().enumerate() {
            if entries
                .get(positions[source])
                .is_some_and(|(candidate, _)| candidate == &key)
            {
                for version in &entries[positions[source]].1 {
                    versions.insert(version.sequence, version.value.clone());
                }
                positions[source] += 1;
            }
        }

        merged.push((
            key,
            versions
                .into_iter()
                .map(|(sequence, value)| VersionedValue { sequence, value })
                .collect(),
        ));
    }

    Ok(merged)
}

/// Applies a global history boundary to fully merged key histories.
fn retain_history_from(
    entries: Vec<(Vec<u8>, Vec<VersionedValue>)>,
    history_start: u64,
) -> Vec<(Vec<u8>, Vec<VersionedValue>)> {
    entries
        .into_iter()
        .filter_map(|(key, versions)| {
            let after_boundary =
                versions.partition_point(|version| version.sequence <= history_start);
            let keep_from = after_boundary.saturating_sub(1);
            let mut retained = versions.into_iter().skip(keep_from).collect::<Vec<_>>();

            if retained.first().is_some_and(|version| {
                version.sequence <= history_start && version.value == Value::Tombstone
            }) {
                retained.remove(0);
            }

            (!retained.is_empty()).then_some((key, retained))
        })
        .collect()
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

#[derive(Default)]
struct Manifest {
    sequence: u64,
    history_start: u64,
    sstables: Vec<String>,
}

/// Reads the manifest metadata plus one SSTable file name per line, oldest
/// first. A missing manifest means a fresh store.
fn read_manifest(path: &Path) -> io::Result<Manifest> {
    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Manifest::default()),
        Err(e) => return Err(e),
    };
    let mut manifest = Manifest::default();
    let mut saw_sequence = false;
    let mut saw_history_start = false;
    for line in BufReader::new(file).lines() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if let Some(raw_sequence) = trimmed.strip_prefix("sequence=") {
            if saw_sequence {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "manifest contains more than one sequence",
                ));
            }
            manifest.sequence = raw_sequence.parse().map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "invalid manifest sequence")
            })?;
            saw_sequence = true;
        } else if let Some(raw_history_start) = trimmed.strip_prefix("history_start=") {
            if saw_history_start {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "manifest contains more than one history boundary",
                ));
            }
            manifest.history_start = raw_history_start.parse().map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid manifest history boundary",
                )
            })?;
            saw_history_start = true;
        } else {
            manifest.sstables.push(trimmed.to_string());
        }
    }
    if !saw_sequence {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "manifest is missing its sequence",
        ));
    }
    if !saw_history_start {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "manifest is missing its history boundary",
        ));
    }
    if manifest.history_start > manifest.sequence {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "manifest history boundary is newer than its sequence",
        ));
    }
    Ok(manifest)
}

/// Writes the manifest atomically (temp file + fsync + rename) so a crash never
/// leaves a partially-written catalog of live SSTables.
fn write_manifest(
    path: &Path,
    sequence: u64,
    history_start: u64,
    names: &[String],
) -> io::Result<()> {
    let mut tmp = path.as_os_str().to_os_string();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);

    let mut file = File::create(&tmp)?;
    writeln!(file, "sequence={sequence}")?;
    writeln!(file, "history_start={history_start}")?;
    for name in names {
        writeln!(file, "{name}")?;
    }
    file.sync_all()?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

// ---- WAL codec -------------------------------------------------------------

/// A decoded WAL record.
enum Record {
    Set {
        sequence: u64,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        sequence: u64,
        key: Vec<u8>,
    },
    Batch {
        sequence: u64,
        operations: Vec<BatchOperation>,
    },
}

impl Record {
    fn sequence(&self) -> u64 {
        match self {
            Record::Set { sequence, .. }
            | Record::Delete { sequence, .. }
            | Record::Batch { sequence, .. } => *sequence,
        }
    }

    fn apply_to(self, map: &mut MemTable) {
        match self {
            Record::Set {
                sequence,
                key,
                value,
            } => {
                insert_version(map, key, sequence, Value::Set(value));
            }
            Record::Delete { sequence, key } => {
                insert_version(map, key, sequence, Value::Tombstone);
            }
            Record::Batch {
                sequence,
                operations,
            } => {
                for operation in operations {
                    apply_operation(map, sequence, operation);
                }
            }
        }
    }
}

fn apply_operation(map: &mut MemTable, sequence: u64, operation: BatchOperation) {
    match operation {
        BatchOperation::Set { key, value } => {
            insert_version(map, key, sequence, Value::Set(value));
        }
        BatchOperation::Delete { key } => {
            insert_version(map, key, sequence, Value::Tombstone);
        }
    }
}

fn insert_version(map: &mut MemTable, key: Vec<u8>, sequence: u64, value: Value) {
    let versions = map.entry(key).or_default();
    if let Some(last) = versions.last_mut()
        && last.sequence == sequence
    {
        last.value = value;
        return;
    }
    versions.push(VersionedValue { sequence, value });
}

/// Encodes a SET record: `[OP_SET][sequence][key_len][key][val_len][value]`.
fn write_set<W: Write>(w: &mut W, sequence: u64, key: &[u8], value: &[u8]) -> io::Result<()> {
    w.write_all(&[OP_SET])?;
    w.write_all(&sequence.to_be_bytes())?;
    write_chunk(w, key)?;
    write_chunk(w, value)?;
    Ok(())
}

/// Encodes a DELETE record: `[OP_DELETE][sequence][key_len][key]`.
fn write_delete<W: Write>(w: &mut W, sequence: u64, key: &[u8]) -> io::Result<()> {
    w.write_all(&[OP_DELETE])?;
    w.write_all(&sequence.to_be_bytes())?;
    write_chunk(w, key)?;
    Ok(())
}

/// Encodes all operations into one WAL record, making the batch atomic during
/// replay because no mutation is applied until the complete record is decoded.
fn write_batch<W: Write>(
    w: &mut W,
    sequence: u64,
    operations: &[BatchOperation],
) -> io::Result<()> {
    let count = u32::try_from(operations.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "batch is too large"))?;
    w.write_all(&[OP_BATCH])?;
    w.write_all(&sequence.to_be_bytes())?;
    w.write_all(&count.to_be_bytes())?;
    for operation in operations {
        match operation {
            BatchOperation::Set { key, value } => {
                w.write_all(&[OP_SET])?;
                write_chunk(w, key)?;
                write_chunk(w, value)?;
            }
            BatchOperation::Delete { key } => {
                w.write_all(&[OP_DELETE])?;
                write_chunk(w, key)?;
            }
        }
    }
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
            let sequence = read_sequence(r)?;
            let key = read_chunk(r)?;
            let value = read_chunk(r)?;
            Ok(Some(Record::Set {
                sequence,
                key,
                value,
            }))
        }
        OP_DELETE => {
            let sequence = read_sequence(r)?;
            let key = read_chunk(r)?;
            Ok(Some(Record::Delete { sequence, key }))
        }
        OP_BATCH => {
            let sequence = read_sequence(r)?;
            let count = read_u32(r)? as usize;
            let mut operations = Vec::new();
            for _ in 0..count {
                operations.push(read_batch_operation(r)?);
            }
            Ok(Some(Record::Batch {
                sequence,
                operations,
            }))
        }
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown WAL op code: {other}"),
        )),
    }
}

fn read_batch_operation<R: Read>(r: &mut R) -> io::Result<BatchOperation> {
    match read_u8(r)? {
        OP_SET => Ok(BatchOperation::Set {
            key: read_chunk(r)?,
            value: read_chunk(r)?,
        }),
        OP_DELETE => Ok(BatchOperation::Delete {
            key: read_chunk(r)?,
        }),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown WAL batch operation: {other}"),
        )),
    }
}

fn write_chunk<W: Write>(w: &mut W, bytes: &[u8]) -> io::Result<()> {
    let len = u32::try_from(bytes.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "WAL field is too large"))?;
    w.write_all(&len.to_be_bytes())?;
    w.write_all(bytes)
}

/// Reads a length-prefixed byte chunk (`[u32 len][bytes]`).
fn read_chunk<R: Read>(r: &mut R) -> io::Result<Vec<u8>> {
    let len = read_u32(r)? as usize;
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)?;
    Ok(buf)
}

fn read_u8<R: Read>(r: &mut R) -> io::Result<u8> {
    let mut byte = [0u8; 1];
    r.read_exact(&mut byte)?;
    Ok(byte[0])
}

fn read_u32<R: Read>(r: &mut R) -> io::Result<u32> {
    let mut bytes = [0u8; 4];
    r.read_exact(&mut bytes)?;
    Ok(u32::from_be_bytes(bytes))
}

fn read_u64<R: Read>(r: &mut R) -> io::Result<u64> {
    let mut bytes = [0u8; 8];
    r.read_exact(&mut bytes)?;
    Ok(u64::from_be_bytes(bytes))
}

fn read_sequence<R: Read>(r: &mut R) -> io::Result<u64> {
    let sequence = read_u64(r)?;
    if sequence == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "WAL sequence must be greater than zero",
        ));
    }
    Ok(sequence)
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

/// Reads `KVDB_COMPACTION_THRESHOLD`. Zero disables automatic compaction;
/// positive values are clamped to at least two SSTables.
fn compaction_threshold_from_env() -> Option<usize> {
    match std::env::var("KVDB_COMPACTION_THRESHOLD") {
        Ok(value) => match value.parse::<usize>() {
            Ok(0) => None,
            Ok(value) => Some(value.max(2)),
            Err(_) => Some(DEFAULT_COMPACTION_THRESHOLD),
        },
        Err(_) => Some(DEFAULT_COMPACTION_THRESHOLD),
    }
}
