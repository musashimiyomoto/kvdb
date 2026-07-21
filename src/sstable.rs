//! Immutable, key-sorted on-disk tables -- the "SSTable" of an LSM tree.
//!
//! A [`Store`](crate::store::Store) flushes its memtable to one of these files
//! when it grows too large. Each file is written once, in ascending key order,
//! and never mutated afterwards; newer state lives in newer files (or in the
//! memtable) and shadows older files during a read.
//!
//! ## On-disk format
//!
//! Pre-release tables contain a short header, checksummed data blocks, a
//! checksummed sparse index with a Bloom filter, and a fixed-size footer:
//!
//! ```text
//!   ["KVDBSST"]
//!   [record ...]                         data blocks (64 records each)
//!   ["KVDBIDX"][block_count][record_count][min_sequence][max_sequence][Bloom filter]
//!     repeated: [first_key][start][end][records_in_block][block_crc32]
//!   [index_offset:u64 BE][index_crc32]["KVDBEND"]
//! ```
//!
//! Each record is encoded as one key plus its versions in ascending sequence:
//!
//! ```text
//!   [key_len:u32 BE][key][version_count:u32 BE]
//!     repeated: [sequence:u64 BE][flag:u8][value_len:u32 BE][value] (Set)
//!     or:       [sequence:u64 BE][flag:u8]                       (Tombstone)
//! ```
//!
//! A lookup first uses the Bloom filter to reject keys that cannot be present,
//! then binary-searches the resident sparse index and scans at most one
//! 64-record block. Opening a table reads only its footer and index; record keys
//! are not all retained in memory.

use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Cursor, Read, Seek, SeekFrom, Write};
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::checksum::{Crc32, crc32};
use crate::limits::{
    MAX_BLOOM_FILTER_BYTES, MAX_BLOOM_HASHES, MAX_KEY_BYTES, MAX_SSTABLE_RECORD_BYTES,
    MAX_VALUE_BYTES, MAX_VERSIONS_PER_KEY,
};

/// Record flags on disk.
const FLAG_SET: u8 = 0;
const FLAG_TOMBSTONE: u8 = 1;

const FILE_MAGIC: &[u8; 7] = b"KVDBSST";
const INDEX_MAGIC: &[u8; 7] = b"KVDBIDX";
const FOOTER_MAGIC: &[u8; 7] = b"KVDBEND";
const FOOTER_LEN: u64 = 19;

/// Number of sorted records covered by one sparse-index entry.
const RECORDS_PER_BLOCK: usize = 64;
const BLOOM_BITS_PER_KEY: usize = 10;
const BLOOM_HASHES: u8 = 7;
const DEFAULT_FILE_CACHE_CAPACITY: usize = 64;
const DEFAULT_BLOCK_CACHE_BYTES: usize = 64 * 1024 * 1024;

type BlockEntries = Vec<(Vec<u8>, Vec<VersionedValue>)>;
type SharedBlockEntries = Arc<BlockEntries>;

struct ChecksummedWriter<'a, W> {
    inner: &'a mut W,
    checksum: &'a mut Crc32,
}

impl<W: Write> Write for ChecksummedWriter<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let written = self.inner.write(bytes)?;
        self.checksum.update(&bytes[..written]);
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

struct ChecksummedReader<'a, R> {
    inner: R,
    checksum: &'a mut Crc32,
}

impl<R> ChecksummedReader<'_, R> {
    fn into_inner(self) -> R {
        self.inner
    }
}

impl<R: Read> Read for ChecksummedReader<'_, R> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        let read = self.inner.read(bytes)?;
        self.checksum.update(&bytes[..read]);
        Ok(read)
    }
}

impl<R: Read> ChecksummedReader<'_, std::io::Take<R>> {
    fn limit(&self) -> u64 {
        self.inner.limit()
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct SsTableWriteStats {
    pub records: u64,
    pub versions: u64,
    pub bytes: u64,
}

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

/// One durable version of a key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VersionedValue {
    pub sequence: u64,
    pub value: Value,
}

/// Point-in-time counters and residency for one Store's SSTable caches.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SsTableCacheMetrics {
    pub file_hits: u64,
    pub file_misses: u64,
    pub file_evictions: u64,
    pub open_files: usize,
    pub block_hits: u64,
    pub block_misses: u64,
    pub block_evictions: u64,
    pub resident_blocks: usize,
    pub resident_bytes: usize,
}

#[derive(Debug, Hash, PartialEq, Eq)]
struct BlockCacheKey {
    path: PathBuf,
    start: u64,
    end: u64,
}

#[derive(Debug)]
struct CachedFile {
    file: File,
    last_used: u64,
}

#[derive(Debug)]
struct CachedBlock {
    entries: SharedBlockEntries,
    charge: usize,
    last_used: u64,
}

#[derive(Debug)]
struct SsTableCacheInner {
    file_capacity: usize,
    block_capacity_bytes: usize,
    files: HashMap<PathBuf, CachedFile>,
    blocks: HashMap<BlockCacheKey, CachedBlock>,
    block_bytes: usize,
    clock: u64,
    metrics: SsTableCacheMetrics,
}

/// Shared cache for the immutable tables owned by one Store.
#[derive(Debug)]
pub(crate) struct SsTableCache {
    inner: Mutex<SsTableCacheInner>,
}

impl SsTableCache {
    pub(crate) fn from_env() -> Arc<Self> {
        Arc::new(Self::new(
            usize_env(
                "KVDB_SSTABLE_FILE_CACHE_CAPACITY",
                DEFAULT_FILE_CACHE_CAPACITY,
            ),
            usize_env("KVDB_SSTABLE_BLOCK_CACHE_BYTES", DEFAULT_BLOCK_CACHE_BYTES),
        ))
    }

    fn new(file_capacity: usize, block_capacity_bytes: usize) -> Self {
        Self {
            inner: Mutex::new(SsTableCacheInner {
                file_capacity,
                block_capacity_bytes,
                files: HashMap::new(),
                blocks: HashMap::new(),
                block_bytes: 0,
                clock: 0,
                metrics: SsTableCacheMetrics::default(),
            }),
        }
    }

    pub(crate) fn configure(&self, file_capacity: usize, block_capacity_bytes: usize) {
        let mut inner = self.lock();
        inner.file_capacity = file_capacity;
        inner.block_capacity_bytes = block_capacity_bytes;
        inner.enforce_limits();
    }

    pub(crate) fn metrics(&self) -> SsTableCacheMetrics {
        let inner = self.lock();
        SsTableCacheMetrics {
            open_files: inner.files.len(),
            resident_blocks: inner.blocks.len(),
            resident_bytes: inner.block_bytes,
            ..inner.metrics
        }
    }

    pub(crate) fn invalidate(&self, path: &Path) {
        let mut inner = self.lock();
        inner.files.remove(path);
        let retired = inner
            .blocks
            .keys()
            .filter(|key| key.path == path)
            .map(|key| (key.path.clone(), key.start, key.end))
            .collect::<Vec<_>>();
        for (path, start, end) in retired {
            if let Some(block) = inner.blocks.remove(&BlockCacheKey { path, start, end }) {
                inner.block_bytes = inner.block_bytes.saturating_sub(block.charge);
            }
        }
    }

    fn insert_open_file(&self, path: &Path, file: File) {
        let mut inner = self.lock();
        inner.insert_file(path.to_path_buf(), file);
    }

    fn version_at(
        &self,
        path: &Path,
        block: &BlockIndex,
        key: &[u8],
        sequence: u64,
    ) -> io::Result<Option<VersionedValue>> {
        let cache_key = BlockCacheKey {
            path: path.to_path_buf(),
            start: block.start,
            end: block.end,
        };
        let mut inner = self.lock();
        inner.clock = inner.clock.wrapping_add(1);
        let now = inner.clock;
        if let Some(cached) = inner.blocks.get_mut(&cache_key) {
            cached.last_used = now;
            let entries = Arc::clone(&cached.entries);
            inner.metrics.block_hits = inner.metrics.block_hits.saturating_add(1);
            return Ok(find_version(&entries, key, sequence));
        }

        inner.metrics.block_misses = inner.metrics.block_misses.saturating_add(1);
        let cache_limit = u64::try_from(inner.block_capacity_bytes).unwrap_or(u64::MAX);
        if inner.block_capacity_bytes == 0 || block.end - block.start > cache_limit {
            return inner.scan_version(path, block, key, sequence);
        }
        let bytes = inner.read_range(path, block.start, block.end)?;
        let entries = Arc::new(decode_block(block, &bytes)?);
        let charge = decoded_block_charge(&entries);
        inner.insert_block(cache_key, Arc::clone(&entries), charge);
        Ok(find_version(&entries, key, sequence))
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, SsTableCacheInner> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl SsTableCacheInner {
    fn scan_version(
        &mut self,
        path: &Path,
        block: &BlockIndex,
        key: &[u8],
        sequence: u64,
    ) -> io::Result<Option<VersionedValue>> {
        if self.file_capacity == 0 {
            self.metrics.file_misses = self.metrics.file_misses.saturating_add(1);
            let mut file = File::open(path)?;
            return scan_block_version(&mut file, block, key, sequence);
        }

        self.clock = self.clock.wrapping_add(1);
        let now = self.clock;
        if !self.files.contains_key(path) {
            self.metrics.file_misses = self.metrics.file_misses.saturating_add(1);
            let file = File::open(path)?;
            self.insert_file(path.to_path_buf(), file);
        } else {
            self.metrics.file_hits = self.metrics.file_hits.saturating_add(1);
        }
        let cached = self.files.get_mut(path).expect("file was inserted");
        cached.last_used = now;
        scan_block_version(&mut cached.file, block, key, sequence)
    }

    fn read_range(&mut self, path: &Path, start: u64, end: u64) -> io::Result<Vec<u8>> {
        let length = usize::try_from(end.saturating_sub(start))
            .map_err(|_| invalid_data("SSTable block is too large"))?;
        let mut bytes = vec![0; length];

        if self.file_capacity == 0 {
            self.metrics.file_misses = self.metrics.file_misses.saturating_add(1);
            let mut file = File::open(path)?;
            file.seek(SeekFrom::Start(start))?;
            file.read_exact(&mut bytes)?;
            return Ok(bytes);
        }

        self.clock = self.clock.wrapping_add(1);
        let now = self.clock;
        if !self.files.contains_key(path) {
            self.metrics.file_misses = self.metrics.file_misses.saturating_add(1);
            let file = File::open(path)?;
            self.insert_file(path.to_path_buf(), file);
        } else {
            self.metrics.file_hits = self.metrics.file_hits.saturating_add(1);
        }
        let cached = self.files.get_mut(path).expect("file was inserted");
        cached.last_used = now;
        cached.file.seek(SeekFrom::Start(start))?;
        cached.file.read_exact(&mut bytes)?;
        Ok(bytes)
    }

    fn insert_file(&mut self, path: PathBuf, file: File) {
        if self.file_capacity == 0 {
            return;
        }
        if self.files.contains_key(&path) {
            return;
        }
        while self.files.len() >= self.file_capacity {
            self.evict_file();
        }
        self.clock = self.clock.wrapping_add(1);
        self.files.insert(
            path,
            CachedFile {
                file,
                last_used: self.clock,
            },
        );
    }

    fn insert_block(&mut self, key: BlockCacheKey, entries: SharedBlockEntries, charge: usize) {
        if self.block_capacity_bytes == 0 || charge > self.block_capacity_bytes {
            return;
        }
        while self.block_bytes.saturating_add(charge) > self.block_capacity_bytes {
            self.evict_block();
        }
        self.clock = self.clock.wrapping_add(1);
        self.blocks.insert(
            key,
            CachedBlock {
                entries,
                charge,
                last_used: self.clock,
            },
        );
        self.block_bytes = self.block_bytes.saturating_add(charge);
    }

    fn enforce_limits(&mut self) {
        while self.files.len() > self.file_capacity {
            self.evict_file();
        }
        while self.block_bytes > self.block_capacity_bytes {
            self.evict_block();
        }
    }

    fn evict_file(&mut self) {
        let Some(path) = self
            .files
            .iter()
            .min_by_key(|(_, cached)| cached.last_used)
            .map(|(path, _)| path.clone())
        else {
            return;
        };
        self.files.remove(&path);
        self.metrics.file_evictions = self.metrics.file_evictions.saturating_add(1);
    }

    fn evict_block(&mut self) {
        let Some(key) = self
            .blocks
            .iter()
            .min_by_key(|(_, cached)| cached.last_used)
            .map(|(key, _)| BlockCacheKey {
                path: key.path.clone(),
                start: key.start,
                end: key.end,
            })
        else {
            return;
        };
        if let Some(block) = self.blocks.remove(&key) {
            self.block_bytes = self.block_bytes.saturating_sub(block.charge);
            self.metrics.block_evictions = self.metrics.block_evictions.saturating_add(1);
        }
    }
}

#[derive(Clone, Debug)]
struct BlockIndex {
    first_key: Vec<u8>,
    start: u64,
    end: u64,
    records: usize,
    checksum: u32,
}

#[derive(Debug)]
struct BloomFilter {
    bits: Vec<u8>,
    hashes: u8,
}

impl BloomFilter {
    fn from_hashes(hashes: &[(u64, u64)]) -> Self {
        let bit_count = hashes
            .len()
            .saturating_mul(BLOOM_BITS_PER_KEY)
            .max(64)
            .next_multiple_of(8);
        let mut filter = Self {
            bits: vec![0; bit_count / 8],
            hashes: BLOOM_HASHES,
        };
        for &(h1, h2) in hashes {
            filter.insert_hashes(h1, h2);
        }
        filter
    }

    fn may_contain(&self, key: &[u8]) -> bool {
        self.bit_positions(key)
            .all(|bit| self.bits[bit / 8] & (1 << (bit % 8)) != 0)
    }

    fn insert_hashes(&mut self, h1: u64, h2: u64) {
        let bit_count = self.bits.len() * 8;
        for bit in (0..self.hashes)
            .map(|i| h1.wrapping_add(u64::from(i).wrapping_mul(h2)) as usize % bit_count)
        {
            self.bits[bit / 8] |= 1 << (bit % 8);
        }
    }

    fn bit_positions(&self, key: &[u8]) -> impl Iterator<Item = usize> {
        let h1 = hash(key, 0xcbf2_9ce4_8422_2325);
        let h2 = hash(key, 0x9e37_79b9_7f4a_7c15) | 1;
        let bit_count = self.bits.len() * 8;
        (0..self.hashes)
            .map(move |i| h1.wrapping_add(u64::from(i).wrapping_mul(h2)) as usize % bit_count)
    }
}

fn key_hashes(key: &[u8]) -> (u64, u64) {
    (
        hash(key, 0xcbf2_9ce4_8422_2325),
        hash(key, 0x9e37_79b9_7f4a_7c15) | 1,
    )
}

/// A handle to one on-disk SSTable and its resident sparse block index.
pub struct SsTable {
    path: PathBuf,
    blocks: Vec<BlockIndex>,
    bloom: BloomFilter,
    len: usize,
    min_sequence: u64,
    max_sequence: u64,
    cache: Arc<SsTableCache>,
}

struct IndexMetadata {
    blocks: Vec<BlockIndex>,
    bloom: BloomFilter,
    records: usize,
    min_sequence: u64,
    max_sequence: u64,
}

impl SsTable {
    /// Writes sorted `entries` to a pre-release SSTable at `path`, atomically.
    /// The data and sparse index are streamed to a temp file, fsynced, and
    /// renamed into place, so a crash never leaves a half-written `path`.
    pub fn write<'a, I>(path: &Path, entries: I) -> io::Result<()>
    where
        I: IntoIterator<Item = (&'a [u8], &'a [VersionedValue])>,
    {
        Self::write_stream(
            path,
            entries
                .into_iter()
                .map(|(key, versions)| Ok((key.to_vec(), versions.to_vec()))),
        )?;
        Ok(())
    }

    /// Writes owned records as they are produced, retaining only sparse index
    /// metadata and compact Bloom hashes rather than the complete table data.
    pub(crate) fn write_stream<I>(path: &Path, entries: I) -> io::Result<SsTableWriteStats>
    where
        I: IntoIterator<Item = io::Result<(Vec<u8>, Vec<VersionedValue>)>>,
    {
        let tmp = tmp_path(path);
        let file = File::create(&tmp)?;
        let mut w = BufWriter::new(file);
        w.write_all(FILE_MAGIC)?;

        let mut blocks = Vec::<BlockIndex>::new();
        let mut bloom_hashes = Vec::new();
        let mut previous_key: Option<Vec<u8>> = None;
        let mut offset = FILE_MAGIC.len() as u64;
        let mut record_count = 0usize;
        let mut version_count = 0u64;
        let mut block_checksum = Crc32::new();
        let mut min_sequence = u64::MAX;
        let mut max_sequence = 0u64;

        for entry in entries {
            let (key, versions) = entry?;
            if previous_key
                .as_deref()
                .is_some_and(|previous| previous >= key.as_slice())
            {
                return Err(invalid_input(
                    "SSTable entries must have unique keys in ascending order",
                ));
            }
            if record_count.is_multiple_of(RECORDS_PER_BLOCK) {
                if let Some(previous) = blocks.last_mut() {
                    previous.end = offset;
                    previous.checksum = block_checksum.finish();
                }
                block_checksum = Crc32::new();
                blocks.push(BlockIndex {
                    first_key: key.clone(),
                    start: offset,
                    end: 0,
                    records: 0,
                    checksum: 0,
                });
            }

            write_record(
                &mut ChecksummedWriter {
                    inner: &mut w,
                    checksum: &mut block_checksum,
                },
                &key,
                &versions,
            )?;
            offset = offset
                .checked_add(encoded_record_len(&key, &versions)?)
                .ok_or_else(|| invalid_input("SSTable is too large"))?;
            blocks.last_mut().expect("a block was just created").records += 1;
            record_count += 1;
            version_count = version_count.saturating_add(versions.len() as u64);
            min_sequence = min_sequence.min(versions[0].sequence);
            max_sequence = max_sequence.max(versions[versions.len() - 1].sequence);
            bloom_hashes.push(key_hashes(&key));
            previous_key = Some(key);
        }

        let index_offset = offset;
        if let Some(last) = blocks.last_mut() {
            last.end = index_offset;
            last.checksum = block_checksum.finish();
        }
        let mut index_checksum = Crc32::new();
        write_index(
            &mut ChecksummedWriter {
                inner: &mut w,
                checksum: &mut index_checksum,
            },
            &blocks,
            record_count,
            if record_count == 0 { 0 } else { min_sequence },
            max_sequence,
            &BloomFilter::from_hashes(&bloom_hashes),
        )?;
        let index_offset_bytes = index_offset.to_be_bytes();
        index_checksum.update(&index_offset_bytes);
        w.write_all(&index_offset_bytes)?;
        w.write_all(&index_checksum.finish().to_be_bytes())?;
        w.write_all(FOOTER_MAGIC)?;
        w.flush()?;
        crate::failpoint::hit("sstable_before_sync");
        w.get_ref().sync_all()?;
        crate::failpoint::hit("sstable_after_sync");
        std::fs::rename(&tmp, path)?;
        crate::failpoint::hit("sstable_after_rename");
        sync_parent_directory(path)?;
        Ok(SsTableWriteStats {
            records: record_count as u64,
            versions: version_count,
            bytes: std::fs::metadata(path)?.len(),
        })
    }

    /// Opens an existing table and loads its sparse index and Bloom filter.
    pub fn open(path: &Path) -> io::Result<SsTable> {
        Self::open_with_cache(path, SsTableCache::from_env())
    }

    pub(crate) fn open_with_cache(path: &Path, cache: Arc<SsTableCache>) -> io::Result<SsTable> {
        let mut file = File::open(path)?;
        let mut magic = [0u8; FILE_MAGIC.len()];
        file.read_exact(&mut magic)?;
        if &magic != FILE_MAGIC {
            return Err(invalid_data("invalid SSTable file magic"));
        }
        let metadata = read_index(&mut file)?;
        cache.insert_open_file(path, file);

        Ok(SsTable {
            path: path.to_path_buf(),
            blocks: metadata.blocks,
            bloom: metadata.bloom,
            len: metadata.records,
            min_sequence: metadata.min_sequence,
            max_sequence: metadata.max_sequence,
            cache,
        })
    }

    /// Looks up `key`. Returns a live value, a tombstone, or no record. At most
    /// one data block is read after binary-searching the sparse index.
    pub fn get(&self, key: &[u8]) -> io::Result<Option<Value>> {
        self.get_at(key, u64::MAX)
    }

    /// Looks up the newest version whose sequence is at most `sequence`.
    pub fn get_at(&self, key: &[u8], sequence: u64) -> io::Result<Option<Value>> {
        Ok(self.version_at(key, sequence)?.map(|version| version.value))
    }

    pub(crate) fn latest_sequence(&self, key: &[u8]) -> io::Result<Option<u64>> {
        Ok(self
            .version_at(key, u64::MAX)?
            .map(|version| version.sequence))
    }

    fn version_at(&self, key: &[u8], sequence: u64) -> io::Result<Option<VersionedValue>> {
        if !self.bloom.may_contain(key) {
            return Ok(None);
        }
        let Some(block_pos) = self
            .blocks
            .partition_point(|block| block.first_key.as_slice() <= key)
            .checked_sub(1)
        else {
            return Ok(None);
        };
        self.cache
            .version_at(&self.path, &self.blocks[block_pos], key, sequence)
    }

    /// Reads every record in ascending key order. This is intentionally a disk
    /// scan: callers that need the full table must not force a dense resident
    /// index.
    pub fn entries(&self) -> io::Result<Vec<(Vec<u8>, Vec<VersionedValue>)>> {
        let mut entries = Vec::with_capacity(self.len);
        if self.blocks.is_empty() {
            return Ok(entries);
        }

        let mut file = File::open(&self.path)?;
        let mut previous_table_key: Option<Vec<u8>> = None;
        for block in &self.blocks {
            file.seek(SeekFrom::Start(block.start))?;
            let mut checksum = Crc32::new();
            let checked = ChecksummedReader {
                inner: (&mut file).take(block.end - block.start),
                checksum: &mut checksum,
            };
            let mut reader = BufReader::new(checked);
            let mut previous_key: Option<Vec<u8>> = None;
            let mut records_read = 0usize;
            while let Some((key, versions)) = read_record(&mut reader)? {
                validate_block_record(block, &key, previous_key.as_deref(), records_read)?;
                if previous_table_key
                    .as_deref()
                    .is_some_and(|previous| previous >= key.as_slice())
                {
                    return Err(invalid_data("SSTable keys are not strictly ordered"));
                }
                previous_key = Some(key.clone());
                previous_table_key = Some(key.clone());
                entries.push((key, versions));
                records_read += 1;
            }
            if records_read != block.records {
                return Err(invalid_data(
                    "SSTable block record count does not match index",
                ));
            }
            drop(reader);
            if checksum.finish() != block.checksum {
                return Err(invalid_data("SSTable block checksum mismatch"));
            }
        }

        if entries.len() != self.len {
            return Err(invalid_data("SSTable record count does not match index"));
        }
        Ok(entries)
    }

    /// Streams records in key order with at most one decoded record resident.
    pub(crate) fn iter(&self) -> io::Result<SsTableIter> {
        SsTableIter::open(&self.path, &self.blocks)
    }

    /// Reads every key in ascending order. This is intentionally a disk scan:
    /// callers that need the full key set must not force a dense resident index.
    pub fn keys(&self) -> io::Result<Vec<Vec<u8>>> {
        Ok(self.entries()?.into_iter().map(|(key, _)| key).collect())
    }

    /// Number of records (live values and tombstones) in this table.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the table holds no records.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub(crate) fn sequence_bounds(&self) -> Option<(u64, u64)> {
        (!self.is_empty()).then_some((self.min_sequence, self.max_sequence))
    }
}

pub(crate) struct SsTableIter {
    reader: BufReader<File>,
    blocks: Vec<BlockIndex>,
    block_pos: usize,
    record_pos: usize,
    block_checksum: Crc32,
    previous_key: Option<Vec<u8>>,
    failed: bool,
}

impl SsTableIter {
    fn open(path: &Path, blocks: &[BlockIndex]) -> io::Result<Self> {
        let mut reader = BufReader::new(File::open(path)?);
        let mut magic = [0u8; FILE_MAGIC.len()];
        reader.read_exact(&mut magic)?;
        if &magic != FILE_MAGIC {
            return Err(invalid_data("invalid SSTable file magic"));
        }
        Ok(Self {
            reader,
            blocks: blocks.to_vec(),
            block_pos: 0,
            record_pos: 0,
            block_checksum: Crc32::new(),
            previous_key: None,
            failed: false,
        })
    }

    fn next_record(&mut self) -> io::Result<Option<(Vec<u8>, Vec<VersionedValue>)>> {
        let Some(block) = self.blocks.get(self.block_pos) else {
            return Ok(None);
        };

        let record = read_record(&mut ChecksummedReader {
            inner: &mut self.reader,
            checksum: &mut self.block_checksum,
        })?;
        let Some((key, versions)) = record else {
            return Err(invalid_data(
                "SSTable ended before its indexed record count",
            ));
        };
        validate_block_record(block, &key, self.previous_key.as_deref(), self.record_pos)?;
        self.record_pos += 1;
        if self.record_pos == block.records {
            if self.reader.stream_position()? != block.end {
                return Err(invalid_data("SSTable record data exceeds indexed bounds"));
            }
            if self.block_checksum.finish() != block.checksum {
                return Err(invalid_data("SSTable block checksum mismatch"));
            }
            self.block_pos += 1;
            self.record_pos = 0;
            self.block_checksum = Crc32::new();
        }
        self.previous_key = Some(key.clone());
        Ok(Some((key, versions)))
    }
}

impl Iterator for SsTableIter {
    type Item = io::Result<(Vec<u8>, Vec<VersionedValue>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        match self.next_record() {
            Ok(Some(entry)) => Some(Ok(entry)),
            Ok(None) => None,
            Err(error) => {
                self.failed = true;
                Some(Err(error))
            }
        }
    }
}

fn write_index<W: Write>(
    w: &mut W,
    blocks: &[BlockIndex],
    records: usize,
    min_sequence: u64,
    max_sequence: u64,
    bloom: &BloomFilter,
) -> io::Result<()> {
    let block_count =
        u32::try_from(blocks.len()).map_err(|_| invalid_input("SSTable has too many blocks"))?;
    let record_count =
        u64::try_from(records).map_err(|_| invalid_input("SSTable has too many records"))?;
    let bloom_len = u32::try_from(bloom.bits.len())
        .map_err(|_| invalid_input("SSTable Bloom filter is too large"))?;

    w.write_all(INDEX_MAGIC)?;
    w.write_all(&block_count.to_be_bytes())?;
    w.write_all(&record_count.to_be_bytes())?;
    w.write_all(&min_sequence.to_be_bytes())?;
    w.write_all(&max_sequence.to_be_bytes())?;
    w.write_all(&[bloom.hashes])?;
    w.write_all(&bloom_len.to_be_bytes())?;
    w.write_all(&bloom.bits)?;
    for block in blocks {
        write_chunk(w, &block.first_key)?;
        w.write_all(&block.start.to_be_bytes())?;
        w.write_all(&block.end.to_be_bytes())?;
        let count = u32::try_from(block.records)
            .map_err(|_| invalid_input("SSTable block has too many records"))?;
        w.write_all(&count.to_be_bytes())?;
        w.write_all(&block.checksum.to_be_bytes())?;
    }
    Ok(())
}

fn read_index(file: &mut File) -> io::Result<IndexMetadata> {
    let file_len = file.metadata()?.len();
    let minimum_len =
        FILE_MAGIC.len() as u64 + INDEX_MAGIC.len() as u64 + 4 + 8 + 8 + 8 + 5 + FOOTER_LEN;
    if file_len < minimum_len {
        return Err(invalid_data(
            "SSTable is too short for its header and index",
        ));
    }

    let footer_offset = file_len - FOOTER_LEN;
    file.seek(SeekFrom::Start(footer_offset))?;
    let mut index_offset_bytes = [0u8; 8];
    file.read_exact(&mut index_offset_bytes)?;
    let index_offset = u64::from_be_bytes(index_offset_bytes);
    let stored_index_checksum = read_u32(file)?;
    let mut actual_footer_magic = [0u8; FOOTER_MAGIC.len()];
    file.read_exact(&mut actual_footer_magic)?;
    if &actual_footer_magic != FOOTER_MAGIC {
        return Err(invalid_data("invalid SSTable footer magic"));
    }
    if index_offset < FILE_MAGIC.len() as u64 || index_offset >= footer_offset {
        return Err(invalid_data("invalid SSTable index offset"));
    }

    file.seek(SeekFrom::Start(index_offset))?;
    let mut index_checksum = Crc32::new();
    let mut reader = ChecksummedReader {
        inner: file.take(footer_offset - index_offset),
        checksum: &mut index_checksum,
    };
    let mut actual_index_magic = [0u8; INDEX_MAGIC.len()];
    reader.read_exact(&mut actual_index_magic)?;
    if &actual_index_magic != INDEX_MAGIC {
        return Err(invalid_data("invalid SSTable index magic"));
    }

    let block_count = read_u32(&mut reader)? as usize;
    let record_count = usize::try_from(read_u64(&mut reader)?)
        .map_err(|_| invalid_data("SSTable record count is too large"))?;
    let min_sequence = read_u64(&mut reader)?;
    let max_sequence = read_u64(&mut reader)?;
    let hashes = read_u8(&mut reader)?;
    let bloom_len = u64::from(read_u32(&mut reader)?);
    if hashes == 0
        || hashes > MAX_BLOOM_HASHES
        || bloom_len == 0
        || bloom_len > MAX_BLOOM_FILTER_BYTES as u64
        || bloom_len > reader.limit()
    {
        return Err(invalid_data("SSTable Bloom filter has invalid bounds"));
    }
    let mut bits = vec![0; bloom_len as usize];
    reader.read_exact(&mut bits)?;
    let bloom = BloomFilter { bits, hashes };
    // Even an empty-key index entry needs 28 bytes. Reject a corrupt count
    // before using it as a potentially enormous allocation capacity.
    if block_count as u64 > reader.limit() / 28 {
        return Err(invalid_data("SSTable block count exceeds index size"));
    }
    let mut blocks = Vec::with_capacity(block_count);
    for _ in 0..block_count {
        let first_key = read_index_key(&mut reader)?;
        let start = read_u64(&mut reader)?;
        let end = read_u64(&mut reader)?;
        let records = read_u32(&mut reader)? as usize;
        let checksum = read_u32(&mut reader)?;
        blocks.push(BlockIndex {
            first_key,
            start,
            end,
            records,
            checksum,
        });
    }
    if reader.limit() != 0 {
        return Err(invalid_data("unexpected bytes at end of SSTable index"));
    }

    let _ = reader.into_inner();
    index_checksum.update(&index_offset_bytes);
    if index_checksum.finish() != stored_index_checksum {
        return Err(invalid_data("SSTable index checksum mismatch"));
    }
    validate_index(
        &blocks,
        record_count,
        min_sequence,
        max_sequence,
        index_offset,
    )?;
    Ok(IndexMetadata {
        blocks,
        bloom,
        records: record_count,
        min_sequence,
        max_sequence,
    })
}

fn validate_index(
    blocks: &[BlockIndex],
    record_count: usize,
    min_sequence: u64,
    max_sequence: u64,
    index_offset: u64,
) -> io::Result<()> {
    if blocks.is_empty() {
        if record_count != 0
            || min_sequence != 0
            || max_sequence != 0
            || index_offset != FILE_MAGIC.len() as u64
        {
            return Err(invalid_data("empty SSTable has an inconsistent index"));
        }
        return Ok(());
    }
    if record_count == 0 || min_sequence == 0 || max_sequence < min_sequence {
        return Err(invalid_data("SSTable sequence bounds are invalid"));
    }
    if blocks[0].start != FILE_MAGIC.len() as u64 {
        return Err(invalid_data("first SSTable block has an invalid offset"));
    }

    let mut total_records = 0usize;
    for (i, block) in blocks.iter().enumerate() {
        if block.records == 0 || block.records > RECORDS_PER_BLOCK {
            return Err(invalid_data("SSTable block has an invalid record count"));
        }
        if i + 1 < blocks.len() && block.records != RECORDS_PER_BLOCK {
            return Err(invalid_data("non-final SSTable block is not full"));
        }
        if block.start >= block.end || block.end > index_offset {
            return Err(invalid_data("SSTable block has invalid bounds"));
        }
        if i > 0 {
            let previous = &blocks[i - 1];
            if previous.end != block.start || previous.first_key >= block.first_key {
                return Err(invalid_data("SSTable block index is not ordered"));
            }
        }
        total_records = total_records
            .checked_add(block.records)
            .ok_or_else(|| invalid_data("SSTable record count overflow"))?;
    }
    if blocks.last().expect("non-empty").end != index_offset || total_records != record_count {
        return Err(invalid_data("SSTable index totals are inconsistent"));
    }
    Ok(())
}

fn validate_block_record(
    block: &BlockIndex,
    key: &[u8],
    previous_key: Option<&[u8]>,
    record_pos: usize,
) -> io::Result<()> {
    if record_pos >= block.records {
        return Err(invalid_data(
            "SSTable block contains more records than indexed",
        ));
    }
    if record_pos == 0 && key != block.first_key {
        return Err(invalid_data("SSTable block first key does not match index"));
    }
    if previous_key.is_some_and(|previous| previous >= key) {
        return Err(invalid_data("SSTable block keys are not strictly ordered"));
    }
    Ok(())
}

fn decode_block(block: &BlockIndex, bytes: &[u8]) -> io::Result<BlockEntries> {
    verify_block_checksum(block, bytes)?;
    let mut reader = Cursor::new(bytes);
    let mut entries = Vec::with_capacity(block.records);
    let mut previous_key: Option<Vec<u8>> = None;
    while let Some((key, versions)) = read_record(&mut reader)? {
        validate_block_record(block, &key, previous_key.as_deref(), entries.len())?;
        previous_key = Some(key.clone());
        entries.push((key, versions));
    }
    if entries.len() != block.records {
        return Err(invalid_data(
            "SSTable block record count does not match index",
        ));
    }
    Ok(entries)
}

fn verify_block_checksum(block: &BlockIndex, bytes: &[u8]) -> io::Result<()> {
    if crc32(&[bytes]) != block.checksum {
        return Err(invalid_data("SSTable block checksum mismatch"));
    }
    Ok(())
}

fn find_version(
    entries: &[(Vec<u8>, Vec<VersionedValue>)],
    key: &[u8],
    sequence: u64,
) -> Option<VersionedValue> {
    let record_pos = entries
        .binary_search_by(|(record_key, _)| record_key.as_slice().cmp(key))
        .ok()?;
    entries[record_pos]
        .1
        .iter()
        .rev()
        .find(|version| version.sequence <= sequence)
        .cloned()
}

fn scan_block_version(
    file: &mut File,
    block: &BlockIndex,
    key: &[u8],
    sequence: u64,
) -> io::Result<Option<VersionedValue>> {
    file.seek(SeekFrom::Start(block.start))?;
    let mut checksum = Crc32::new();
    let checked = ChecksummedReader {
        inner: file.take(block.end - block.start),
        checksum: &mut checksum,
    };
    let mut reader = BufReader::new(checked);
    let mut previous_key: Option<Vec<u8>> = None;
    let mut records_read = 0usize;
    let mut found = None;

    while let Some((record_key, versions)) = read_record(&mut reader)? {
        validate_block_record(block, &record_key, previous_key.as_deref(), records_read)?;
        records_read += 1;
        if record_key.as_slice() == key {
            found = versions
                .into_iter()
                .rev()
                .find(|version| version.sequence <= sequence);
        }
        previous_key = Some(record_key);
    }
    if records_read != block.records {
        return Err(invalid_data(
            "SSTable block record count does not match index",
        ));
    }
    drop(reader);
    if checksum.finish() != block.checksum {
        return Err(invalid_data("SSTable block checksum mismatch"));
    }
    Ok(found)
}

fn decoded_block_charge(entries: &[(Vec<u8>, Vec<VersionedValue>)]) -> usize {
    let mut bytes = entries
        .len()
        .saturating_mul(size_of::<(Vec<u8>, Vec<VersionedValue>)>());
    for (key, versions) in entries {
        bytes = bytes.saturating_add(key.capacity()).saturating_add(
            versions
                .capacity()
                .saturating_mul(size_of::<VersionedValue>()),
        );
        for version in versions {
            if let Value::Set(value) = &version.value {
                bytes = bytes.saturating_add(value.capacity());
            }
        }
    }
    bytes
}

fn usize_env(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

/// The temp path a table is staged at before being renamed into place.
fn tmp_path(path: &Path) -> PathBuf {
    let mut s = path.as_os_str().to_os_string();
    s.push(".tmp");
    PathBuf::from(s)
}

pub(crate) fn staging_path(path: &Path) -> PathBuf {
    tmp_path(path)
}

#[cfg(unix)]
fn sync_parent_directory(path: &Path) -> io::Result<()> {
    let parent = path.parent().filter(|path| !path.as_os_str().is_empty());
    File::open(parent.unwrap_or_else(|| Path::new(".")))?.sync_all()
}

#[cfg(not(unix))]
fn sync_parent_directory(_path: &Path) -> io::Result<()> {
    Ok(())
}

/// Encodes one key and all of its versions (see the module-level format docs).
fn write_record<W: Write>(w: &mut W, key: &[u8], versions: &[VersionedValue]) -> io::Result<()> {
    validate_record(key, versions, io::ErrorKind::InvalidInput)?;
    write_chunk(w, key)?;
    let count = u32::try_from(versions.len())
        .map_err(|_| invalid_input("SSTable key has too many versions"))?;
    w.write_all(&count.to_be_bytes())?;
    for version in versions {
        w.write_all(&version.sequence.to_be_bytes())?;
        match &version.value {
            Value::Set(value) => {
                w.write_all(&[FLAG_SET])?;
                write_chunk(w, value)?;
            }
            Value::Tombstone => w.write_all(&[FLAG_TOMBSTONE])?,
        }
    }
    Ok(())
}

fn encoded_record_len(key: &[u8], versions: &[VersionedValue]) -> io::Result<u64> {
    validate_record(key, versions, io::ErrorKind::InvalidInput)?;
    let mut len = 4u64
        .checked_add(key.len() as u64)
        .and_then(|len| len.checked_add(4))
        .ok_or_else(|| invalid_input("SSTable record is too large"))?;
    for version in versions {
        let version_len = match &version.value {
            Value::Set(value) => 8 + 1 + 4 + value.len() as u64,
            Value::Tombstone => 8 + 1,
        };
        len = len
            .checked_add(version_len)
            .ok_or_else(|| invalid_input("SSTable record is too large"))?;
    }
    if len > MAX_SSTABLE_RECORD_BYTES as u64 {
        return Err(invalid_input("SSTable record exceeds size limit"));
    }
    Ok(len)
}

fn write_chunk<W: Write>(w: &mut W, bytes: &[u8]) -> io::Result<()> {
    let len = u32::try_from(bytes.len()).map_err(|_| invalid_input("record field is too large"))?;
    w.write_all(&len.to_be_bytes())?;
    w.write_all(bytes)
}

/// Reads one record. A torn record is corruption because SSTables are atomic.
fn read_record<R: Read>(r: &mut R) -> io::Result<Option<(Vec<u8>, Vec<VersionedValue>)>> {
    let mut key_len = [0u8; 4];
    match r.read_exact(&mut key_len[..1]) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    r.read_exact(&mut key_len[1..])?;
    let key_len = u32::from_be_bytes(key_len) as usize;
    if key_len > MAX_KEY_BYTES {
        return Err(invalid_data("SSTable key exceeds size limit"));
    }
    let mut key = vec![0u8; key_len];
    r.read_exact(&mut key)?;

    let count = read_u32(r)? as usize;
    if count == 0 || count > MAX_VERSIONS_PER_KEY {
        return Err(invalid_data("SSTable version count exceeds limit"));
    }
    let mut versions = Vec::with_capacity(count.min(1024));
    let mut record_bytes = 4usize + key.len() + 4;
    for _ in 0..count {
        let sequence = read_u64(r)?;
        let value = match read_u8(r)? {
            FLAG_SET => Value::Set(read_chunk(r, MAX_VALUE_BYTES, "SSTable value")?),
            FLAG_TOMBSTONE => Value::Tombstone,
            other => {
                return Err(invalid_data(format!(
                    "unknown SSTable version flag: {other}"
                )));
            }
        };
        record_bytes = record_bytes
            .checked_add(8 + 1 + value_bytes(&value))
            .ok_or_else(|| invalid_data("SSTable record size overflow"))?;
        if record_bytes > MAX_SSTABLE_RECORD_BYTES {
            return Err(invalid_data("SSTable record exceeds size limit"));
        }
        versions.push(VersionedValue { sequence, value });
    }
    validate_versions(&versions, io::ErrorKind::InvalidData)?;
    Ok(Some((key, versions)))
}

fn validate_versions(versions: &[VersionedValue], kind: io::ErrorKind) -> io::Result<()> {
    if versions.is_empty() || versions.len() > MAX_VERSIONS_PER_KEY {
        return Err(io::Error::new(kind, "SSTable version count exceeds limit"));
    }
    let mut previous = 0;
    for version in versions {
        if version.sequence == 0 || version.sequence <= previous {
            return Err(io::Error::new(
                kind,
                "SSTable versions are not strictly increasing",
            ));
        }
        previous = version.sequence;
    }
    Ok(())
}

fn validate_record(key: &[u8], versions: &[VersionedValue], kind: io::ErrorKind) -> io::Result<()> {
    if key.len() > MAX_KEY_BYTES {
        return Err(io::Error::new(kind, "SSTable key exceeds size limit"));
    }
    validate_versions(versions, kind)?;
    let mut encoded_bytes = 4usize + key.len() + 4;
    for version in versions {
        if let Value::Set(value) = &version.value
            && value.len() > MAX_VALUE_BYTES
        {
            return Err(io::Error::new(kind, "SSTable value exceeds size limit"));
        }
        encoded_bytes = encoded_bytes
            .checked_add(8 + 1 + value_bytes(&version.value))
            .ok_or_else(|| io::Error::new(kind, "SSTable record size overflow"))?;
        if encoded_bytes > MAX_SSTABLE_RECORD_BYTES {
            return Err(io::Error::new(kind, "SSTable record exceeds size limit"));
        }
    }
    Ok(())
}

fn value_bytes(value: &Value) -> usize {
    match value {
        Value::Set(value) => 4 + value.len(),
        Value::Tombstone => 0,
    }
}

fn read_index_key<R: Read>(r: &mut ChecksummedReader<'_, std::io::Take<R>>) -> io::Result<Vec<u8>> {
    let len = read_u32(r)? as u64;
    if len > MAX_KEY_BYTES as u64 || len > r.limit() {
        return Err(invalid_data("SSTable index key exceeds index bounds"));
    }
    let mut key = vec![0u8; len as usize];
    r.read_exact(&mut key)?;
    Ok(key)
}

fn read_chunk<R: Read>(r: &mut R, max: usize, field: &str) -> io::Result<Vec<u8>> {
    let len = read_u32(r)? as usize;
    if len > max {
        return Err(invalid_data(format!("{field} exceeds size limit")));
    }
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)?;
    Ok(buf)
}

fn read_u32<R: Read>(r: &mut R) -> io::Result<u32> {
    let mut bytes = [0u8; 4];
    r.read_exact(&mut bytes)?;
    Ok(u32::from_be_bytes(bytes))
}

fn read_u8<R: Read>(r: &mut R) -> io::Result<u8> {
    let mut byte = [0u8; 1];
    r.read_exact(&mut byte)?;
    Ok(byte[0])
}

fn read_u64<R: Read>(r: &mut R) -> io::Result<u64> {
    let mut bytes = [0u8; 8];
    r.read_exact(&mut bytes)?;
    Ok(u64::from_be_bytes(bytes))
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn invalid_input(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

/// A stable, dependency-free 64-bit hash for the Bloom filter.
fn hash(bytes: &[u8], seed: u64) -> u64 {
    bytes.iter().fold(seed, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
    })
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

    fn version(sequence: u64, value: Value) -> Vec<VersionedValue> {
        vec![VersionedValue { sequence, value }]
    }

    #[test]
    fn roundtrips_values_and_tombstones() {
        let path = tmp("roundtrip");
        let entries: Vec<(Vec<u8>, Vec<VersionedValue>)> = vec![
            (b"a".to_vec(), version(1, Value::Set(b"1".to_vec()))),
            (b"b".to_vec(), version(2, Value::Tombstone)),
            (b"c".to_vec(), version(3, Value::Set(b"three".to_vec()))),
        ];
        SsTable::write(
            &path,
            entries
                .iter()
                .map(|(key, versions)| (key.as_slice(), versions.as_slice())),
        )
        .unwrap();

        let sst = SsTable::open(&path).unwrap();
        assert_eq!(sst.len(), 3);
        assert_eq!(sst.blocks.len(), 1);
        assert!(sst.bloom.may_contain(b"a"));
        assert_eq!(sst.get(b"a").unwrap(), Some(Value::Set(b"1".to_vec())));
        assert_eq!(sst.get(b"b").unwrap(), Some(Value::Tombstone));
        assert_eq!(sst.get(b"c").unwrap(), Some(Value::Set(b"three".to_vec())));
        assert_eq!(sst.get(b"missing").unwrap(), None);
        assert_eq!(
            sst.keys().unwrap(),
            [b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]
        );

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn returns_the_version_visible_at_a_sequence() {
        let path = tmp("versions");
        let versions = vec![
            VersionedValue {
                sequence: 1,
                value: Value::Set(b"old".to_vec()),
            },
            VersionedValue {
                sequence: 2,
                value: Value::Set(b"new".to_vec()),
            },
            VersionedValue {
                sequence: 3,
                value: Value::Tombstone,
            },
            VersionedValue {
                sequence: 4,
                value: Value::Set(b"revived".to_vec()),
            },
        ];
        let entries = [(b"key".as_slice(), versions)];
        SsTable::write(
            &path,
            entries
                .iter()
                .map(|(key, versions)| (*key, versions.as_slice())),
        )
        .unwrap();

        let sst = SsTable::open(&path).unwrap();
        assert_eq!(sst.get_at(b"key", 0).unwrap(), None);
        assert_eq!(
            sst.get_at(b"key", 1).unwrap(),
            Some(Value::Set(b"old".to_vec()))
        );
        assert_eq!(
            sst.get_at(b"key", 2).unwrap(),
            Some(Value::Set(b"new".to_vec()))
        );
        assert_eq!(sst.get_at(b"key", 3).unwrap(), Some(Value::Tombstone));
        assert_eq!(
            sst.get_at(b"key", 4).unwrap(),
            Some(Value::Set(b"revived".to_vec()))
        );

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn bloom_filter_has_no_false_negatives_and_skips_a_data_block_on_miss() {
        let path = tmp("bloom");
        let entries: Vec<(Vec<u8>, Vec<VersionedValue>)> = (0..20)
            .map(|i| {
                (
                    format!("key-{i:04}").into_bytes(),
                    version(i + 1, Value::Set(format!("value-{i}").into_bytes())),
                )
            })
            .collect();
        SsTable::write(
            &path,
            entries
                .iter()
                .map(|(key, versions)| (key.as_slice(), versions.as_slice())),
        )
        .unwrap();

        let sst = SsTable::open(&path).unwrap();
        let bloom = &sst.bloom;
        for (key, _) in &entries {
            assert!(bloom.may_contain(key), "Bloom filter lost key {key:?}");
        }
        let missing = (0..10_000)
            .map(|i| format!("missing-{i}").into_bytes())
            .find(|key| !bloom.may_contain(key))
            .expect("a Bloom filter should reject at least one missing key");

        // Corrupt the first record after opening. A negative Bloom result must
        // return before opening and scanning this damaged data block.
        {
            use std::io::{Seek, Write};

            let mut file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
            file.seek(SeekFrom::Start(FILE_MAGIC.len() as u64)).unwrap();
            file.write_all(&[0xFF]).unwrap();
        }
        assert_eq!(sst.get(&missing).unwrap(), None);

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn sparse_index_finds_keys_at_block_boundaries() {
        let path = tmp("blocks");
        let entries: Vec<(Vec<u8>, Vec<VersionedValue>)> = (0..(RECORDS_PER_BLOCK * 2 + 5))
            .map(|i| {
                (
                    format!("key-{i:04}").into_bytes(),
                    version(i as u64 + 1, Value::Set(format!("value-{i}").into_bytes())),
                )
            })
            .collect();
        SsTable::write(
            &path,
            entries
                .iter()
                .map(|(key, versions)| (key.as_slice(), versions.as_slice())),
        )
        .unwrap();

        let sst = SsTable::open(&path).unwrap();
        assert_eq!(sst.blocks.len(), 3);
        assert_eq!(sst.len(), entries.len());
        for i in [0, RECORDS_PER_BLOCK - 1, RECORDS_PER_BLOCK, 127, 128, 132] {
            assert_eq!(
                sst.get(format!("key-{i:04}").as_bytes()).unwrap(),
                Some(Value::Set(format!("value-{i}").into_bytes()))
            );
        }
        assert_eq!(sst.get(b"key-0063x").unwrap(), None);
        assert_eq!(sst.get(b"aaa").unwrap(), None);
        assert_eq!(sst.get(b"zzz").unwrap(), None);

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn file_and_block_caches_hit_and_respect_limits() {
        let path = tmp("cache-bounds");
        let entries: Vec<(Vec<u8>, Vec<VersionedValue>)> = (0..(RECORDS_PER_BLOCK * 3))
            .map(|i| {
                (
                    format!("key-{i:04}").into_bytes(),
                    version(i as u64 + 1, Value::Set(vec![b'v'; 16])),
                )
            })
            .collect();
        SsTable::write(
            &path,
            entries
                .iter()
                .map(|(key, versions)| (key.as_slice(), versions.as_slice())),
        )
        .unwrap();

        let cache = Arc::new(SsTableCache::new(1, usize::MAX));
        let sst = SsTable::open_with_cache(&path, Arc::clone(&cache)).unwrap();
        assert!(sst.get(b"key-0000").unwrap().is_some());
        assert!(sst.get(b"key-0001").unwrap().is_some());
        let first = cache.metrics();
        assert_eq!(first.block_misses, 1);
        assert_eq!(first.block_hits, 1);
        assert_eq!(first.open_files, 1);
        assert_eq!(first.resident_blocks, 1);

        cache.configure(1, first.resident_bytes);
        assert!(sst.get(b"key-0064").unwrap().is_some());
        let bounded = cache.metrics();
        assert_eq!(bounded.resident_blocks, 1);
        assert!(bounded.resident_bytes <= first.resident_bytes);
        assert_eq!(bounded.block_evictions, 1);

        cache.configure(0, 0);
        let before_uncached = cache.metrics();
        assert!(sst.get(b"key-0128").unwrap().is_some());
        assert!(sst.get(b"key-0129").unwrap().is_some());
        let uncached = cache.metrics();
        assert_eq!(uncached.open_files, 0);
        assert_eq!(uncached.resident_blocks, 0);
        assert_eq!(uncached.file_misses - before_uncached.file_misses, 2);
        assert_eq!(uncached.block_misses - before_uncached.block_misses, 2);

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn data_block_checksum_detects_corruption_on_every_read_path() {
        let path = tmp("block-checksum");
        let entries: Vec<(Vec<u8>, Vec<VersionedValue>)> = (0..4)
            .map(|i| {
                (
                    format!("key-{i}").into_bytes(),
                    version(i + 1, Value::Set(format!("value-{i}").into_bytes())),
                )
            })
            .collect();
        SsTable::write(
            &path,
            entries
                .iter()
                .map(|(key, versions)| (key.as_slice(), versions.as_slice())),
        )
        .unwrap();

        let sst = SsTable::open(&path).unwrap();
        let corrupt_offset = sst.blocks[0].end - 1;
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        file.seek(SeekFrom::Start(corrupt_offset)).unwrap();
        let byte = read_u8(&mut file).unwrap();
        file.seek(SeekFrom::Start(corrupt_offset)).unwrap();
        file.write_all(&[byte ^ 1]).unwrap();
        drop(file);

        let error = sst.get(b"key-0").unwrap_err();
        assert!(error.to_string().contains("block checksum mismatch"));
        sst.cache.configure(0, 0);
        let error = sst.get(b"key-0").unwrap_err();
        assert!(error.to_string().contains("block checksum mismatch"));
        let error = sst.entries().unwrap_err();
        assert!(error.to_string().contains("block checksum mismatch"));
        let error = sst
            .iter()
            .unwrap()
            .find_map(Result::err)
            .expect("streaming iteration must detect block corruption");
        assert!(error.to_string().contains("block checksum mismatch"));

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn index_checksum_detects_corruption_during_open() {
        let path = tmp("index-checksum");
        let entries = [(b"key".as_slice(), version(1, Value::Set(b"value".to_vec())))];
        SsTable::write(
            &path,
            entries
                .iter()
                .map(|(key, versions)| (*key, versions.as_slice())),
        )
        .unwrap();

        let file_len = std::fs::metadata(&path).unwrap().len();
        let corrupt_offset = file_len - FOOTER_LEN - 1;
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        file.seek(SeekFrom::Start(corrupt_offset)).unwrap();
        let byte = read_u8(&mut file).unwrap();
        file.seek(SeekFrom::Start(corrupt_offset)).unwrap();
        file.write_all(&[byte ^ 1]).unwrap();
        drop(file);

        let error = match SsTable::open(&path) {
            Ok(_) => panic!("expected index checksum corruption to fail"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("index checksum mismatch"));
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn rejects_unsorted_entries() {
        let path = tmp("unsorted");
        let entries = [
            (b"b".as_slice(), version(1, Value::Set(b"2".to_vec()))),
            (b"a".as_slice(), version(2, Value::Set(b"1".to_vec()))),
        ];
        let error = SsTable::write(
            &path,
            entries
                .iter()
                .map(|(key, versions)| (*key, versions.as_slice())),
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        std::fs::remove_file(tmp_path(&path)).ok();
    }
}
