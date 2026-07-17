//! Immutable, key-sorted on-disk tables -- the "SSTable" of an LSM tree.
//!
//! A [`Store`](crate::store::Store) flushes its memtable to one of these files
//! when it grows too large. Each file is written once, in ascending key order,
//! and never mutated afterwards; newer state lives in newer files (or in the
//! memtable) and shadows older files during a read.
//!
//! ## On-disk format
//!
//! Version 1 tables contain a short header, a flat sequence of sorted records,
//! a sparse block index with a Bloom filter, and a fixed-size footer:
//!
//! ```text
//!   ["KVDBSST1"]
//!   [record ...]                         data blocks (64 records each)
//!   ["KVDBIDX1"][block_count][record_count][Bloom filter]
//!     repeated: [first_key][start][end][records_in_block]
//!   [index_offset:u64 BE]["KVDBEND1"]
//! ```
//!
//! Each record is encoded as:
//!
//! ```text
//!   [flag:u8][key_len:u32 BE][key]            (flag = 1: Tombstone)
//!   [flag:u8][key_len:u32 BE][key][val_len:u32 BE][value]   (flag = 0: Set)
//! ```
//!
//! A lookup first uses the Bloom filter to reject keys that cannot be present,
//! then binary-searches the resident sparse index and scans at most one
//! 64-record block. Opening a table reads only its footer and index; record keys
//! are not all retained in memory.

use std::cmp::Ordering;
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// Record flags on disk.
const FLAG_SET: u8 = 0;
const FLAG_TOMBSTONE: u8 = 1;

const FILE_MAGIC: &[u8; 8] = b"KVDBSST1";
const INDEX_MAGIC: &[u8; 8] = b"KVDBIDX1";
const FOOTER_MAGIC: &[u8; 8] = b"KVDBEND1";
const FOOTER_LEN: u64 = 16;

/// Number of sorted records covered by one sparse-index entry.
const RECORDS_PER_BLOCK: usize = 64;
const BLOOM_BITS_PER_KEY: usize = 10;
const BLOOM_HASHES: u8 = 7;

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
        let head = 1 + 4 + key.len() as u64;
        match self {
            Value::Set(v) => head + 4 + v.len() as u64,
            Value::Tombstone => head,
        }
    }
}

#[derive(Debug)]
struct BlockIndex {
    first_key: Vec<u8>,
    start: u64,
    end: u64,
    records: usize,
}

#[derive(Debug)]
struct BloomFilter {
    bits: Vec<u8>,
    hashes: u8,
}

impl BloomFilter {
    fn from_keys(keys: &[Vec<u8>]) -> Self {
        let bit_count = keys
            .len()
            .saturating_mul(BLOOM_BITS_PER_KEY)
            .max(64)
            .next_multiple_of(8);
        let mut filter = Self {
            bits: vec![0; bit_count / 8],
            hashes: BLOOM_HASHES,
        };
        for key in keys {
            filter.insert(key);
        }
        filter
    }

    fn may_contain(&self, key: &[u8]) -> bool {
        self.bit_positions(key)
            .all(|bit| self.bits[bit / 8] & (1 << (bit % 8)) != 0)
    }

    fn insert(&mut self, key: &[u8]) {
        for bit in self.bit_positions(key).collect::<Vec<_>>() {
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

/// A handle to one on-disk SSTable and its resident sparse block index.
pub struct SsTable {
    path: PathBuf,
    blocks: Vec<BlockIndex>,
    bloom: BloomFilter,
    len: usize,
}

impl SsTable {
    /// Writes sorted `entries` to a version 1 SSTable at `path`, atomically.
    /// The data and sparse index are streamed to a temp file, fsynced, and
    /// renamed into place, so a crash never leaves a half-written `path`.
    pub fn write<'a, I>(path: &Path, entries: I) -> io::Result<()>
    where
        I: IntoIterator<Item = (&'a [u8], &'a Value)>,
    {
        let tmp = tmp_path(path);
        let file = File::create(&tmp)?;
        let mut w = BufWriter::new(file);
        w.write_all(FILE_MAGIC)?;

        let mut blocks = Vec::<BlockIndex>::new();
        let mut bloom_keys = Vec::new();
        let mut previous_key: Option<Vec<u8>> = None;
        let mut offset = FILE_MAGIC.len() as u64;
        let mut record_count = 0usize;

        for (key, value) in entries {
            if previous_key
                .as_deref()
                .is_some_and(|previous| previous >= key)
            {
                return Err(invalid_input(
                    "SSTable entries must have unique keys in ascending order",
                ));
            }
            if record_count.is_multiple_of(RECORDS_PER_BLOCK) {
                if let Some(previous) = blocks.last_mut() {
                    previous.end = offset;
                }
                blocks.push(BlockIndex {
                    first_key: key.to_vec(),
                    start: offset,
                    end: 0,
                    records: 0,
                });
            }

            write_record(&mut w, key, value)?;
            offset = offset
                .checked_add(value.encoded_len(key))
                .ok_or_else(|| invalid_input("SSTable is too large"))?;
            blocks.last_mut().expect("a block was just created").records += 1;
            record_count += 1;
            bloom_keys.push(key.to_vec());
            previous_key = Some(key.to_vec());
        }

        let index_offset = offset;
        if let Some(last) = blocks.last_mut() {
            last.end = index_offset;
        }
        write_index(
            &mut w,
            &blocks,
            record_count,
            &BloomFilter::from_keys(&bloom_keys),
        )?;
        w.write_all(&index_offset.to_be_bytes())?;
        w.write_all(FOOTER_MAGIC)?;
        w.flush()?;
        w.get_ref().sync_all()?;
        std::fs::rename(&tmp, path)?;
        Ok(())
    }

    /// Opens an existing table and loads its sparse index and Bloom filter.
    pub fn open(path: &Path) -> io::Result<SsTable> {
        let mut file = File::open(path)?;
        let mut magic = [0u8; FILE_MAGIC.len()];
        file.read_exact(&mut magic)?;
        if &magic != FILE_MAGIC {
            return Err(invalid_data("invalid SSTable file magic"));
        }
        let (blocks, bloom, len) = read_index(&mut file)?;

        Ok(SsTable {
            path: path.to_path_buf(),
            blocks,
            bloom,
            len,
        })
    }

    /// Looks up `key`. Returns a live value, a tombstone, or no record. At most
    /// one data block is read after binary-searching the sparse index.
    pub fn get(&self, key: &[u8]) -> io::Result<Option<Value>> {
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
        let block = &self.blocks[block_pos];

        let mut file = File::open(&self.path)?;
        file.seek(SeekFrom::Start(block.start))?;
        let mut reader = BufReader::new(file.take(block.end - block.start));
        let mut previous_key: Option<Vec<u8>> = None;
        let mut records_read = 0usize;

        while let Some((record_key, value)) = read_record(&mut reader)? {
            validate_block_record(block, &record_key, previous_key.as_deref(), records_read)?;
            records_read += 1;
            match record_key.as_slice().cmp(key) {
                Ordering::Less => previous_key = Some(record_key),
                Ordering::Equal => return Ok(Some(value)),
                Ordering::Greater => return Ok(None),
            }
        }
        if records_read != block.records {
            return Err(invalid_data(
                "SSTable block record count does not match index",
            ));
        }
        Ok(None)
    }

    /// Reads every record in ascending key order. This is intentionally a disk
    /// scan: callers that need the full table must not force a dense resident
    /// index.
    pub fn entries(&self) -> io::Result<Vec<(Vec<u8>, Value)>> {
        let mut entries = Vec::with_capacity(self.len);
        if self.blocks.is_empty() {
            return Ok(entries);
        }

        let mut file = File::open(&self.path)?;
        let mut previous_table_key: Option<Vec<u8>> = None;
        for block in &self.blocks {
            file.seek(SeekFrom::Start(block.start))?;
            let mut reader = BufReader::new((&mut file).take(block.end - block.start));
            let mut previous_key: Option<Vec<u8>> = None;
            let mut records_read = 0usize;
            while let Some((key, value)) = read_record(&mut reader)? {
                validate_block_record(block, &key, previous_key.as_deref(), records_read)?;
                if previous_table_key
                    .as_deref()
                    .is_some_and(|previous| previous >= key.as_slice())
                {
                    return Err(invalid_data("SSTable keys are not strictly ordered"));
                }
                previous_key = Some(key.clone());
                previous_table_key = Some(key.clone());
                entries.push((key, value));
                records_read += 1;
            }
            if records_read != block.records {
                return Err(invalid_data(
                    "SSTable block record count does not match index",
                ));
            }
        }

        if entries.len() != self.len {
            return Err(invalid_data("SSTable record count does not match index"));
        }
        Ok(entries)
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
}

fn write_index<W: Write>(
    w: &mut W,
    blocks: &[BlockIndex],
    records: usize,
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
    }
    Ok(())
}

fn read_index(file: &mut File) -> io::Result<(Vec<BlockIndex>, BloomFilter, usize)> {
    let file_len = file.metadata()?.len();
    let minimum_len = FILE_MAGIC.len() as u64 + INDEX_MAGIC.len() as u64 + 4 + 8 + 5 + FOOTER_LEN;
    if file_len < minimum_len {
        return Err(invalid_data(
            "SSTable is too short for its header and index",
        ));
    }

    let footer_offset = file_len - FOOTER_LEN;
    file.seek(SeekFrom::Start(footer_offset))?;
    let index_offset = read_u64(file)?;
    let mut actual_footer_magic = [0u8; FOOTER_MAGIC.len()];
    file.read_exact(&mut actual_footer_magic)?;
    if &actual_footer_magic != FOOTER_MAGIC {
        return Err(invalid_data("invalid SSTable footer magic"));
    }
    if index_offset < FILE_MAGIC.len() as u64 || index_offset >= footer_offset {
        return Err(invalid_data("invalid SSTable index offset"));
    }

    file.seek(SeekFrom::Start(index_offset))?;
    let mut reader = file.take(footer_offset - index_offset);
    let mut actual_index_magic = [0u8; INDEX_MAGIC.len()];
    reader.read_exact(&mut actual_index_magic)?;
    if &actual_index_magic != INDEX_MAGIC {
        return Err(invalid_data("invalid SSTable index magic"));
    }

    let block_count = read_u32(&mut reader)? as usize;
    let record_count = usize::try_from(read_u64(&mut reader)?)
        .map_err(|_| invalid_data("SSTable record count is too large"))?;
    let hashes = read_u8(&mut reader)?;
    let bloom_len = u64::from(read_u32(&mut reader)?);
    if hashes == 0 || bloom_len == 0 || bloom_len > reader.limit() {
        return Err(invalid_data("SSTable Bloom filter has invalid bounds"));
    }
    let mut bits = vec![0; bloom_len as usize];
    reader.read_exact(&mut bits)?;
    let bloom = BloomFilter { bits, hashes };
    // Even an empty-key index entry needs 24 bytes. Reject a corrupt count
    // before using it as a potentially enormous allocation capacity.
    if block_count as u64 > reader.limit() / 24 {
        return Err(invalid_data("SSTable block count exceeds index size"));
    }
    let mut blocks = Vec::with_capacity(block_count);
    for _ in 0..block_count {
        let first_key = read_index_key(&mut reader)?;
        let start = read_u64(&mut reader)?;
        let end = read_u64(&mut reader)?;
        let records = read_u32(&mut reader)? as usize;
        blocks.push(BlockIndex {
            first_key,
            start,
            end,
            records,
        });
    }
    if reader.limit() != 0 {
        return Err(invalid_data("unexpected bytes at end of SSTable index"));
    }

    validate_index(&blocks, record_count, index_offset)?;
    Ok((blocks, bloom, record_count))
}

fn validate_index(blocks: &[BlockIndex], record_count: usize, index_offset: u64) -> io::Result<()> {
    if blocks.is_empty() {
        if record_count != 0 || index_offset != FILE_MAGIC.len() as u64 {
            return Err(invalid_data("empty SSTable has an inconsistent index"));
        }
        return Ok(());
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
            write_chunk(w, key)?;
            write_chunk(w, v)?;
        }
        Value::Tombstone => {
            w.write_all(&[FLAG_TOMBSTONE])?;
            write_chunk(w, key)?;
        }
    }
    Ok(())
}

fn write_chunk<W: Write>(w: &mut W, bytes: &[u8]) -> io::Result<()> {
    let len = u32::try_from(bytes.len()).map_err(|_| invalid_input("record field is too large"))?;
    w.write_all(&len.to_be_bytes())?;
    w.write_all(bytes)
}

/// Reads one record. A torn record is corruption because SSTables are atomic.
fn read_record<R: Read>(r: &mut R) -> io::Result<Option<(Vec<u8>, Value)>> {
    let mut flag = [0u8; 1];
    match r.read_exact(&mut flag) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }

    match flag[0] {
        FLAG_SET => {
            let key = read_chunk(r)?;
            let value = read_chunk(r)?;
            Ok(Some((key, Value::Set(value))))
        }
        FLAG_TOMBSTONE => {
            let key = read_chunk(r)?;
            Ok(Some((key, Value::Tombstone)))
        }
        other => Err(invalid_data(format!(
            "unknown SSTable record flag: {other}"
        ))),
    }
}

fn read_index_key<R: Read>(r: &mut std::io::Take<R>) -> io::Result<Vec<u8>> {
    let len = read_u32(r)? as u64;
    if len > r.limit() {
        return Err(invalid_data("SSTable index key exceeds index bounds"));
    }
    let mut key = vec![0u8; len as usize];
    r.read_exact(&mut key)?;
    Ok(key)
}

fn read_chunk<R: Read>(r: &mut R) -> io::Result<Vec<u8>> {
    let len = read_u32(r)? as usize;
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
    fn bloom_filter_has_no_false_negatives_and_skips_a_data_block_on_miss() {
        let path = tmp("bloom");
        let entries: Vec<(Vec<u8>, Value)> = (0..20)
            .map(|i| {
                (
                    format!("key-{i:04}").into_bytes(),
                    Value::Set(format!("value-{i}").into_bytes()),
                )
            })
            .collect();
        SsTable::write(
            &path,
            entries.iter().map(|(key, value)| (key.as_slice(), value)),
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
        let entries: Vec<(Vec<u8>, Value)> = (0..(RECORDS_PER_BLOCK * 2 + 5))
            .map(|i| {
                (
                    format!("key-{i:04}").into_bytes(),
                    Value::Set(format!("value-{i}").into_bytes()),
                )
            })
            .collect();
        SsTable::write(&path, entries.iter().map(|(k, v)| (k.as_slice(), v))).unwrap();

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
    fn rejects_unsorted_entries() {
        let path = tmp("unsorted");
        let entries = [
            (b"b".as_slice(), Value::Set(b"2".to_vec())),
            (b"a".as_slice(), Value::Set(b"1".to_vec())),
        ];
        let error = SsTable::write(&path, entries.iter().map(|(k, v)| (*k, v))).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        std::fs::remove_file(tmp_path(&path)).ok();
    }
}
