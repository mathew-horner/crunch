use std::fs::File;
use std::io::prelude::*;
use std::io::{self, SeekFrom};
use std::path::PathBuf;

use bloom::BloomFilter;

use crate::error::{PairComponent, WriteError};
use crate::sparse_index::SparseIndex;

// TODO: These should probably be configurable at the Database level.
const BLOOM_FILTER_FALSE_POSITIVE_RATE: f32 = 0.0001;
const SPARSE_INDEX_RANGE_SIZE: usize = 4;

type Value = Option<String>;

pub struct Segment {
    pub file: File,
    pub path: PathBuf,
    bloom_filter: BloomFilter,
    sparse_index: SparseIndex,
}

impl Segment {
    pub fn open(path: PathBuf) -> Self {
        let file = File::open(&path).unwrap();
        Self::new(file, path)
    }

    pub fn new(mut file: File, path: PathBuf) -> Self {
        let (bloom_filter, sparse_index) = create_data_structures_for_segment(&mut file, &path);
        Self { file, path, bloom_filter, sparse_index }
    }

    /// Read the value for `key` from this file, if any.
    pub fn get(&mut self, key: &str) -> Option<Value> {
        log::trace!("looking in {:?} for {key}", self.path);

        // Each lookup in the bloom filter has a chance of being a false positive, but
        // every negative is correct. So we can exit early if the membership test
        // returns false.
        if !self.bloom_filter.contains(&key) {
            log::trace!("{key} was not in bloom filter for {:?}", self.path);
            return None;
        }

        let (start, end) = self.sparse_index.get_byte_range(key);
        let start = start.unwrap_or(0);
        self.file.seek(SeekFrom::Start(start)).unwrap();
        log::trace!("byte range constrained to {start}..{end:?}");

        let mut elapsed_bytes = start;
        for entry in EntryIter::new(&mut self.file) {
            if end.is_some() && elapsed_bytes >= end.unwrap() {
                break;
            }
            match entry {
                Entry::Assignment { key: k, value } if k == key => {
                    log::trace!("found {key} in {:?}", self.path);
                    return Some(Some(value));
                },
                Entry::Tombstone { key: k } if k == key => {
                    log::trace!("found tombstone for {key} in {:?}", self.path);
                    return Some(None);
                },
                _ => {},
            };
            elapsed_bytes += entry.stride() as u64;
        }

        log::trace!("{key} was not in {:?}", self.path);
        None
    }

    /// Print details about the inner state of the segment file and its
    /// associated in-memory resources.
    pub fn inspect(&self) {
        println!("Sparse Index");
        self.sparse_index.inner().iter().for_each(|(key, offset)| println!("{key} @ {offset}"));
    }
}

fn create_data_structures_for_segment(
    file: &mut File,
    path: &PathBuf,
) -> (BloomFilter, SparseIndex) {
    let size = EntryIter::from_start(file).count() as u32;
    log::trace!("size of {path:?}: {size}");
    let mut bloom_filter = BloomFilter::with_rate(BLOOM_FILTER_FALSE_POSITIVE_RATE, size);
    let mut sparse_index = SparseIndex::new();
    let mut elapsed_bytes = 0;

    for (idx, entry) in EntryIter::from_start(file).enumerate() {
        bloom_filter.insert(entry.key());
        if idx % SPARSE_INDEX_RANGE_SIZE == 0 {
            sparse_index.insert(entry.key(), elapsed_bytes);
        }
        elapsed_bytes += entry.stride() as u64;
    }

    (bloom_filter, sparse_index)
}

/// Iterates over the entries in a segment file.
pub struct EntryIter<'a> {
    file: &'a mut File,
}

impl<'a> EntryIter<'a> {
    pub fn new(file: &'a mut File) -> Self {
        Self { file }
    }

    /// Seeks to the start of the file before iteration.
    pub fn from_start(file: &'a mut File) -> Self {
        file.seek(SeekFrom::Start(0)).unwrap();
        Self::new(file)
    }
}

impl Iterator for EntryIter<'_> {
    type Item = Entry;

    fn next(&mut self) -> Option<Self::Item> {
        let mut indicator = [0; 1];
        match self.file.read_exact(&mut indicator) {
            Ok(_) => {},
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return None,
            error => error.unwrap(),
        };

        match EntryIndicator::from_u8_opt(indicator[0]) {
            Some(EntryIndicator::Assignment) => {
                let mut size_bytes = [0; 4];
                self.file.read_exact(&mut size_bytes).unwrap();
                let size = u32::from_be_bytes(size_bytes);
                let mut key_buffer = vec![0; size as usize];
                self.file.read_exact(&mut key_buffer).unwrap();

                let mut size_bytes = [0; 4];
                self.file.read_exact(&mut size_bytes).unwrap();
                let size = u32::from_be_bytes(size_bytes);
                let mut value_buffer = vec![0; size as usize];
                self.file.read_exact(&mut value_buffer).unwrap();

                let key = std::str::from_utf8(&key_buffer).unwrap();
                let value = std::str::from_utf8(&value_buffer).unwrap();
                Some(Entry::Assignment { key: key.to_owned(), value: value.to_owned() })
            },
            Some(EntryIndicator::Tombstone) => {
                let mut size_bytes = [0; 4];
                self.file.read_exact(&mut size_bytes).unwrap();
                let size = u32::from_be_bytes(size_bytes);
                let mut key_buffer = vec![0; size as usize];
                self.file.read_exact(&mut key_buffer).unwrap();
                let key = std::str::from_utf8(&key_buffer).unwrap();
                Some(Entry::Tombstone { key: key.to_owned() })
            },
            None => {
                let position = self.file.seek(SeekFrom::Current(0)).unwrap();
                log::warn!("failed to parse indicator {} @ {position}", indicator[0]);
                None
            },
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum Entry {
    /// A key-value assignment.
    Assignment { key: String, value: String },
    /// A marker that a key is now deleted.
    Tombstone { key: String },
}

impl Entry {
    /// An entry has a key whether it is an assignment or tombstone, this is a
    /// helper method to extract that without having to pattern match at the
    /// call site.
    pub fn key(&self) -> &String {
        match self {
            Self::Assignment { key, .. } => &key,
            Self::Tombstone { key } => &key,
        }
    }

    pub fn write(&self, file: &mut File) -> Result<(), WriteError> {
        match self {
            Self::Assignment { key, value } => write(file, key, value),
            Self::Tombstone { key } => tombstone(file, key),
        }
    }

    // TODO: Should this be usize?
    fn stride(&self) -> usize {
        match self {
            Self::Assignment { key, value } => {
                key.as_bytes().len() + value.as_bytes().len() + 8 + 1
            },
            Self::Tombstone { key } => key.as_bytes().len() + 4 + 1,
        }
    }
}

#[repr(u8)]
enum EntryIndicator {
    /// The data represents a key-value assignment.
    Assignment = 0,
    /// The data represents a key deletion.
    Tombstone,
}

impl EntryIndicator {
    /// Return the [`Indicator`] variant for a `u8`, if any.
    fn from_u8_opt(num: u8) -> Option<Self> {
        match num {
            0 => Some(Self::Assignment),
            1 => Some(Self::Tombstone),
            _ => None,
        }
    }
}

/// Write a key-value pair to a data file in the custom binary format.
pub fn write(file: &mut File, key: &str, value: &str) -> Result<(), WriteError> {
    let key_bytes = key.as_bytes();
    let value_bytes = value.as_bytes();

    // Add 8 bytes here for the two u32 length prefixes.
    // TODO: Is it wise to pre-allocate this if our key or value might be too long?
    // We should do that check earlier...
    let mut bytes = Vec::with_capacity(key_bytes.len() + value_bytes.len() + 8 + 1);
    bytes.extend([EntryIndicator::Assignment as u8]);

    for (component_bytes, component) in
        [(key_bytes, PairComponent::Key), (value_bytes, PairComponent::Value)]
    {
        let size = component_bytes.len();
        let size = u32::try_from(size)
            .map_err(|_| WriteError::TooLarge(component, size, u32::max_value() as usize))?;
        bytes.extend(size.to_be_bytes());
        bytes.extend(component_bytes);
    }

    file.write_all(&bytes)?;
    Ok(())
}

/// Write a deletion marker (tombstone) to a data file in the custom binary
/// format.
pub fn tombstone(file: &mut File, key: &str) -> Result<(), WriteError> {
    let key_bytes = key.as_bytes();
    let size = key_bytes.len();
    let size = u32::try_from(size)
        .map_err(|_| WriteError::TooLarge(PairComponent::Key, size, u32::max_value() as usize))?;

    let mut bytes = Vec::with_capacity(size as usize + 4 + 1);
    bytes.extend([EntryIndicator::Tombstone as u8]);
    bytes.extend(size.to_be_bytes());
    bytes.extend(key_bytes);

    file.write_all(&bytes)?;
    Ok(())
}
