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

pub struct Segment {
    pub file: File,
    pub path: PathBuf,
    bloom_filter: BloomFilter,
    sparse_index: SparseIndex,
}

impl Segment {
    pub fn new(mut file: File, path: PathBuf) -> Self {
        let (bloom_filter, sparse_index) = create_data_structures_for_segment(&mut file, &path);
        Self { file, path, bloom_filter, sparse_index }
    }

    /// Read the value for `key` from this file, if any.
    pub fn get(&mut self, key: &str) -> Option<String> {
        log::trace!("looking in {:?} for {key}", self.path);

        // Each lookup in the bloom filter has a chance of being a false positive, but
        // every negative is correct. So we can exit early if the membership test
        // returns false.
        if !self.bloom_filter.contains(&key) {
            log::trace!("{key} was not in bloom filter for {:?}", self.path);
            return None;
        }

        let range = self.sparse_index.get_byte_range(key);
        let start = range.start.unwrap_or(0);
        self.file.seek(SeekFrom::Start(start)).unwrap();
        log::trace!("byte range constrained to {range:?}");

        let mut elapsed_bytes = start;
        for (k, v) in PairIter::new(&mut self.file) {
            if range.end.is_some() && elapsed_bytes >= range.end.unwrap() {
                break;
            }
            if k == key {
                log::trace!("found {key} in {:?}", self.path);
                return Some(v);
            }
            elapsed_bytes += (k.as_bytes().len() + v.as_bytes().len() + 8) as u64;
        }

        log::trace!("{key} was not in {:?}", self.path);
        None
    }

    /// Read the entirety of the file to a string.
    ///
    /// This method is used for automated tests.
    #[cfg(test)]
    pub fn read_to_string(&mut self) -> String {
        self.file.seek(SeekFrom::Start(0)).unwrap();
        let mut buffer = String::new();
        self.file.read_to_string(&mut buffer).unwrap();
        self.file.seek(SeekFrom::Start(0)).unwrap();
        buffer
    }

    /// Print details about the inner state of the segment file and its
    /// associated in-memory resources.
    pub fn inspect(&self) {
        println!("Sparse Index");
        for (key, offset) in self.sparse_index.inner() {
            println!("{key} @ {offset}");
        }
    }
}

fn create_data_structures_for_segment(
    file: &mut File,
    path: &PathBuf,
) -> (BloomFilter, SparseIndex) {
    let size = PairIter::from_start(file).count() as u32;
    log::trace!("size of {path:?}: {size}");
    let mut bloom_filter = BloomFilter::with_rate(BLOOM_FILTER_FALSE_POSITIVE_RATE, size);
    let mut sparse_index = SparseIndex::new();
    let mut elapsed_bytes = 0;

    for (idx, (key, value)) in PairIter::from_start(file).enumerate() {
        bloom_filter.insert(&key);
        if idx % SPARSE_INDEX_RANGE_SIZE == 0 {
            sparse_index.insert(&key, elapsed_bytes);
        }
        elapsed_bytes += (key.as_bytes().len() + value.as_bytes().len() + 8) as u64;
    }

    (bloom_filter, sparse_index)
}

/// Iterates over the key-value pairs in a segment file.
pub struct PairIter<'a> {
    file: &'a mut File,
}

impl<'a> PairIter<'a> {
    pub fn new(file: &'a mut File) -> Self {
        Self { file }
    }

    /// Seeks to the start of the file before iteration.
    pub fn from_start(file: &'a mut File) -> Self {
        file.seek(SeekFrom::Start(0)).unwrap();
        Self::new(file)
    }
}

impl Iterator for PairIter<'_> {
    type Item = (String, String);

    fn next(&mut self) -> Option<Self::Item> {
        let mut size_bytes = [0; 4];
        match self.file.read_exact(&mut size_bytes) {
            Ok(_) => {},
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return None,
            error => error.unwrap(),
        };
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
        Some((key.to_owned(), value.to_owned()))
    }
}

/// Write a key-value pair to a data file in the custom binary format.
pub fn write(file: &mut File, key: &str, value: &str) -> Result<(), WriteError> {
    let key_bytes = key.as_bytes();
    let value_bytes = value.as_bytes();

    // Add 8 bytes here for the two u32 length prefixes.
    let mut bytes = Vec::with_capacity(key_bytes.len() + value_bytes.len() + 8);

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
