use std::fs::File;
use std::io::prelude::*;
use std::io::{BufReader, SeekFrom};
use std::path::PathBuf;

use bloom::BloomFilter;

use crate::sparse_index::SparseIndex;
use crate::util::Assignment;

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
        let (bloom_filter, sparse_index) = create_data_structures_for_segment(&mut file);
        Self { file, path, bloom_filter, sparse_index }
    }

    /// Read the value for `key` from this file, if any.
    pub fn get(&mut self, key: &str) -> Option<String> {
        if !self.bloom_filter.contains(&key) {
            return None;
        }
        let range = self.sparse_index.get_byte_range(&key.into());
        let start = range.start.unwrap_or(0);
        self.file.seek(SeekFrom::Start(start)).unwrap();
        let mut elapsed_bytes = start;

        for line in BufReader::new(&self.file).lines() {
            if range.end.is_some() && elapsed_bytes >= range.end.unwrap() {
                break;
            }
            if let Ok(line) = line {
                if let Ok(Assignment { key: k, value: v }) = Assignment::parse(&line) {
                    if k == key {
                        return Some(v.to_owned());
                    }
                }
                elapsed_bytes += line.as_bytes().len() as u64 + 1;
            }
        }
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
}

fn create_data_structures_for_segment(file: &mut File) -> (BloomFilter, SparseIndex) {
    file.seek(SeekFrom::Start(0)).unwrap();
    let line_count = BufReader::new(&*file).lines().count();
    let mut bloom_filter =
        BloomFilter::with_rate(BLOOM_FILTER_FALSE_POSITIVE_RATE, line_count as u32);
    let mut sparse_index = SparseIndex::new();
    let mut elapsed_bytes = 0;
    file.seek(SeekFrom::Start(0)).unwrap();

    for (idx, line) in BufReader::new(&*file).lines().enumerate() {
        if let Ok(line) = line {
            if let Ok(Assignment { key, .. }) = Assignment::parse(&line) {
                bloom_filter.insert(&key);
                if idx % SPARSE_INDEX_RANGE_SIZE == 0 {
                    sparse_index.insert(&key, elapsed_bytes);
                }
            }
            elapsed_bytes += line.as_bytes().len() as u64 + 1;
        }
    }
    (bloom_filter, sparse_index)
}
