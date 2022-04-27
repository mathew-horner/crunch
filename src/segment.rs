use std::{
    fs::File,
    io::{prelude::*, BufReader, SeekFrom},
    path::PathBuf,
};

use bloom::BloomFilter;

use crate::{
    sparse_index::SparseIndex,
    util::{parse_assignment, Assignment},
};

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
        let (bloom_filter, sparse_index) =
            create_data_structures_for_segment_file(&mut file);

        Self {
            file,
            path,
            bloom_filter,
            sparse_index,
        }
    }

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
                if let Ok(Assignment { key: k, value: v }) = parse_assignment(&line) {
                    if k == key {
                        return Some(v);
                    }
                }
                elapsed_bytes += line.as_bytes().len() as u64 + 1;
            }
        }

        None
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
    for (i, line) in BufReader::new(&*file).lines().enumerate() {
        if let Ok(line) = line {
            if let Ok(Assignment { key: k, .. }) = parse_assignment(&line) {
                bloom_filter.insert(&k);
                if i % SPARSE_INDEX_RANGE_SIZE == 0 {
                    sparse_index.insert(&k, elapsed_bytes);
                }
            }
            elapsed_bytes += line.as_bytes().len() as u64 + 1;
        }
    }

    (bloom_filter, sparse_index)
}
