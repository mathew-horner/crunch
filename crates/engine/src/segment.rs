use std::fs::File;
use std::io::prelude::*;
use std::io::{self, SeekFrom};
use std::path::{Path, PathBuf};

use anyhow::anyhow;
use bloom::BloomFilter;

use crate::error::{Error, PairComponent};
use crate::sparse_index::SparseIndex;

// TODO: These should probably be configurable at the Database level.
const BLOOM_FILTER_FALSE_POSITIVE_RATE: f32 = 0.0001;
const SPARSE_INDEX_RANGE_SIZE: usize = 4;

type Value = Option<String>;

pub struct SegmentHandle {
    file: File,
    path: PathBuf,
    bloom_filter: BloomFilter,
    sparse_index: SparseIndex,
}

impl SegmentHandle {
    pub fn open(path: PathBuf) -> Result<Self, io::Error> {
        let mut file = File::open(&path)?;
        let size = EntryIter::from_start(&mut file)?.count() as u32;
        log::trace!("size of {path:?}: {size}");
        let mut bloom_filter = BloomFilter::with_rate(BLOOM_FILTER_FALSE_POSITIVE_RATE, size);
        let mut sparse_index = SparseIndex::new();
        let mut elapsed_bytes = 0;

        for (idx, entry) in EntryIter::from_start(&mut file)?.enumerate() {
            bloom_filter.insert(entry.key());
            if idx % SPARSE_INDEX_RANGE_SIZE == 0 {
                sparse_index.insert(entry.key(), elapsed_bytes);
            }
            elapsed_bytes += entry.stride() as u64;
        }

        Ok(Self { file, path, bloom_filter, sparse_index })
    }

    pub fn get(&mut self, key: &str) -> Result<Option<Value>, io::Error> {
        log::trace!("looking in {:?} for {key}", self.path);

        // Each lookup in the bloom filter has a chance of being a false positive, but
        // every negative is correct. So we can exit early if the membership test
        // returns false.
        if !self.bloom_filter.contains(&key) {
            log::trace!("{key} was not in bloom filter for {:?}", self.path);
            return Ok(None);
        }

        let (byte_start, byte_end) = self.sparse_index.get_byte_range(key);
        let byte_start = byte_start.unwrap_or(0);
        self.file.seek(SeekFrom::Start(byte_start))?;
        log::trace!("byte range constrained to {byte_start}..{byte_end:?}");

        let mut elapsed_bytes = byte_start;
        for entry in EntryIter::new(&mut self.file) {
            if byte_end.is_some_and(|end| elapsed_bytes >= end) {
                break;
            }
            match entry {
                Entry::Assignment { key: k, value } if k == key => {
                    log::trace!("found {key} in {:?}", self.path);
                    return Ok(Some(Some(value)));
                },
                Entry::Tombstone { key: k } if k == key => {
                    log::trace!("found tombstone for {key} in {:?}", self.path);
                    return Ok(Some(None));
                },
                _ => {},
            };
            elapsed_bytes += entry.stride() as u64;
        }

        log::trace!("{key} was not in {:?}", self.path);
        Ok(None)
    }

    pub fn inspect(&self) {
        println!("Sparse Index");
        self.sparse_index.inner().iter().for_each(|(key, offset)| println!("{key} @ {offset}"));
    }
}

/// Iterator over the entries in a segment file.
pub struct EntryIter<'a> {
    file: &'a mut File,
}

impl<'a> EntryIter<'a> {
    pub fn new(file: &'a mut File) -> Self {
        Self { file }
    }

    /// Seek to the start of the file before iteration.
    pub fn from_start(file: &'a mut File) -> Result<Self, io::Error> {
        file.seek(SeekFrom::Start(0))?;
        Ok(Self::new(file))
    }

    fn step(&mut self) -> anyhow::Result<Option<Entry>> {
        let mut indicator_bytes = [0; 1];
        match self.file.read_exact(&mut indicator_bytes) {
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            error => error?,
        };

        match EntryIndicator::from_u8_opt(indicator_bytes[0]) {
            Some(EntryIndicator::Assignment) => {
                let mut size_bytes = [0; 4];
                self.file.read_exact(&mut size_bytes)?;
                let size = u32::from_be_bytes(size_bytes);
                let mut key_buffer = vec![0; size as usize];
                self.file.read_exact(&mut key_buffer)?;

                let mut size_bytes = [0; 4];
                self.file.read_exact(&mut size_bytes)?;
                let size = u32::from_be_bytes(size_bytes);
                let mut value_buffer = vec![0; size as usize];
                self.file.read_exact(&mut value_buffer)?;

                let key = std::str::from_utf8(&key_buffer)?;
                let value = std::str::from_utf8(&value_buffer)?;
                Ok(Some(Entry::Assignment { key: key.to_owned(), value: value.to_owned() }))
            },
            Some(EntryIndicator::Tombstone) => {
                let mut size_bytes = [0; 4];
                self.file.read_exact(&mut size_bytes)?;
                let size = u32::from_be_bytes(size_bytes);
                let mut key_buffer = vec![0; size as usize];
                self.file.read_exact(&mut key_buffer)?;
                let key = std::str::from_utf8(&key_buffer)?;
                Ok(Some(Entry::Tombstone { key: key.to_owned() }))
            },
            None => {
                let position = self.file.seek(SeekFrom::Current(0))?;
                Err(anyhow!("failed to parse indicator {} @ {position}", indicator_bytes[0]))
            },
        }
    }
}

impl Iterator for EntryIter<'_> {
    type Item = Entry;

    fn next(&mut self) -> Option<Self::Item> {
        self.step()
            .inspect_err(|error| {
                log::warn!("failed to step entry iter: {error}");
            })
            .ok()
            .flatten()
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum Entry {
    Assignment { key: String, value: String },
    Tombstone { key: String },
}

impl Entry {
    pub fn key(&self) -> &String {
        match self {
            Self::Assignment { key, .. } => &key,
            Self::Tombstone { key } => &key,
        }
    }

    pub fn write(&self, file: &mut File) -> Result<(), Error> {
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
    Assignment = 0,
    Tombstone,
}

impl EntryIndicator {
    fn from_u8_opt(num: u8) -> Option<Self> {
        match num {
            0 => Some(Self::Assignment),
            1 => Some(Self::Tombstone),
            _ => None,
        }
    }
}

pub fn write(file: &mut File, key: &str, value: &str) -> Result<(), Error> {
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
            .map_err(|_| Error::TooLarge(component, size, u32::max_value() as usize))?;
        bytes.extend(size.to_be_bytes());
        bytes.extend(component_bytes);
    }

    file.write_all(&bytes)?;
    Ok(())
}

pub fn tombstone(file: &mut File, key: &str) -> Result<(), Error> {
    let key_bytes = key.as_bytes();
    let size = key_bytes.len();
    let size = u32::try_from(size)
        .map_err(|_| Error::TooLarge(PairComponent::Key, size, u32::max_value() as usize))?;

    let mut bytes = Vec::with_capacity(size as usize + 4 + 1);
    bytes.extend([EntryIndicator::Tombstone as u8]);
    bytes.extend(size.to_be_bytes());
    bytes.extend(key_bytes);

    file.write_all(&bytes)?;
    Ok(())
}

pub fn segment_file_number(path: impl AsRef<Path>) -> Option<u32> {
    path.as_ref()
        .file_name()?
        .to_str()?
        .strip_prefix("segment-")?
        .strip_suffix(".dat")?
        .parse()
        .ok()
}

pub fn segment_filename(number: u32) -> String {
    format!("segment-{number}.dat")
}

pub fn is_segment_filename(filename: &str) -> bool {
    filename.starts_with("segment")
}
