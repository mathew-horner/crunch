use std::collections::VecDeque;
use std::fs::{create_dir_all, remove_file, File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::thread::{self, JoinHandle};

use crunch_common::env::parse_env;

use crate::compaction::compaction_loop;
use crate::error::Error;
use crate::memtable::Memtable;
use crate::segment::{
    self, is_segment_filename, segment_filename, segment_id, Entry, EntryIter, SegmentHandle,
};

/// Handles disk I/O for the database engine.
pub struct Store {
    directory: PathBuf,
    segments: Arc<RwLock<VecDeque<PathBuf>>>,
    wal: File,

    /// Set to `true` to kill the compaction loop.
    compaction_kill_flag: Arc<AtomicBool>,

    /// Wait on this after `compaction_kill_flag` is set to cleanly shut down
    /// the compaction loop.
    compaction_join_handle: Option<JoinHandle<()>>,
}

#[derive(Debug)]
pub struct StoreArgs {
    /// When this is enabled, a background thread known as the "compaction loop"
    /// runs and intermittently (on a period defined by
    /// `compaction_interval_seconds`) compacts segment files together.
    pub compaction_enabled: bool,

    pub compaction_interval_seconds: u64,
}

impl StoreArgs {
    /// Parse arguments from environment variables prefixed with
    /// `CRUNCH_ENGINE_STORE`.
    pub fn from_env() -> Self {
        let compaction_enabled = parse_env("engine", Some("store"), "compaction_enabled", true);
        let compaction_interval_seconds =
            parse_env("engine", Some("store"), "compaction_interval_seconds", 600);
        Self { compaction_enabled, compaction_interval_seconds }
    }
}

impl Default for StoreArgs {
    fn default() -> Self {
        Self { compaction_enabled: true, compaction_interval_seconds: 600 }
    }
}

impl Store {
    pub fn new(directory: PathBuf, args: StoreArgs) -> Result<Self, Error> {
        let segments = initialize_store_at_path(&directory)?;
        let wal = open_wal(&directory)?;
        let mut store = Self {
            directory,
            segments: Arc::new(RwLock::new(segments)),
            wal,
            compaction_kill_flag: Arc::new(AtomicBool::new(false)),
            compaction_join_handle: None,
        };
        if args.compaction_enabled {
            store.compaction_join_handle = Some({
                let path = store.directory.clone();
                let segments = store.segments.clone();
                let compaction_kill_flag = store.compaction_kill_flag.clone();
                std::thread::spawn(move || {
                    compaction_loop(
                        args.compaction_interval_seconds,
                        path,
                        segments,
                        compaction_kill_flag,
                    )
                })
            });
        }
        log::debug!("store initialized with {args:?}");
        Ok(store)
    }

    /// Write a `key`:`value` pair to the WAL.
    pub fn set(&mut self, key: &str, value: &str) -> Result<(), Error> {
        segment::write(&mut self.wal, key, value)
    }

    /// Read `key`'s value from disk, if it exists.
    pub fn get(&self, key: &str) -> Result<Option<String>, Error> {
        let segments = self.segments.read()?;
        for segment in segments.iter().rev() {
            let mut segment = SegmentHandle::open(segment.to_owned())?;
            match segment.get(key)? {
                Some(value) => return Ok(value),
                _ => {},
            };
        }
        Ok(None)
    }

    /// Write a tombstone for `key` to disk.
    pub fn delete(&mut self, key: &str) -> Result<(), Error> {
        segment::tombstone(&mut self.wal, key)
    }

    /// Cleanly shut down the compaction loop, if it is running.
    pub fn stop(self) -> thread::Result<()> {
        self.compaction_kill_flag.swap(true, Ordering::Relaxed);
        if let Some(handle) = self.compaction_join_handle {
            handle.join()?;
        }
        Ok(())
    }

    /// Write the contents of the `memtable` to a new segment file on disk.
    pub fn write_memtable(&mut self, memtable: &Memtable) -> Result<(), Error> {
        // The id of the new segment file will be the highest one on disk + 1.
        let last_segment_id = self.segments.read()?.iter().last().and_then(segment_id).unwrap_or(0);
        let next_segment_id = last_segment_id + 1;

        let next_segment_path = self.directory.clone().join(segment_filename(next_segment_id));
        let mut next_segment = File::create(next_segment_path.clone())?;
        for (key, value) in memtable.iter() {
            match value {
                Some(value) => segment::write(&mut next_segment, key, value)?,
                None => segment::tombstone(&mut next_segment, key)?,
            }
        }
        log::debug!("wrote memtable to {next_segment_path:?}");
        self.segments.write()?.push_back(next_segment_path);

        // Delete and recreate the WAL, which means that if the engine crashes after the
        // deletion and before the re-creation, there will be no WAL on disk. Since the
        // engine expects that it may have to recreate the WAL, and our engine is only
        // single threaded (outside of compaction, which only touches segment files),
        // this is fine.
        remove_file(wal_path(&self.directory))?;
        self.wal = open_wal(&self.directory)?;
        Ok(())
    }

    /// Seed the `memtable` with the contents of the WAL.
    pub fn replay_wal(&mut self, memtable: &mut Memtable) -> Result<(), Error> {
        Ok(EntryIter::from_start(&mut self.wal)?.for_each(|entry| {
            match entry {
                Entry::Assignment { key, value } => memtable.set(key, value),
                Entry::Tombstone { key } => memtable.delete(&key),
            };
        }))
    }

    pub fn list_segments(&self) -> Result<Vec<PathBuf>, Error> {
        Ok(self.segments.read()?.iter().cloned().collect())
    }

    pub fn inspect_segment(&self, filename: &str) -> Result<(), Error> {
        let path = self.directory.join(filename);
        let guard = self.segments.read()?;
        let Some(segment) = guard.iter().find(|segment| **segment == path) else {
            println!("Error: segment not found");
            return Ok(());
        };
        _ = SegmentHandle::open(segment.to_owned())
            .inspect_err(|error| println!("Error: could not open segment, reason: {error:?}"))
            .inspect(|segment| segment.inspect());
        Ok(())
    }
}

/// Creates a store directory at the given `path` if one does not already exist.
///
/// If one does, it returns the existing segment files to seed the [`Store`].
fn initialize_store_at_path(path: &PathBuf) -> Result<VecDeque<PathBuf>, io::Error> {
    if !path.exists() {
        log::info!("no store detected at {path:?}, creating directory");
        create_dir_all(path)?;
        Ok(VecDeque::new())
    } else {
        log::info!("existing store detected at {path:?}");
        Ok(std::fs::read_dir(path)?
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let filetype = entry.file_type().ok()?;
                let filename = entry.file_name();
                let filename = filename.to_str()?;
                (filetype.is_file() && is_segment_filename(filename)).then_some(entry)
            })
            .map(|entry| PathBuf::from(entry.path()))
            .collect())
    }
}

fn wal_path(store_path: &Path) -> PathBuf {
    store_path.join("wal.dat")
}

fn open_wal(store_path: &Path) -> Result<File, io::Error> {
    let path = wal_path(store_path);
    OpenOptions::new().create(true).append(true).read(true).open(&path)
}
