use std::collections::VecDeque;
use std::fs::{create_dir_all, remove_file, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::thread::{self, JoinHandle};

use walkdir::WalkDir;

use crate::compaction::compaction_loop;
use crate::env::parse_env;
use crate::memtable::Memtable;
use crate::segment::{self, segment_file_number, Entry, EntryIter, SegmentHandle};

pub struct Store {
    path: PathBuf,
    segments: Arc<RwLock<VecDeque<PathBuf>>>,
    wal: File,

    /// Flipping this flag to `true` will kill the compactor.
    compaction_kill_flag: Arc<AtomicBool>,

    /// This handle can be used to wait for the compactor to gracefully exit,
    /// which is triggered with the `compaction_kill_flag`
    compaction_join_handle: Option<JoinHandle<()>>,
}

#[derive(Debug)]
pub struct StoreArgs {
    pub compaction_enabled: bool,
    pub compaction_interval_seconds: u64,
}

impl StoreArgs {
    /// Get configuration values from store config environment variables.
    pub fn from_env() -> Self {
        let compaction_enabled = parse_env("store", "compaction_enabled", true);
        let compaction_interval_seconds = parse_env("store", "compaction_interval_seconds", 600);
        Self { compaction_enabled, compaction_interval_seconds }
    }
}

impl Default for StoreArgs {
    fn default() -> Self {
        Self { compaction_enabled: true, compaction_interval_seconds: 600 }
    }
}

impl Store {
    /// Initialize a store which will persist its data to the given directory.
    pub fn new(path: PathBuf, args: StoreArgs) -> Self {
        let segments = initialize_store_at_path(&path);
        let wal = open_wal(&path);
        let mut store = Self {
            path,
            segments: Arc::new(RwLock::new(segments)),
            wal,
            compaction_kill_flag: Arc::new(AtomicBool::new(false)),
            compaction_join_handle: None,
        };
        if args.compaction_enabled {
            store.compaction_join_handle = Some({
                let path = store.path.clone();
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
        store
    }

    /// Append an assignment to the WAL.
    pub fn set(&mut self, key: &str, value: &str) {
        segment::write(&mut self.wal, key, value).unwrap();
    }

    /// Read the value for `key` from disk, if any.
    pub fn get(&mut self, key: &str) -> Option<String> {
        let segments = self.segments.read().unwrap();
        for segment in segments.iter().rev() {
            let mut segment = SegmentHandle::open(segment.to_owned());
            match segment.get(key) {
                Some(value) => return value,
                _ => {},
            };
        }
        None
    }

    /// Append a tombstone to the WAL to indicate that a key should be deleted.
    pub fn delete(&mut self, key: &str) {
        segment::tombstone(&mut self.wal, key).unwrap();
    }

    /// Gracefully shutdown the store.
    pub fn stop(self) -> thread::Result<()> {
        self.compaction_kill_flag.swap(true, Ordering::Relaxed);
        if let Some(handle) = self.compaction_join_handle {
            handle.join()?;
        }
        Ok(())
    }

    /// Write the contents of the `memtable` to a new segment file on disk.
    pub fn write_memtable(&mut self, memtable: &Memtable) {
        // The id of the new segment file will be the highest one on disk + 1.
        let last_segment_id =
            self.segments.read().unwrap().iter().last().and_then(segment_file_number).unwrap_or(0);
        let path = self.path.clone().join(format!("segment-{}.dat", last_segment_id + 1));

        let mut file = File::create(path.clone()).unwrap();
        for (key, value) in memtable.iter() {
            match value {
                Some(value) => segment::write(&mut file, key, value).unwrap(),
                None => segment::tombstone(&mut file, key).unwrap(),
            }
        }
        log::debug!("wrote memtable to {path:?}");
        self.segments.write().unwrap().push_back(path);

        // Delete and recreate the WAL, which means that if the engine crashes after the
        // deletion and before the re-creation, there will be no WAL on disk. Since the
        // engine expects that it may have to recreate the WAL, and our engine is only
        // single threaded (outside of compaction, which only touches segment files),
        // this is fine.
        remove_file(wal_path(&self.path)).unwrap();
        self.wal = open_wal(&self.path);
    }

    /// Replay the WAL and seed the `memtable`.
    pub fn replay_wal(&mut self, memtable: &mut Memtable) {
        EntryIter::from_start(&mut self.wal).for_each(|entry| {
            match entry {
                Entry::Assignment { key, value } => memtable.set(key, value),
                Entry::Tombstone { key } => memtable.delete(&key),
            };
        })
    }

    /// Print details about the inner state of the segment file, if it exists.
    pub fn inspect_segment(&self, filename: &str) {
        let path = self.path.join(filename);
        let guard = self.segments.read().unwrap();
        let Some(segment) = guard.iter().find(|segment| **segment == path) else {
            println!("Error: segment not found");
            return;
        };
        SegmentHandle::open(segment.to_owned()).inspect();
    }
}

/// Creates a store directory at the given `path` if one does not already exist.
///
/// If one does, it returns the existing segment files to seed the [`Store`].
fn initialize_store_at_path(path: &PathBuf) -> VecDeque<PathBuf> {
    let mut files = VecDeque::new();
    if !path.exists() {
        log::info!("no store detected at {path:?}, creating directory");
        create_dir_all(path).unwrap();
    } else {
        log::info!("existing store detected at {path:?}");
        // TODO: We don't want to recursively walk the directory, what were you thinking
        // 2022 me?
        for entry in WalkDir::new(path).follow_links(false).into_iter().filter_map(Result::ok) {
            let filename = entry.file_name().to_string_lossy();
            // TODO: This is not a great way to detect / filter out non-segment files.
            if filename.starts_with("segment") {
                files.push_back(PathBuf::from(entry.path()));
            }
        }
    }
    files
}

/// Return the path to the WAL file in the given store.
fn wal_path(store_path: &Path) -> PathBuf {
    store_path.join("wal.dat")
}

/// Open or create the WAL file in the given store.
fn open_wal(store_path: &Path) -> File {
    let path = wal_path(store_path);
    OpenOptions::new().create(true).append(true).open(&path).unwrap()
}
