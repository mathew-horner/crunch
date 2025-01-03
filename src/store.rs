use std::fs::{create_dir_all, File};
use std::io::prelude::*;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

use walkdir::WalkDir;

use crate::compaction::{compaction_loop, CompactionParams};
use crate::env::parse_env;
use crate::memtable::Memtable;
use crate::segment::Segment;

pub struct Store {
    path: PathBuf,
    segments: Arc<Mutex<Vec<Segment>>>,

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
    /// Initialize a store which will persist its data files at the given `path`
    /// directory.
    pub fn new(path: PathBuf, args: StoreArgs) -> Self {
        let segments = initialize_store_at_path(&path);
        let mut store = Self {
            path,
            segments: Arc::new(Mutex::new(segments)),
            compaction_kill_flag: Arc::new(AtomicBool::new(false)),
            compaction_join_handle: None,
        };
        if args.compaction_enabled {
            store.compaction_join_handle = Some(compaction_loop(CompactionParams {
                interval_seconds: args.compaction_interval_seconds,
                path: store.path.clone(),
                segments: store.segments.clone(),
                compaction_kill_flag: store.compaction_kill_flag.clone(),
            }));
        }
        log::debug!("store initialized with {args:?}");
        store
    }

    /// Read the value for `key` from disk, if any.
    pub fn get(&mut self, key: &str) -> Option<String> {
        let mut segments = self.segments.lock().unwrap();
        for segment in segments.iter_mut().rev() {
            if let Some(value) = segment.get(key) {
                return Some(value);
            }
        }
        None
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
        let mut files = self.segments.lock().unwrap();
        let path = self.path.clone().join(
            // TODO: This should be based on the segment file with the highest number + 1, not the
            // length. This is because we compact files now so segment_files.len()
            // won't always be equal to the highest numbered segment file.
            format!("segment-{}.dat", files.len() + 1),
        );
        let mut file = File::create(path.clone()).unwrap();
        for (key, value) in memtable.iter() {
            file.write_all(format!("{}={}\n", key, value).as_bytes()).unwrap();
        }
        log::debug!("wrote memtable to {path:?}");
        files.push(Segment::new(File::open(path.clone()).unwrap(), path));
    }
}

fn initialize_store_at_path(path: &PathBuf) -> Vec<Segment> {
    let mut files = Vec::new();
    if !path.exists() {
        log::info!("no store detected at {path:?}, creating directory");
        create_dir_all(path).unwrap();
    } else {
        log::info!("existing store detected at {path:?}");
        for entry in WalkDir::new(path).follow_links(false).into_iter().filter_map(Result::ok) {
            let filename = entry.file_name().to_string_lossy();
            // TODO: This is not a great way to detect / filter out non-segment files.
            if filename.starts_with("segment") {
                let file = File::open(entry.path()).unwrap();
                files.push(Segment::new(file, PathBuf::from(entry.path())));
            }
        }
    }
    files
}
