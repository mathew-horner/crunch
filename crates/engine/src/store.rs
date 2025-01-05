use std::fs::{create_dir_all, remove_file, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

use walkdir::WalkDir;

use crate::compaction::{compaction_loop, CompactionParams};
use crate::env::parse_env;
use crate::memtable::Memtable;
use crate::segment::{self, PairIter, Segment};

pub struct Store {
    path: PathBuf,
    segments: Arc<Mutex<Vec<Segment>>>,
    wal: Wal,

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
        let wal = Wal::new(path.clone());
        let mut store = Self {
            path,
            segments: Arc::new(Mutex::new(segments)),
            wal,
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
        segments.iter_mut().rev().find_map(|segment| segment.get(key))
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
            // TODO: Don't unwrap.
            segment::write(&mut file, key, value).unwrap();
        }
        log::debug!("wrote memtable to {path:?}");
        files.push(Segment::new(File::open(path.clone()).unwrap(), path));
        drop(files);
        self.wal.clear();
    }

    /// Append an assignment to the WAL.
    pub fn write_ahead(&mut self, key: &str, val: &str) {
        self.wal.write(key, val);
    }

    /// Replay the WAL and seed the memtable.
    ///
    /// The WAL is an important mechanism for crash recovery, and speedy writes.
    pub fn replay_wal(&self, memtable: &mut Memtable) {
        self.wal.replay(memtable);
    }

    /// Print details about the inner state of the segment file, if it exists.
    pub fn inspect_segment(&self, filename: &str) {
        let path = self.path.join(filename);
        let guard = self.segments.lock().unwrap();
        let Some(segment) = guard.iter().find(|segment| segment.path == path) else {
            println!("Error: segment not found");
            return;
        };
        segment.inspect();
    }
}

/// Creates a store directory at the given `path` if one does not already exist.
///
/// If one does, it returns the existing segment files to seed the [`Store`].
fn initialize_store_at_path(path: &PathBuf) -> Vec<Segment> {
    let mut files = Vec::new();
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
                let file = File::open(entry.path()).unwrap();
                files.push(Segment::new(file, PathBuf::from(entry.path())));
            }
        }
    }
    files
}

struct Wal {
    file: File,
    store_path: PathBuf,
}

impl Wal {
    fn new(store_path: PathBuf) -> Self {
        Self { file: open_wal(&store_path), store_path }
    }

    /// Clear the WAL; meant to be called during checkpoints.
    ///
    /// This function deletes and recreates the WAL, which means that if the
    /// engine crashes after the deletion and before the re-creation, there
    /// will be no WAL on disk. Since the engine expects that it may have to
    /// recreate the WAL, and our engine is only single threaded
    /// (outside of compaction, which only touches segment files), this is fine.
    fn clear(&mut self) {
        remove_file(self.path()).unwrap();
        self.file = open_wal(&self.store_path);
    }

    fn write(&mut self, key: &str, value: &str) {
        // TODO: Don't unwrap.
        segment::write(&mut self.file, key, value).unwrap();
    }

    fn replay(&self, memtable: &mut Memtable) {
        let mut file = File::open(self.path()).unwrap();
        PairIter::from_start(&mut file).for_each(|(key, value)| memtable.set(key, value))
    }

    fn path(&self) -> PathBuf {
        wal_path(&self.store_path)
    }
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
