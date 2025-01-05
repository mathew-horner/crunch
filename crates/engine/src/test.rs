use std::fs::{create_dir, remove_dir_all, File};
use std::io::Write;
use std::path::{Path, PathBuf};

/// Manages disk resources such as segment files in automated tests.
pub struct StoreFixture {
    path: PathBuf,
    segment_files: usize,
}

impl StoreFixture {
    /// Create a new directory at `path` for a test run.
    pub fn init(path: impl AsRef<Path>) -> Self {
        _ = remove_dir_all(path.as_ref());
        create_dir(path.as_ref()).unwrap();
        Self { path: PathBuf::from(path.as_ref()), segment_files: 0 }
    }

    /// Create a new segment file on disk with the given file `contents`.
    pub fn create_segment_file(&mut self, contents: &str) -> File {
        let path = self.allocate_segment_file();
        let mut file = File::create_new(path).unwrap();
        file.write_all(contents.as_bytes()).unwrap();
        file
    }

    /// Allocate an ID for a new segment file in the store and return its
    /// assigned path.
    pub fn allocate_segment_file(&mut self) -> PathBuf {
        let id = self.segment_files + 1;
        self.segment_files += 1;
        self.path.join(format!("segment-{id}"))
    }
}

impl Drop for StoreFixture {
    fn drop(&mut self) {
        remove_dir_all(&self.path).unwrap();
    }
}
