use std::fs::{create_dir, remove_dir_all, File};
use std::path::{Path, PathBuf};

use crate::segment::segment_filename;

pub struct StoreFixture {
    path: PathBuf,
    segment_file_count: usize,
}

impl StoreFixture {
    pub fn init(path: impl AsRef<Path>) -> Self {
        _ = remove_dir_all(path.as_ref());
        create_dir(path.as_ref()).unwrap();
        Self { path: PathBuf::from(path.as_ref()), segment_file_count: 0 }
    }

    /// Create a new segment file with the given `pairs` as its data.
    ///
    /// This function will sort the pairs in ascending lexicographical order by
    /// key before it writes them.
    pub fn create_segment_file(
        &mut self,
        pairs: impl IntoIterator<Item = (&'static str, &'static str)>,
    ) -> File {
        let path = self.allocate_segment_file();
        let mut file = File::create_new(path).unwrap();
        let mut pairs: Vec<_> = pairs.into_iter().collect();
        pairs.sort_by_key(|pair| pair.0);
        pairs
            .into_iter()
            .for_each(|(key, value)| crate::segment::write(&mut file, key, value).unwrap());
        file
    }

    /// Allocate an ID for a new file in the store and return its path.
    pub fn allocate_segment_file(&mut self) -> PathBuf {
        let id = self.segment_file_count + 1;
        self.segment_file_count += 1;
        self.path.join(segment_filename(id as u32))
    }
}

impl Drop for StoreFixture {
    fn drop(&mut self) {
        remove_dir_all(&self.path).unwrap();
    }
}
