use std::collections::BTreeMap;
use std::ops::Bound;

/// The sparse index keeps track of a subset of keys and their offsets within
/// segment files, to enable faster lookups.
pub struct SparseIndex {
    index: BTreeMap<String, u64>,
}

impl SparseIndex {
    pub fn new() -> Self {
        Self { index: BTreeMap::new() }
    }

    /// Return the byte range in which the key would exist in the segment file.
    ///
    /// NOTE: This function does not actually guarantee existence.
    pub fn get_byte_range(&self, key: &str) -> (Option<u64>, Option<u64>) {
        let start = self
            .index
            .range::<str, (Bound<&str>, Bound<&str>)>((Bound::Unbounded, Bound::Included(key)))
            .last()
            .map(|(_, offset)| *offset);
        let end = self
            .index
            .range::<str, (Bound<&str>, Bound<&str>)>((Bound::Excluded(key), Bound::Unbounded))
            .next()
            .map(|(_, offset)| *offset);
        (start, end)
    }

    pub fn insert(&mut self, key: &str, offset: u64) {
        self.index.insert(key.into(), offset);
    }

    pub fn inner(&self) -> &BTreeMap<String, u64> {
        &self.index
    }
}

#[cfg(test)]
mod test {
    use super::*;

    mod get_byte_range {
        use super::*;

        #[test]
        fn empty_index() {
            assert_eq!(SparseIndex::new().get_byte_range("a"), (None, None));
        }

        #[test]
        fn before_min_key() {
            let mut index = SparseIndex::new();
            index.insert("hello", 0);
            index.insert("world", 1);
            let range = index.get_byte_range("asdf");
            assert_eq!(range, (None, Some(0)));
        }

        #[test]
        fn between_keys() {
            let mut index = SparseIndex::new();
            index.insert("hello", 0);
            index.insert("world", 1);
            let range = index.get_byte_range("middle");
            assert_eq!(range, (Some(0), Some(1)));
        }

        #[test]
        fn equal_to_key() {
            let mut index = SparseIndex::new();
            index.insert("hello", 0);
            index.insert("thiskey", 1);
            index.insert("world", 2);
            let range = index.get_byte_range("thiskey");
            assert_eq!(range, (Some(1), Some(2)));
        }

        #[test]
        fn after_max_key() {
            let mut index = SparseIndex::new();
            index.insert("hello", 0);
            index.insert("world", 1);
            let range = index.get_byte_range("zebra");
            assert_eq!(range, (Some(1), None));
        }
    }
}
