use std::collections::BTreeMap;
use std::ops::Range;

/// The sparse index keeps track of certain keys and their offset within segment
/// files, to enable faster lookups.
pub struct SparseIndex {
    index: BTreeMap<String, u64>,
}

impl SparseIndex {
    pub fn new() -> Self {
        Self { index: BTreeMap::new() }
    }

    pub fn get_byte_range(&self, key: &str) -> Range<Option<u64>> {
        // TODO: Can't we binary search here?
        // TODO: Also, can't we return None if there is no way the key is in this file?
        let mut iter = self.index.iter().peekable();
        let mut start = 0;
        let mut end = None;
        while iter.peek().is_some() {
            let curr = iter.next().unwrap();
            let next = iter.peek();
            start = *curr.1;
            end = match next {
                Some(pair) => Some(*pair.1),
                None => None,
            };
            if key >= curr.0 && next.is_some() && key < next.unwrap().0 {
                break;
            }
        }
        Some(start)..end
    }

    /// Index a `key` with the given `offset`.
    pub fn insert(&mut self, key: &str, offset: u64) {
        self.index.insert(key.into(), offset);
    }

    pub fn inner(&self) -> &BTreeMap<String, u64> {
        &self.index
    }
}
