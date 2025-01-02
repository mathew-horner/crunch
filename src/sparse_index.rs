use std::collections::BTreeMap;
use std::ops::Range;

pub struct SparseIndex {
    index: BTreeMap<String, u64>,
}

impl SparseIndex {
    pub fn new() -> Self {
        Self { index: BTreeMap::new() }
    }

    pub fn get_byte_range(&self, key: &String) -> Range<Option<u64>> {
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

            if *key >= *curr.0 && next.is_some() && *key < *next.unwrap().0 {
                break;
            }
        }

        Some(start)..end
    }

    pub fn insert(&mut self, key: &str, offset: u64) {
        self.index.insert(key.into(), offset);
    }
}
