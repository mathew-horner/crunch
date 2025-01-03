use rbtree::{Iter, RBTree};

use crate::env::parse_env;

pub struct Memtable {
    tree: RBTree<String, String>,
    capacity: usize,
}

#[derive(Debug)]
pub struct MemtableArgs {
    pub capacity: usize,
}

impl MemtableArgs {
    /// Get configuration values from the memtable config environment variables.
    pub fn from_env() -> Self {
        let capacity = parse_env("memtable", "capacity", 1024);
        Self { capacity }
    }
}

impl Default for MemtableArgs {
    fn default() -> Self {
        Self { capacity: 1024 }
    }
}

impl Memtable {
    pub fn new(args: MemtableArgs) -> Self {
        let tree = RBTree::new();
        log::debug!("memtable initialized with {args:?}");
        Self { tree, capacity: args.capacity }
    }

    /// Set `key` to `value` in memory.
    pub fn set(&mut self, key: &str, value: &str) {
        self.tree.replace_or_insert(key.into(), value.into());
    }

    /// Get the value for `key` in memory, if any.
    pub fn get(&self, key: &str) -> Option<String> {
        // TODO: Don't re-allocate the key here.
        self.tree.get(&key.into()).map(ToOwned::to_owned)
    }

    /// Delete the `key` from memory.
    pub fn delete(&mut self, key: &str) {
        self.tree.remove(&key.into());
    }

    /// Return whether the memtable has reached its configured `capacity`.
    pub fn full(&self) -> bool {
        self.tree.len() >= self.capacity
    }

    /// Return an iterator over the key:value pairs in memory.
    pub fn iter(&self) -> Iter<String, String> {
        self.tree.iter()
    }

    /// Clear all data from memory.
    pub fn reset(&mut self) {
        self.tree = RBTree::new();
    }

    /// Return the number of keys the memtable should hold.
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}
