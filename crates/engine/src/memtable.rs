use std::collections::{btree_map, BTreeMap};

use crate::env::parse_env;

type Value = Option<String>;

pub struct Memtable {
    tree: BTreeMap<String, Value>,
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
        let tree = BTreeMap::new();
        log::debug!("memtable initialized with {args:?}");
        Self { tree, capacity: args.capacity }
    }

    /// Set `key` to `value` in memory.
    pub fn set(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.tree.insert(key.into(), Some(value.into()));
    }

    /// Get the value for `key` in memory, if any.
    pub fn get(&self, key: &str) -> Option<Value> {
        self.tree
            .get(key)
            .inspect(|value| {
                match value {
                    Some(_) => log::trace!("found {key} in memtable"),
                    None => log::trace!("found tombstone for {key} in memtable"),
                };
            })
            .map(ToOwned::to_owned)
    }

    /// Delete the `key` from memory.
    pub fn delete(&mut self, key: &str) {
        self.tree.insert(key.into(), None);
    }

    /// Return whether the memtable has reached its configured `capacity`.
    pub fn full(&self) -> bool {
        self.tree.len() >= self.capacity
    }

    /// Return an iterator over the key:value pairs in memory.
    pub fn iter(&self) -> btree_map::Iter<String, Value> {
        self.tree.iter()
    }

    /// Clear all data from memory.
    pub fn reset(&mut self) {
        self.tree = BTreeMap::new();
    }

    /// Return the number of keys the memtable should hold.
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}
