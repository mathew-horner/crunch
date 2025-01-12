use std::collections::{btree_map, BTreeMap};

use crunch_common::env::parse_env;

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
    pub fn from_env() -> Self {
        let capacity = parse_env("engine", Some("memtable"), "capacity", 1024);
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

    pub fn set(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.tree.insert(key.into(), Some(value.into()));
    }

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

    pub fn delete(&mut self, key: &str) {
        self.tree.insert(key.into(), None);
    }

    pub fn full(&self) -> bool {
        self.tree.len() >= self.capacity
    }

    pub fn iter(&self) -> btree_map::Iter<String, Value> {
        self.tree.iter()
    }

    pub fn reset(&mut self) {
        self.tree = BTreeMap::new();
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }
}
