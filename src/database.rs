use std::path::PathBuf;
use std::thread;

use crate::memtable::{Memtable, MemtableArgs};
use crate::store::{Store, StoreArgs};

pub struct Database {
    memtable: Memtable,
    store: Store,
}

pub struct DatabaseArgs {
    pub memtable: MemtableArgs,
    pub store: StoreArgs,
}

impl Default for DatabaseArgs {
    fn default() -> Self {
        Self { memtable: Default::default(), store: Default::default() }
    }
}

impl Database {
    pub fn new(path: PathBuf, DatabaseArgs { memtable, store }: DatabaseArgs) -> Self {
        Self { memtable: Memtable::new(memtable), store: Store::new(path, store) }
    }

    pub fn set(&mut self, key: &str, value: &str) {
        self.memtable.set(key, value);
        if self.memtable.full() {
            self.flush_memtable();
        }
    }

    pub fn get(&mut self, key: &str) -> Option<String> {
        if let Some(value) = self.memtable.get(key) {
            return Some(value);
        }
        self.store.get(key)
    }

    pub fn delete(&mut self, key: &str) {
        self.memtable.delete(key);
    }

    pub fn stop(self) -> thread::Result<()> {
        self.store.stop()
    }

    fn flush_memtable(&mut self) {
        self.store.write_memtable(&self.memtable);
        self.memtable.reset();
    }
}
