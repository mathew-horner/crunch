use std::path::PathBuf;
use std::thread;

use crate::memtable::{Memtable, MemtableArgs};
use crate::store::{Store, StoreArgs};

pub struct Engine {
    memtable: Memtable,
    store: Store,
}

#[derive(Default)]
pub struct EngineArgs {
    pub memtable: MemtableArgs,
    pub store: StoreArgs,
}

impl Engine {
    pub fn new(path: PathBuf, args: EngineArgs) -> Self {
        let memtable = Memtable::new(args.memtable);
        let store = Store::new(path, args.store);
        log::debug!("engine initialized");
        Self { memtable, store }
    }

    /// Set `key` to `value`.
    ///
    /// This operation is fast in LSM storage engines because the data is only
    /// written to the append-only WAL and stored in the memtable at write time.
    /// Data is flushed to segment files *asynchronously*.
    pub fn set(&mut self, key: &str, value: &str) {
        // TODO: This is not durable, we need to write updates to a WAL.
        self.memtable.set(key, value);
        if self.memtable.full() {
            self.flush_memtable();
        }
    }

    /// Get the value for `key`, if any.
    pub fn get(&mut self, key: &str) -> Option<String> {
        if let Some(value) = self.memtable.get(key) {
            return Some(value);
        }
        self.store.get(key)
    }

    /// Delete the `key`.
    pub fn delete(&mut self, key: &str) {
        self.memtable.delete(key);
    }

    /// Gracefully shutdown the storage engine.
    pub fn stop(self) -> thread::Result<()> {
        self.store.stop()
    }

    /// Clear and write the contents of the memtable to disk.
    fn flush_memtable(&mut self) {
        log::debug!("memtable has hit capacity ({}), flushing to disk", self.memtable.capacity());
        self.store.write_memtable(&self.memtable);
        self.memtable.reset();
    }
}
