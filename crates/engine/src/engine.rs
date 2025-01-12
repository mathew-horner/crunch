use std::path::PathBuf;
use std::thread;

use crate::error::Error;
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
    pub fn new(path: PathBuf, args: EngineArgs) -> Result<Self, Error> {
        let mut memtable = Memtable::new(args.memtable);
        let mut store = Store::new(path, args.store)?;
        store.replay_wal(&mut memtable)?;
        log::debug!("engine initialized");
        Ok(Self { memtable, store })
    }

    /// Set `key` to `value`.
    ///
    /// This operation is fast in LSM storage engines because the data is only
    /// written to the append-only WAL and stored in the memtable at write time.
    /// Data is flushed to segment files *asynchronously*.
    pub fn set(&mut self, key: &str, value: &str) -> Result<(), Error> {
        self.store.set(key, value)?;
        self.memtable.set(key, value);
        if self.memtable.full() {
            self.flush_memtable()?;
        }
        Ok(())
    }

    /// Get the value for `key`, if any.
    pub fn get(&mut self, key: &str) -> Result<Option<String>, Error> {
        if let Some(value) = self.memtable.get(key) {
            return Ok(value);
        }
        self.store.get(key)
    }

    /// Delete the `key`.
    pub fn delete(&mut self, key: &str) -> Result<(), Error> {
        self.store.delete(key)?;
        self.memtable.delete(key);
        Ok(())
    }

    /// Gracefully shutdown the storage engine.
    pub fn stop(self) -> thread::Result<()> {
        self.store.stop()
    }

    /// Clear and write the contents of the memtable to disk.
    fn flush_memtable(&mut self) -> Result<(), Error> {
        log::debug!("memtable has hit capacity ({}), flushing to disk", self.memtable.capacity());
        self.store.write_memtable(&self.memtable)?;
        self.memtable.reset();
        Ok(())
    }

    /// Return a reference to the underlying [`Store`].
    pub fn store(&self) -> &Store {
        &self.store
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::fs::remove_dir_all;
    use std::time::Duration;

    use rand::seq::SliceRandom;
    use rand::Rng;

    use super::*;

    #[test]
    fn sledgehammer() {
        const DIR: &str = "sledgehammer";

        _ = env_logger::try_init();
        let keys: Vec<_> = (0..26).map(|n| char::from_u32(n + 97).unwrap().to_string()).collect();
        let mut map = HashMap::new();

        _ = remove_dir_all(DIR);
        let mut engine = Engine::new(PathBuf::from(DIR), EngineArgs {
            memtable: MemtableArgs { capacity: 10 },
            store: StoreArgs { compaction_enabled: true, compaction_interval_seconds: 0 },
        })
        .unwrap();

        let mut deletes = 0;
        let mut inserts = 0;
        let mut reads = 0;

        for _ in 0..200 {
            match rand::thread_rng().gen_range(0..=2) {
                0 => {
                    // Random deletion
                    let mut rng = rand::thread_rng();
                    let key = keys.choose(&mut rng).unwrap();
                    map.remove(key);
                    engine.delete(key).unwrap();
                    deletes += 1;
                },
                1 => {
                    // Random insertion
                    let mut rng = rand::thread_rng();
                    let key = keys.choose(&mut rng).unwrap();
                    let value = rng.gen_range(0..1_000_000);
                    let value = value.to_string();
                    engine.set(key, &value).unwrap();
                    map.insert(key, value);
                    inserts += 1;
                },
                2 => {
                    // Random get and test equivalence to value in map
                    let mut rng = rand::thread_rng();
                    let key = keys.choose(&mut rng).unwrap();
                    let map_value = map.get(key);
                    let eng_value = engine.get(key).unwrap();
                    assert_eq!(map_value, eng_value.as_ref());
                    reads += 1;
                },
                _ => unreachable!(),
            }
            std::thread::sleep(Duration::from_millis(15));
        }

        log::info!("sledgehammer: deletes={deletes} inserts={inserts} reads={reads}");
        log::info!("sledgehammer: waiting for compactor to finish...");

        // Wait until the compactor has worked through all the segment files.
        while std::fs::read_dir(DIR)
            .unwrap()
            .filter(|entry| {
                // TODO: This should be abstracted into a function to not duplicate the
                // definition of what a segment file is.
                entry.as_ref().unwrap().file_name().to_str().unwrap().starts_with("segment")
            })
            .count()
            >= 2
        {}

        // One final assertion loop to ensure that the compactor worked properly.
        for (key, value) in map {
            assert_eq!(engine.get(key).unwrap().unwrap(), value);
        }

        remove_dir_all(DIR).unwrap();
    }
}
