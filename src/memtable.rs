use rbtree::{Iter, RBTree};

pub struct Memtable {
    tree: RBTree<String, String>,
    capacity: usize,
}

pub struct MemtableArgs {
    pub capacity: usize,
}

impl Default for MemtableArgs {
    fn default() -> Self {
        Self { capacity: 1024 }
    }
}

impl Memtable {
    pub fn new(MemtableArgs { capacity }: MemtableArgs) -> Self {
        Self { tree: RBTree::new(), capacity }
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
}
