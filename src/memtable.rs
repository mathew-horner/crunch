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
        Self {
            tree: RBTree::new(),
            capacity,
        }
    }

    pub fn set(&mut self, key: &str, value: &str) {
        self.tree.replace_or_insert(key.into(), value.into());
    }

    pub fn get(&self, key: &str) -> Option<String> {
        match self.tree.get(&key.into()) {
            Some(value) => Some(value.into()),
            None => None,
        }
    }

    pub fn delete(&mut self, key: &str) {
        self.tree.remove(&key.into());
    }

    pub fn full(&self) -> bool {
        self.tree.len() >= self.capacity
    }

    pub fn iter(&self) -> Iter<String, String> {
        self.tree.iter()
    }

    pub fn reset(&mut self) {
        self.tree = RBTree::new();
    }
}
