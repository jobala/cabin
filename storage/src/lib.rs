use std::{ops::Bound, rc::Rc};

use crate::memtable::{iterator::MemtableIterator, table::Memtable};
use anyhow::Result;
use bytes::Bytes;

mod memtable;

pub struct Storage {
    memtable: Memtable,
    frozen_memtables: Vec<Memtable>,
}

pub struct Config {}

pub fn new(config: &Config) -> Storage {
    Storage {
        memtable: Memtable::new(0),
        frozen_memtables: vec![],
    }
}

impl Storage {
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.memtable.put(key, value)
    }

    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.memtable.get(key)
    }
    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> MemtableIterator {
        !unimplemented!()
    }
}
