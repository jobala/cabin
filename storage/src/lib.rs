use std::{ops::Bound, rc::Rc};

use crate::{
    common::errors::KeyNotFound,
    memtable::{iterator::MemtableIterator, table::Memtable},
};
use anyhow::Result;
use bytes::Bytes;

mod common;
mod memtable;

pub struct Storage {
    memtable: Rc<Memtable>,
    frozen_memtables: Vec<Rc<Memtable>>,
}

pub struct Config {}

pub fn new(config: &Config) -> Storage {
    Storage {
        memtable: Rc::new(Memtable::new(0)),
        frozen_memtables: vec![],
    }
}

impl Storage {
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let _ = self.memtable.put(key, value);
        self.frozen_memtables.insert(0, self.memtable.clone());

        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Result<Bytes, KeyNotFound> {
        self.memtable.get(key).ok_or(KeyNotFound)
    }

    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> MemtableIterator {
        !unimplemented!()
    }
}
