use std::{
    ops::Bound,
    sync::{atomic::Ordering::Relaxed, Arc, RwLock},
};

use crate::{
    common::errors::KeyNotFound,
    memtable::{iterator::MemtableIterator, table::Memtable},
};
use anyhow::Result;
use bytes::Bytes;

mod common;
mod memtable;
pub fn new(config: &Config) -> Storage {
    Storage {
        capacity: config.memtable_capacity,
        state: RwLock::new(StorageState {
            memtable: Arc::new(Memtable::new(0)),
            frozen_memtables: Vec::new(),
        }),
    }
}

impl Storage {
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.try_freeze();

        let guard = self.state.read().unwrap();
        let memtable = guard.memtable.clone();

        let _ = memtable.put(key, value);

        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Result<Bytes, KeyNotFound> {
        let guard = self.state.read().unwrap();
        let memtable = guard.memtable.clone();

        if let Some(value) = memtable.get(key) {
            Ok(value)
        } else {
            for table in guard.frozen_memtables.clone() {
                let value = table.get(key);
                if value.is_some() {
                    return Ok(value.unwrap());
                }
            }

            Err(KeyNotFound)
        }
    }

    fn try_freeze(&self) {
        let mut guard = self.state.write().unwrap();
        let memtable = guard.memtable.clone();

        if memtable.size.load(Relaxed) > self.capacity {
            guard.frozen_memtables.insert(0, memtable);
            guard.memtable = Arc::new(Memtable::new(0))
        }
    }

    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> MemtableIterator {
        !unimplemented!()
    }
}

#[derive(Debug)]
pub struct Storage {
    state: RwLock<StorageState>,
    capacity: usize,
}

#[derive(Debug)]
struct StorageState {
    memtable: Arc<Memtable>,
    frozen_memtables: Vec<Arc<Memtable>>,
}

pub struct Config {
    memtable_capacity: usize,
}
