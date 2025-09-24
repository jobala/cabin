use std::{
    ops::Bound,
    sync::{Arc, Mutex, MutexGuard, RwLock},
};

use crate::{
    common::errors::KeyNotFound,
    memtable::{iterator::MemtableIterator, table::Memtable},
};
use anyhow::Result;
use bytes::Bytes;

mod common;
mod memtable;

pub fn new(config: Config) -> Storage {
    Storage {
        config,
        state_lock: Mutex::new(()),
        state: RwLock::new(StorageState {
            memtable: Arc::new(Memtable::new(0)),
            frozen_memtables: Vec::new(),
        }),
    }
}

impl Storage {
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let guard = self.state.read().unwrap();
        let memtable = guard.memtable.clone();

        memtable.put(key, value)?;
        self.try_freeze();
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
        let guard = self.state.read().unwrap();
        let memtable = guard.memtable.clone();

        if memtable.get_size() > self.config.sst_size {
            let lock = self.state_lock.lock().unwrap();
            self.freeze(lock);
        }
    }

    fn freeze(&self, _state_lock: MutexGuard<()>) {
        let mut guard = self.state.write().unwrap();
        let memtable = guard.memtable.clone();

        // check again, another thread might have frozen the memtable already.
        if memtable.get_size() > self.config.sst_size {
            guard.frozen_memtables.insert(0, memtable);
            guard.memtable = Arc::new(Memtable::new(0));
        }
    }

    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> MemtableIterator {
        !unimplemented!()
    }
}

#[derive(Debug)]
pub struct Storage {
    state: RwLock<StorageState>,
    state_lock: Mutex<()>,
    config: Config,
}

#[derive(Debug)]
struct StorageState {
    memtable: Arc<Memtable>,
    frozen_memtables: Vec<Arc<Memtable>>,
}

#[derive(Debug)]
pub struct Config {
    sst_size: usize,
}
