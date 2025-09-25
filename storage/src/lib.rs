use std::sync::{Arc, Mutex, MutexGuard, RwLock};

use crate::{common::errors::KeyNotFound, memtable::table::Memtable};
use anyhow::Result;
use bytes::Bytes;

mod common;
mod memtable;

pub fn new(config: Config) -> Storage {
    Storage {
        config,
        state_lock: Mutex::new(()),
        state: RwLock::new(StorageState {
            memtable: Arc::new(Memtable::new()),
            frozen_memtables: Vec::new(),
        }),
    }
}

impl Storage {
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let size;

        {
            let guard = self.state.read().unwrap();
            let memtable = guard.memtable.clone();
            memtable.put(key, value)?;
            size = memtable.get_size();
        }

        self.try_freeze(size);
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Result<Bytes, KeyNotFound> {
        let guard = self.state.read().unwrap();
        let memtable = guard.memtable.clone();

        let Some(value) = memtable.get(key) else {
            for frozen_table in guard.frozen_memtables.clone() {
                if let Some(value) = frozen_table.get(key) {
                    return Ok(value);
                }
            }

            return Err(KeyNotFound);
        };

        Ok(value)
    }

    fn try_freeze(&self, size: usize) {
        if size >= self.config.sst_size {
            let lock = self.state_lock.lock().unwrap();
            self.freeze(&lock);
        }
    }

    fn freeze(&self, _state_lock: &MutexGuard<()>) {
        let mut guard = self.state.write().unwrap();
        let memtable = guard.memtable.clone();

        // check again, another thread might have frozen the memtable already.
        if memtable.get_size() >= self.config.sst_size {
            guard.frozen_memtables.insert(0, memtable);
            guard.memtable = Arc::new(Memtable::new());
        }
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
    pub sst_size: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filled_up_memtables_are_frozen() {
        let config = Config { sst_size: 4 };
        let storage = new(config);

        let input = vec![b"1", b"2", b"3", b"4", b"5"];
        for entry in input {
            storage.put(entry, entry).unwrap();
        }

        assert_eq!(2, storage.state.read().unwrap().frozen_memtables.len());
        assert_eq!(2, storage.state.read().unwrap().memtable.get_size());
    }
}
