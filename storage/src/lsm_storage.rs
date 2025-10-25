use std::{
    collections::HashMap,
    ops::Bound,
    sync::{Arc, Mutex, MutexGuard, RwLock},
};

use crate::{
    SSTable, SSTableIterator,
    common::{errors::KeyNotFound, iterator::StorageIterator},
    iterators::{
        lsm_iterator::LsmIterator, merged_iterator::MergedIterator,
        two_merge_iterator::TwoMergeIterator,
    },
    memtable::{memtable_iterator::map_bound, table::Memtable},
    sst::BlockCache,
};
use anyhow::Result;
use bytes::Bytes;

#[derive(Debug)]
pub struct Storage {
    state: RwLock<Arc<StorageState>>,
    block_cache: Arc<BlockCache>,
    state_lock: Mutex<()>,
    config: Config,
}

#[derive(Debug)]
struct StorageState {
    memtable: Arc<Memtable>,
    frozen_memtables: Vec<Arc<Memtable>>,
    l0_sstables: Vec<usize>,
    sstables: HashMap<usize, Arc<SSTable>>,
}

#[derive(Debug)]
pub struct Config {
    pub sst_size: usize,
}

pub fn new(config: Config) -> Storage {
    Storage {
        config,
        state_lock: Mutex::new(()),
        block_cache: Arc::new(BlockCache::new(1 << 20)), // 4gb cache
        state: RwLock::new(Arc::new(StorageState {
            memtable: Arc::new(Memtable::default()),
            frozen_memtables: Vec::new(),
            l0_sstables: vec![],
            sstables: HashMap::new(),
        })),
    }
}

impl Storage {
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let size;

        {
            let guard = self.state.read().unwrap();
            guard.memtable.put(key, value)?;
            size = guard.memtable.get_size();
        }

        self.try_freeze(size);
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Result<Bytes, KeyNotFound> {
        let guard = self.state.read().unwrap();

        let Some(value) = guard.memtable.get(key) else {
            for frozen_table in guard.frozen_memtables.clone() {
                if let Some(value) = frozen_table.get(key) {
                    return Ok(value);
                }
            }

            return Err(KeyNotFound);
        };

        Ok(value)
    }

    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<LsmIterator> {
        let state = {
            let guard = self.state.read().unwrap();
            guard.clone()
        };

        let mut iters = vec![];

        // insert memtables from newest to oldest
        iters.push(state.memtable.scan(lower, upper));

        let frozen_tables = &state.frozen_memtables;
        for frozen_table in frozen_tables {
            iters.push(frozen_table.scan(lower, upper));
        }
        let mem_iters = MergedIterator::new(iters);

        let mut table_iters = Vec::with_capacity(state.l0_sstables.len());
        // TODO: only consider sstables that might contain the key
        for table_id in state.l0_sstables.iter() {
            let table = state.sstables[table_id].clone();
            let iter = match lower {
                Bound::Included(key) => SSTableIterator::create_and_seek_to_key(table, key)?,
                Bound::Unbounded => SSTableIterator::create_and_seek_to_first(table)?,
                Bound::Excluded(key) => {
                    let mut iter = SSTableIterator::create_and_seek_to_key(table, key)?;

                    if iter.is_valid() && iter.key() == key {
                        iter.next()?;
                    }
                    iter
                }
            };

            table_iters.push(iter);
        }

        let sst_iters = MergedIterator::new(table_iters);
        let inner = TwoMergeIterator::create(mem_iters, sst_iters).unwrap();
        Ok(LsmIterator::new(inner, map_bound(upper)))
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
            let mut frozen_memtables = guard.frozen_memtables.clone();
            frozen_memtables.insert(0, memtable);

            *guard = Arc::new(StorageState {
                memtable: Arc::new(Memtable::default()),
                frozen_memtables,
                l0_sstables: guard.l0_sstables.clone(),
                sstables: guard.sstables.clone(),
            });

            drop(guard);
        }
    }
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
