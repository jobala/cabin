use std::{
    collections::HashMap,
    fs,
    ops::Bound,
    sync::{
        Arc, Mutex, MutexGuard, RwLock,
        atomic::{AtomicUsize, Ordering::SeqCst},
    },
};

use crate::{
    FileObject, SSTable, SSTableBuilder, SSTableIterator,
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
    sst_id: AtomicUsize,
    config: Config,
    db_dir: String,
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
    pub block_size: usize,
}

pub fn new(config: Config) -> Storage {
    let db_dir = ".cabin";
    let block_cache = Arc::new(BlockCache::new(1 << 20)); // 4gb

    let (l0_sstables, sstables) = load_sstables(".cabin/sst", block_cache);

    Storage {
        config,
        db_dir: db_dir.to_string(),
        sst_id: AtomicUsize::new(0),
        state_lock: Mutex::new(()),
        block_cache: Arc::new(BlockCache::new(1 << 20)), // 4gb cache
        state: RwLock::new(Arc::new(StorageState {
            memtable: Arc::new(Memtable::new(0)),
            frozen_memtables: Vec::new(),
            l0_sstables,
            sstables,
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
        let state = {
            let guard = self.state.read().unwrap();
            guard.clone()
        };

        let mut res = state.memtable.get(key);

        // search in frozen memtables
        if res.is_none() {
            for frozen_table in state.frozen_memtables.clone() {
                if frozen_table.get(key).is_some() {
                    res = frozen_table.get(key);
                    break;
                }
            }
        }

        // search in ssts
        if res.is_none() {
            let mut table_iters = Vec::with_capacity(state.l0_sstables.len());
            for table_id in state.l0_sstables.iter() {
                let table = state.sstables[table_id].clone();
                if key < table.first_key() || key > table.last_key() {
                    continue;
                }

                let iter = SSTableIterator::create_and_seek_to_key(table, key).unwrap();
                table_iters.push(iter);
            }

            let merged_iter = MergedIterator::new(table_iters);
            if !merged_iter.key().is_empty() && merged_iter.key() == key {
                res = Some(Bytes::copy_from_slice(merged_iter.value()))
            } else {
                res = None;
            }
        }

        match res {
            Some(value) => Ok(value),
            None => Err(KeyNotFound),
        }
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
                memtable: Arc::new(Memtable::new(self.next_sst_id())),
                frozen_memtables,
                l0_sstables: guard.l0_sstables.clone(),
                sstables: guard.sstables.clone(),
            });

            drop(guard);
        }
    }

    fn flush_frozen_memtable(&self) -> Result<()> {
        let mut sst_builder = SSTableBuilder::new(self.config.block_size);
        let mut guard = self.state.write().unwrap();

        let mut memtables = guard.frozen_memtables.clone();
        let mut l0_sstables = guard.l0_sstables.clone();
        let mut sstables = guard.sstables.clone();

        let Some(memtable) = memtables.pop() else {
            return Ok(());
        };
        memtable.flush(&mut sst_builder)?;

        let sst = sst_builder.build(
            memtable.id,
            self.block_cache.clone(),
            self.sst_path(memtable.id),
        )?;
        l0_sstables.push(memtable.id);
        sstables.insert(memtable.id, Arc::new(sst));

        *guard = Arc::new(StorageState {
            memtable: guard.memtable.clone(),
            frozen_memtables: memtables,
            l0_sstables,
            sstables,
        });

        Ok(())
    }

    fn next_sst_id(&self) -> usize {
        self.sst_id.fetch_add(1, SeqCst)
    }

    fn sst_path(&self, id: usize) -> String {
        format!("{}/sst/{}.sst", self.db_dir, id)
    }
}

fn load_sstables(
    path: &str,
    block_cache: Arc<BlockCache>,
) -> (Vec<usize>, HashMap<usize, Arc<SSTable>>) {
    let mut l0_sstables = vec![];
    let mut sstables = HashMap::new();

    for file_path in fs::read_dir(path).unwrap() {
        let sst_path = file_path.unwrap().path();
        let file = FileObject::open(sst_path.as_path()).expect("failed to open file");
        let sst = SSTable::open(0, block_cache.clone(), file).expect("failed to open sstable");

        l0_sstables.push(sst.id);
        sstables.insert(0, Arc::new(sst));
    }

    (l0_sstables, sstables)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filled_up_memtables_are_frozen() {
        let config = Config {
            sst_size: 4,
            block_size: 4,
        };
        let storage = new(config);

        let input = vec![b"1", b"2", b"3", b"4", b"5"];
        for entry in input {
            storage.put(entry, entry).unwrap();
        }

        assert_eq!(2, storage.state.read().unwrap().frozen_memtables.len());
        assert_eq!(2, storage.state.read().unwrap().memtable.get_size());
    }

    #[test]
    fn test_flushing_frozen_memtable() {}

    #[test]
    fn test_loading_sstables() {}

    #[test]
    fn test_scanning_storage_with_empty_memtables_and_filled_sstables() {}

    #[test]
    fn test_scanning_filled_memtables_and_sstables() {}
}
