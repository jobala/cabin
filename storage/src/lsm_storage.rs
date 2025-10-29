use std::{
    collections::HashMap,
    ops::Bound,
    path::Path,
    sync::{
        Arc, Mutex, MutexGuard, RwLock,
        atomic::{AtomicUsize, Ordering::SeqCst},
    },
    thread,
};

use crate::{
    SSTable, SSTableIterator,
    common::{errors::KeyNotFound, iterator::StorageIterator},
    iterators::{
        lsm_iterator::LsmIterator, merged_iterator::MergedIterator,
        two_merge_iterator::TwoMergeIterator,
    },
    lsm_util::{create_db_dir, load_sstables},
    memtable::{memtable_iterator::map_bound, table::Memtable},
    sst::BlockCache,
};
use anyhow::Result;
use bytes::Bytes;

#[derive(Debug)]
pub struct Storage {
    pub(crate) state: RwLock<Arc<StorageState>>,
    pub(crate) config: Config,
    pub(crate) block_cache: Arc<BlockCache>,
    pub(crate) state_lock: Mutex<()>,
    pub(crate) sst_id: AtomicUsize,
    // pub(crate) flush_thread: Mutex<Option<thread::JoinHandle<()>>>,
    // pub(crate) flush_notifier: crossbeam::channel::Sender<()>,
}

#[derive(Debug)]
pub(crate) struct StorageState {
    pub(crate) memtable: Arc<Memtable>,
    pub(crate) frozen_memtables: Vec<Arc<Memtable>>,
    pub(crate) l0_sstables: Vec<usize>,
    pub(crate) sstables: HashMap<usize, Arc<SSTable>>,
}

#[derive(Debug)]
pub struct Config {
    pub sst_size: usize,
    pub block_size: usize,
    pub num_memtable_limit: usize,
    pub db_dir: String,
}

pub fn new(config: Config) -> Storage {
    let db_dir = Path::new(&config.db_dir);
    create_db_dir(db_dir);
    let block_cache = Arc::new(BlockCache::new(1 << 20)); // 4gb
    let (l0_sstables, sstables) = load_sstables(db_dir, block_cache).expect("loaded sstables");

    let sst_id = match l0_sstables.iter().max() {
        Some(id) => id + 1,
        None => 0,
    };

    Storage {
        config,
        sst_id: AtomicUsize::new(sst_id),
        state_lock: Mutex::new(()),
        block_cache: Arc::new(BlockCache::new(1 << 20)), // 4gb cache
        state: RwLock::new(Arc::new(StorageState {
            memtable: Arc::new(Memtable::new(sst_id)),
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

            self.inc_sst_id();
            let id = self.sst_id.load(SeqCst);

            *guard = Arc::new(StorageState {
                memtable: Arc::new(Memtable::new(id)),
                frozen_memtables,
                l0_sstables: guard.l0_sstables.clone(),
                sstables: guard.sstables.clone(),
            });

            drop(guard);
        }
    }

    fn inc_sst_id(&self) -> usize {
        self.sst_id.fetch_add(1, SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use std::str::from_utf8;

    use tempfile::tempdir;

    use crate::lsm_util::get_entries;

    use super::*;

    #[test]
    fn filled_up_memtables_are_frozen() {
        let config = Config {
            sst_size: 4,
            block_size: 32,
            db_dir: String::from(tempdir().unwrap().path().to_str().unwrap()),
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
    fn can_flush_frozen_memtable() {
        let config = Config {
            sst_size: 4,
            block_size: 32,
            db_dir: String::from(tempdir().unwrap().path().to_str().unwrap()),
        };
        let storage = new(config);

        let input = vec![b"1", b"2", b"3", b"4", b"5"];
        for entry in input {
            storage.put(entry, entry).unwrap();
        }

        assert_eq!(2, storage.state.read().unwrap().frozen_memtables.len());
        assert_eq!(2, storage.state.read().unwrap().memtable.get_size());

        storage
            .flush_frozen_memtable()
            .expect("memtable was frozen");
        assert_eq!(1, storage.state.read().unwrap().frozen_memtables.len());
    }

    #[test]
    fn loads_sstables() {
        let db_dir = String::from(tempdir().unwrap().path().to_str().unwrap());
        let config = Config {
            sst_size: 4,
            block_size: 32,
            db_dir: db_dir.clone(),
        };
        let storage = new(config);

        let input = vec![b"1", b"2", b"3", b"4", b"5"];
        for entry in input {
            storage.put(entry, entry).unwrap();
        }
        storage
            .flush_frozen_memtable()
            .expect("memtable to have been frozen");

        // new storage instance
        let config = Config {
            sst_size: 4,
            block_size: 32,
            db_dir: db_dir.clone(),
        };

        let storage = new(config);
        assert_eq!(1, storage.state.read().unwrap().l0_sstables.len());
        assert_eq!(0, storage.state.read().unwrap().l0_sstables[0]);
    }

    #[test]
    fn scans_storage_with_empty_memtables_and_filled_sstables() {
        let db_dir = String::from(tempdir().unwrap().path().to_str().unwrap());
        let config = Config {
            sst_size: 4,
            block_size: 32,
            db_dir: db_dir.clone(),
        };
        let storage = new(config);

        for (key, value) in get_entries() {
            storage.put(key, value).unwrap();
        }
        storage
            .flush_frozen_memtable()
            .expect("memtable to have been frozen");

        // new storage instance
        let config = Config {
            sst_size: 4,
            block_size: 32,
            db_dir: db_dir.clone(),
        };

        let storage = new(config);
        let mut iter = storage.scan(Bound::Unbounded, Bound::Unbounded).unwrap();
        let mut res = vec![];

        while iter.is_valid() {
            res.push(iter.key().to_vec());
            let _ = iter.next();
        }

        assert_eq!(res, vec![b"a", b"b"]);
    }

    #[test]
    fn scans_through_filled_memtables_and_sstables() {
        let db_dir = String::from(tempdir().unwrap().path().to_str().unwrap());
        let config = Config {
            sst_size: 4,
            block_size: 32,
            db_dir: db_dir.clone(),
        };
        let storage = new(config);

        for (key, value) in get_entries() {
            storage.put(key, value).unwrap();
        }

        // will create sstables with  a, b, c, d, e & f
        storage
            .flush_frozen_memtable()
            .expect("memtable to have been frozen");
        storage
            .flush_frozen_memtable()
            .expect("memtable to have been frozen");
        storage
            .flush_frozen_memtable()
            .expect("memtable to have been frozen");

        // new storage instance
        let config = Config {
            sst_size: 4,
            block_size: 32,
            db_dir: db_dir.clone(),
        };

        let new_entries = vec![(b"a", b"20"), (b"e", b"21"), (b"d", b"22"), (b"b", b"23")];
        let storage = new(config);
        for (key, value) in new_entries {
            let _ = storage.put(key, value);
        }

        let mut iter = storage.scan(Bound::Unbounded, Bound::Unbounded).unwrap();
        let mut keys = vec![];
        let mut values = vec![];

        while iter.is_valid() {
            let k = from_utf8(iter.key()).unwrap();
            let v = from_utf8(iter.value()).unwrap();

            keys.push(String::from(k));
            values.push(String::from(v));

            let _ = iter.next();
        }

        // expect the first frozen memtable to have id 3
        // we already have sstables with id 0, 1 & 2
        // we use .last here because frozen memtables are stored newest to oldest
        // with oldest being the first memtable to be frozen
        assert_eq!(
            3,
            storage
                .state
                .read()
                .unwrap()
                .frozen_memtables
                .last()
                .unwrap()
                .id
        );

        assert_eq!(keys, vec!["a", "b", "c", "d", "e", "f"]);
        assert_eq!(values, vec!["20", "23", "3", "22", "21", "6"]);
    }
}
