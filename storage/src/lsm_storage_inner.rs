use anyhow::Result;
use bytes::Bytes;
use std::{
    collections::HashMap,
    ops::Bound,
    path::Path,
    sync::{
        Arc, Mutex, MutexGuard, RwLock,
        atomic::{AtomicUsize, Ordering::SeqCst},
    },
};

use crate::{
    SSTable, SSTableIterator,
    common::{errors::KeyNotFound, iterator::StorageIterator},
    iterators::{
        concat_iterator::ConcatIterator, lsm_iterator::LsmIterator,
        merged_iterator::MergedIterator, two_merge_iterator::TwoMergeIterator,
    },
    lsm_util::{create_db_dir, load_sstables},
    manifest::{Manifest, ManifestRecord},
    memtable::{memtable_iterator::map_bound, table::Memtable},
    sst::BlockCache,
};

#[derive(Debug)]
pub struct StorageInner {
    pub(crate) state: RwLock<Arc<StorageState>>,
    pub(crate) config: Config,
    pub(crate) block_cache: Arc<BlockCache>,
    pub(crate) state_lock: Mutex<()>,
    pub(crate) sst_id: AtomicUsize,
    pub(crate) manifest: Manifest,
}

#[derive(Debug)]
pub(crate) struct StorageState {
    pub(crate) memtable: Arc<Memtable>,
    pub(crate) frozen_memtables: Vec<Arc<Memtable>>,
    pub(crate) l0_sstables: Vec<usize>,
    pub(crate) sstables: HashMap<usize, Arc<SSTable>>,
    pub(crate) levels: Vec<(usize, Vec<usize>)>,
}

#[derive(Debug)]
pub struct Config {
    pub sst_size: usize,
    pub block_size: usize,
    pub num_memtable_limit: usize,
    pub db_dir: String,
    pub enable_wal: bool,
}

pub fn new(config: Config) -> Result<Arc<StorageInner>> {
    let state_lock = Mutex::new(());
    let block_cache = Arc::new(BlockCache::new(4096));
    let db_dir = Path::new(&config.db_dir);

    create_db_dir(db_dir);

    let manifest;
    let mut manifest_records: Vec<ManifestRecord> = vec![];
    let manifest_file = db_dir.join("manifest");
    match Manifest::recover(&manifest_file) {
        Ok((man, manifest_recs)) => {
            manifest = man;
            manifest_records = manifest_recs;
        }
        Err(_) => manifest = Manifest::create(manifest_file)?,
    }

    let (memtable_ids, l0_sst_ids, l1_sst_ids, sstables) =
        load_sstables(db_dir, block_cache, manifest_records)?;
    let sst_id = match ([&memtable_ids[..], &l0_sst_ids[..], &l1_sst_ids[..]].concat())
        .iter()
        .max()
    {
        Some(id) => id + 1,
        None => 0,
    };

    let (memtable, frozen_memtables) = match config.enable_wal {
        true => {
            let wal_path = db_dir.join(format!("{sst_id}.wal"));
            if memtable_ids.is_empty() {
                let memtable = Memtable::new_with_wal(sst_id, wal_path.as_path())?;
                manifest
                    .add_record(
                        &state_lock.lock().unwrap(),
                        ManifestRecord::NewMemtable(sst_id),
                    )
                    .expect("added manifest record");

                (Arc::new(memtable), vec![])
            } else {
                let mut memtables = vec![];
                for id in memtable_ids {
                    let wal_path = db_dir.join(format!("{id}.wal"));
                    let memtable = Memtable::new_with_wal(id, wal_path.as_path())?;
                    memtables.push(Arc::new(memtable));
                }

                (memtables.remove(0), memtables)
            }
        }
        _ => {
            manifest
                .add_record(
                    &state_lock.lock().unwrap(),
                    ManifestRecord::NewMemtable(sst_id),
                )
                .expect("added manifest record");

            (Arc::new(Memtable::new(sst_id)), vec![])
        }
    };

    Ok(Arc::new(StorageInner {
        config,
        state_lock,
        manifest,
        sst_id: AtomicUsize::new(sst_id),
        block_cache: Arc::new(BlockCache::new(1 << 20)), // 1mb cache
        state: RwLock::new(Arc::new(StorageState {
            sstables,
            frozen_memtables,
            memtable,
            l0_sstables: l0_sst_ids,
            levels: vec![(0, l1_sst_ids)],
        })),
    }))
}

impl StorageInner {
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let size;

        {
            let guard = self.state.read().unwrap();
            guard.memtable.put(key, value)?;
            size = guard.memtable.get_size();
        }

        self.try_freeze(size)?;
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

        // search in l0 ssts
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

        // search in l1 sstables
        if res.is_none() {
            let mut tables = Vec::with_capacity(state.levels[0].1.len());
            for table_id in state.levels[0].1.iter() {
                let table = state.sstables[table_id].clone();
                if key < table.first_key() || key > table.last_key() {
                    continue;
                }
                tables.push(table);
            }

            let concat_iter = ConcatIterator::create_and_seek_to_key(tables, key).unwrap();
            if !concat_iter.key().is_empty() && concat_iter.key() == key {
                res = Some(Bytes::copy_from_slice(concat_iter.value()))
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

        let concat_iter = {
            let mut tables = Vec::with_capacity(state.levels[0].1.len());
            for table_id in state.levels[0].1.iter() {
                let table = state.sstables[table_id].clone();
                tables.push(table);
            }
            match lower {
                Bound::Included(key) => ConcatIterator::create_and_seek_to_key(tables, key)?,
                Bound::Unbounded => ConcatIterator::create_and_seek_to_first(tables)?,
                Bound::Excluded(key) => {
                    let mut iter = ConcatIterator::create_and_seek_to_key(tables, key)?;

                    if iter.is_valid() && iter.key() == key {
                        iter.next()?;
                    }
                    iter
                }
            }
        };

        let sst_iters = MergedIterator::new(table_iters);
        let mem_l0 = TwoMergeIterator::create(mem_iters, sst_iters).unwrap();
        let mem_l0_l1 = TwoMergeIterator::create(mem_l0, concat_iter).unwrap();
        Ok(LsmIterator::new(mem_l0_l1, map_bound(upper)))
    }

    fn try_freeze(&self, size: usize) -> Result<()> {
        if size >= self.config.sst_size {
            let lock = self.state_lock.lock().unwrap();
            self.freeze(&lock)?;
        }

        Ok(())
    }

    fn freeze(&self, state_lock: &MutexGuard<()>) -> Result<()> {
        let mut guard = self.state.write().unwrap();
        let memtable = guard.memtable.clone();
        memtable.sync_wal()?;

        // check again, another thread might have frozen the memtable already.
        if memtable.get_size() >= self.config.sst_size {
            let mut frozen_memtables = guard.frozen_memtables.clone();
            frozen_memtables.insert(0, memtable);

            let id = self.get_sst_id();
            self.manifest
                .add_record(state_lock, ManifestRecord::NewMemtable(id))?;
            let memtable = self.create_memtable(id)?;

            *guard = Arc::new(StorageState {
                memtable: Arc::new(memtable),
                frozen_memtables,
                l0_sstables: guard.l0_sstables.clone(),
                sstables: guard.sstables.clone(),
                levels: guard.levels.clone(),
            });

            drop(guard);
        }

        Ok(())
    }

    fn create_memtable(&self, id: usize) -> Result<Memtable> {
        let wal_path = Path::new(&self.config.db_dir).join(format!("{id}.wal"));
        let memtable = match self.config.enable_wal {
            true => Memtable::new_with_wal(id, wal_path.as_path()).expect("memtable with wal"),
            _ => Memtable::new(id),
        };

        Ok(memtable)
    }

    pub(crate) fn get_sst_id(&self) -> usize {
        self.sst_id.fetch_add(1, SeqCst);
        self.sst_id.load(SeqCst)
    }

    pub fn sync(&self) -> Result<()> {
        self.state.read().unwrap().memtable.sync_wal()
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
            num_memtable_limit: 5,
            enable_wal: true,
        };
        let storage = new(config).unwrap();

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
            num_memtable_limit: 5,
            enable_wal: true,
        };
        let storage = new(config).unwrap();

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
            num_memtable_limit: 5,
            enable_wal: true,
        };
        let storage = new(config).unwrap();

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
            num_memtable_limit: 5,
            enable_wal: true,
        };

        let storage = new(config).unwrap();
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
            num_memtable_limit: 5,
            enable_wal: false,
        };
        let storage = new(config).unwrap();

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
            num_memtable_limit: 5,
            enable_wal: true,
        };

        let storage = new(config).unwrap();
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
            num_memtable_limit: 5,
            enable_wal: true,
        };
        let storage = new(config).unwrap();

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
            num_memtable_limit: 5,
            enable_wal: true,
        };

        let new_entries = vec![(b"a", b"20"), (b"e", b"21"), (b"d", b"22"), (b"b", b"23")];
        let storage = new(config).unwrap();
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

        assert_eq!(
            keys,
            vec!["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l"]
        );
        assert_eq!(
            values,
            vec![
                "20", "23", "3", "22", "21", "6", "7", "8", "9", "10", "11", "12"
            ]
        );
    }

    #[test]
    fn reads_the_latest_version_of_a_key() {
        let db_dir = String::from(tempdir().unwrap().path().to_str().unwrap());
        let config = Config {
            sst_size: 4,
            block_size: 32,
            db_dir: db_dir.clone(),
            num_memtable_limit: 5,
            enable_wal: true,
        };
        let storage = new(config).unwrap();

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
            num_memtable_limit: 5,
            enable_wal: false,
        };

        let new_entries = vec![(b"a", b"20"), (b"e", b"21"), (b"d", b"22"), (b"b", b"23")];
        let storage = new(config).unwrap();
        for (key, value) in new_entries {
            let _ = storage.put(key, value);
        }

        // this will create an sst with a & e
        storage
            .flush_frozen_memtable()
            .expect("memtable to have been frozen");

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

        assert_eq!(keys, vec!["a", "b", "c", "d", "e", "f"]);
        assert_eq!(values, vec!["20", "23", "3", "22", "21", "6"]);
    }

    #[test]
    fn get_key_within_range() {
        let db_dir = String::from(tempdir().unwrap().path().to_str().unwrap());
        let config = Config {
            sst_size: 4,
            block_size: 32,
            db_dir: db_dir.clone(),
            num_memtable_limit: 5,
            enable_wal: true,
        };
        let storage = new(config).unwrap();

        for (key, value) in get_entries() {
            storage.put(key, value).unwrap();
        }

        // will create sstables with  a, b, c, d, e & f
        storage
            .flush_frozen_memtable()
            .expect("memtable to have been frozen");
        storage.trigger_compaction().expect("compacted");

        storage
            .flush_frozen_memtable()
            .expect("memtable to have been frozen");
        storage
            .flush_frozen_memtable()
            .expect("memtable to have been frozen");

        let new_entries = vec![(b"a", b"20"), (b"e", b"21"), (b"d", b"22"), (b"b", b"23")];
        for (key, value) in new_entries {
            let _ = storage.put(key, value);
        }

        // this will create an sst with a & e
        storage
            .flush_frozen_memtable()
            .expect("memtable to have been frozen");

        let mut iter = storage
            .scan(Bound::Included(b"d"), Bound::Included(b"f"))
            .unwrap();
        let mut keys = vec![];
        let mut values = vec![];

        while iter.is_valid() {
            let k = from_utf8(iter.key()).unwrap();
            let v = from_utf8(iter.value()).unwrap();

            keys.push(String::from(k));
            values.push(String::from(v));

            let _ = iter.next();
        }

        assert_eq!(keys, vec!["d", "e", "f"]);
        assert_eq!(values, vec!["22", "21", "6"]);
    }

    #[test]
    fn test_manifest_recovery() {
        let db_dir = String::from(tempdir().unwrap().path().to_str().unwrap());
        let config = Config {
            sst_size: 4,
            block_size: 32,
            db_dir: db_dir.clone(),
            num_memtable_limit: 5,
            enable_wal: true,
        };
        let storage = new(config).unwrap();

        for (key, value) in get_entries() {
            storage.put(key, value).unwrap();
        }

        // will create sstables with  a, b, c, d, e & f
        storage
            .flush_frozen_memtable()
            .expect("memtable to have been frozen");
        storage.trigger_compaction().expect("compacted");

        storage
            .flush_frozen_memtable()
            .expect("memtable to have been frozen");
        storage.trigger_compaction().expect("compacted");

        storage
            .flush_frozen_memtable()
            .expect("memtable to have been frozen");
        storage
            .flush_frozen_memtable()
            .expect("memtable to have been frozen");

        let config = Config {
            sst_size: 4,
            block_size: 32,
            db_dir: db_dir.clone(),
            num_memtable_limit: 5,
            enable_wal: true,
        };
        let storage = new(config).unwrap();

        let state = {
            let guard = storage.state.read().unwrap();
            guard.clone()
        };

        assert_eq!(state.l0_sstables, [3, 2]);
        assert_eq!(state.levels[0].1, [8])
    }
}
