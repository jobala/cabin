use std::{
    fs::remove_file,
    sync::Arc,
    thread::{self, JoinHandle},
    time::Duration,
};

use anyhow::{Ok, Result};

use crate::{
    SSTableBuilder, SSTableIterator, Storage, common::iterator::StorageIterator,
    iterators::merged_iterator::MergedIterator, lsm_storage::StorageState,
    manifest::ManifestRecord::Compaction,
};
const COMPACT_INTERVAL: Duration = Duration::from_secs(60);

impl Storage {
    pub fn trigger_compaction(&self) -> Result<()> {
        let state = {
            let guard = self.state.read().unwrap();
            guard.clone()
        };
        let l0 = state.l0_sstables.clone();
        if l0.is_empty() {
            return Ok(());
        }
        let l1 = state.levels[0].1.clone();
        let ssts_to_compact = [&l0[..], &l1[..]].concat();

        let mut iters = vec![];
        for sst_id in ssts_to_compact.iter() {
            let sstable = state.sstables[sst_id].clone();
            let iter = SSTableIterator::create_and_seek_to_first(sstable).unwrap();
            iters.push(iter);
        }

        let mut merged_iter = MergedIterator::new(iters);
        let mut builder = SSTableBuilder::new(self.config.block_size);
        while merged_iter.is_valid() {
            builder.add(merged_iter.key(), merged_iter.value());
            merged_iter.next()?;
        }

        let compact_sst_id = {
            let id = self.get_sst_id();
            let table = builder.build(id, self.block_cache.clone(), self.sst_path(id))?;

            let mut write_guard = self.state.write().unwrap();
            let mut l0_sstables = write_guard.l0_sstables.clone();
            let mut sstables = write_guard.sstables.clone();
            let mut levels = write_guard.levels.clone();

            for sst_id in l0.iter() {
                sstables.remove(sst_id);
                l0_sstables.retain(|&x| x != *sst_id);
                remove_file(self.sst_path(*sst_id))?;
            }

            for sst_id in l1.iter() {
                sstables.remove(sst_id);
                levels[0].1.retain(|&x| x != *sst_id);
                remove_file(self.sst_path(*sst_id))?;
            }

            sstables.insert(id, Arc::new(table));
            levels[0].1.insert(0, id);

            *write_guard = Arc::new(StorageState {
                memtable: write_guard.memtable.clone(),
                frozen_memtables: write_guard.frozen_memtables.clone(),
                l0_sstables,
                sstables,
                levels,
            });

            id
        };

        let state_lock = self.state_lock.lock().unwrap();
        self.manifest
            .add_record(&state_lock, Compaction(compact_sst_id))?;
        Ok(())
    }

    pub fn spawn_compacter(self: &Arc<Self>) -> JoinHandle<()> {
        let this = self.clone();

        thread::spawn(move || {
            loop {
                this.trigger_compaction().expect("sst compaction failed");
                thread::sleep(COMPACT_INTERVAL);
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{ops::Bound, str::from_utf8};

    use bytes::Bytes;
    use tempfile::tempdir;

    use crate::{Config, common::iterator::StorageIterator, lsm_util::get_entries, new};

    #[test]
    fn test_compaction() {
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

        // create 2 sstables
        storage
            .flush_frozen_memtable()
            .expect("memtable to have been frozen");
        storage
            .flush_frozen_memtable()
            .expect("memtable to have been frozen");

        let initial_sst_count = storage.state.read().unwrap().l0_sstables.len();
        storage.trigger_compaction().expect("ssts were compacted");

        let curr_sst_count = storage.state.read().unwrap().l0_sstables.len();
        let l1_entries = storage.state.read().unwrap().levels[0].1.clone();

        assert_eq!(curr_sst_count, initial_sst_count - 2);
        assert_eq!(l1_entries.len(), 1);
    }

    #[test]
    fn scans_through_memtables_l0_l1_sstables() {
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

        // will create two sstables at l0
        storage
            .flush_frozen_memtable()
            .expect("memtable to have been frozen");
        storage
            .flush_frozen_memtable()
            .expect("memtable to have been frozen");

        // compact l0 sstables to an l1 sstable
        storage.trigger_compaction().expect("compacted");

        // create another l0 sstable
        storage
            .flush_frozen_memtable()
            .expect("memtable to have been frozen");

        let new_entries = vec![(b"a", b"20"), (b"e", b"21"), (b"d", b"22"), (b"b", b"23")];
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
    fn test_can_latest_key_is_read_from_l1_sstables() {
        let db_dir = String::from(tempdir().unwrap().path().to_str().unwrap());
        let config = Config {
            sst_size: 4,
            block_size: 32,
            db_dir: db_dir.clone(),
            num_memtable_limit: 5,
            enable_wal: true,
        };
        let storage = new(config).unwrap();
        let mut entries = get_entries();

        // adds a new version of key a=3
        entries[2] = (b"a", b"3");

        for (key, value) in entries {
            storage.put(key, value).unwrap();
        }

        // will create an sstables at l0 with a, b
        storage
            .flush_frozen_memtable()
            .expect("memtable to have been frozen");
        // compact l0 sstables to an l1 sstable
        storage.trigger_compaction().expect("compacted");

        // will create an sstable at l0 with a, c. a newer version of the initially stored a
        storage
            .flush_frozen_memtable()
            .expect("memtable to have been frozen");
        storage.trigger_compaction().expect("compacted");

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

        assert_eq!(
            keys,
            vec!["a", "b", "d", "e", "f", "g", "h", "i", "j", "k", "l"]
        );
        assert_eq!(
            values,
            vec!["3", "2", "4", "5", "6", "7", "8", "9", "10", "11", "12"]
        );
    }

    #[test]
    fn test_get_key() {
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

        // will create two sstables at l0 with a, b, c & d
        storage
            .flush_frozen_memtable()
            .expect("memtable to have been frozen");
        storage
            .flush_frozen_memtable()
            .expect("memtable to have been frozen");

        // compact l0 sstables to an l1 sstable with keys a, b, c & d
        storage.trigger_compaction().expect("compacted");

        // create another l0 sstable, will have keys e & f
        storage
            .flush_frozen_memtable()
            .expect("memtable to have been frozen");

        let l1_value = storage.get(b"a").expect("a exists");
        assert_eq!(l1_value, Bytes::copy_from_slice(b"1"));

        let l0_value = storage.get(b"e").expect("a exists");
        assert_eq!(l0_value, Bytes::copy_from_slice(b"5"));
    }
}
