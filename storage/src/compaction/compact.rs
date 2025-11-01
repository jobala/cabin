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
};
const COMPACT_INTERVAL: Duration = Duration::from_secs(60);

impl Storage {
    pub fn trigger_compaction(&self) -> Result<()> {
        let state = {
            let guard = self.state.read().unwrap();
            guard.clone()
        };
        let ssts_to_compact = state.l0_sstables.clone();
        if ssts_to_compact.is_empty() {
            return Ok(());
        }

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

        let id = self.get_sst_id();
        let table = builder.build(id, self.block_cache.clone(), self.sst_path(id))?;

        let mut write_guard = self.state.write().unwrap();
        let mut l0_sstables = write_guard.l0_sstables.clone();
        let mut sstables = write_guard.sstables.clone();
        let mut levels = write_guard.levels.clone();

        for sst_id in ssts_to_compact.iter() {
            sstables.remove(sst_id);
            l0_sstables.retain(|&x| x != *sst_id);

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
    use tempfile::tempdir;

    use crate::{Config, lsm_util::get_entries, new};

    #[test]
    fn test_compaction() {
        let db_dir = String::from(tempdir().unwrap().path().to_str().unwrap());
        let config = Config {
            sst_size: 4,
            block_size: 32,
            db_dir: db_dir.clone(),
            num_memtable_limit: 5,
        };
        let storage = new(config);

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
}
