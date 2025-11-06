use anyhow::Result;
use std::{
    fs,
    path::Path,
    sync::Arc,
    thread::{self, JoinHandle},
    time::Duration,
};

use crate::{SSTableBuilder, Storage, lsm_storage::StorageState, manifest::ManifestRecord::Flush};

const FLUSH_INTERVAL: Duration = Duration::from_millis(50);

impl Storage {
    pub(crate) fn flush_frozen_memtable(&self) -> Result<()> {
        let sst_id = {
            let mut sst_builder = SSTableBuilder::new(self.config.block_size);
            let mut guard = self.state.write().unwrap();

            let mut frozen_memtables = guard.frozen_memtables.clone();
            let mut l0_sstables = guard.l0_sstables.clone();
            let mut sstables = guard.sstables.clone();

            let Some(memtable) = frozen_memtables.pop() else {
                return Ok(());
            };
            memtable.flush(&mut sst_builder)?;

            let sst = sst_builder.build(
                memtable.id,
                self.block_cache.clone(),
                self.sst_path(memtable.id),
            )?;
            l0_sstables.insert(0, memtable.id);
            sstables.insert(memtable.id, Arc::new(sst));

            if self.config.enable_wal {
                self.remove_wal(memtable.id)?;
            }

            *guard = Arc::new(StorageState {
                frozen_memtables,
                l0_sstables,
                sstables,
                levels: guard.levels.clone(),
                memtable: guard.memtable.clone(),
            });

            memtable.id
        };

        let state_lock = self.state_lock.lock().unwrap();
        self.manifest.add_record(&state_lock, Flush(sst_id))?;
        Ok(())
    }

    fn trigger_flush(&self) -> Result<()> {
        let memtable_count = {
            let guard = self.state.read().unwrap();
            guard.frozen_memtables.len()
        };

        if self.config.num_memtable_limit > memtable_count {
            self.flush_frozen_memtable()?;
        }

        Ok(())
    }

    fn remove_wal(&self, id: usize) -> Result<()> {
        let wal_path = Path::new(&self.config.db_dir).join(format!("{id}.wal"));
        fs::remove_file(wal_path)?;

        Ok(())
    }

    pub fn sst_path(&self, id: usize) -> String {
        format!("{}/{}.sst", self.config.db_dir, id)
    }

    pub fn spawn_flusher(self: &Arc<Self>) -> JoinHandle<()> {
        let this = self.clone();

        thread::spawn(move || {
            loop {
                this.trigger_flush().expect("memtable to have been flushed");
                thread::sleep(FLUSH_INTERVAL);
            }
        })
    }
}
