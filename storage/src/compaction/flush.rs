use anyhow::Result;
use std::{
    sync::Arc,
    thread::{self, JoinHandle},
    time::Duration,
};

use crate::{SSTableBuilder, Storage, lsm_storage::StorageState};

impl Storage {
    pub(crate) fn flush_frozen_memtable(&self) -> Result<()> {
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
        l0_sstables.insert(0, memtable.id);
        sstables.insert(memtable.id, Arc::new(sst));

        *guard = Arc::new(StorageState {
            memtable: guard.memtable.clone(),
            frozen_memtables: memtables,
            l0_sstables,
            sstables,
        });

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

    fn sst_path(&self, id: usize) -> String {
        format!("{}/sst/{}.sst", self.config.db_dir, id)
    }
}

// TODO: suppot msg passing
pub fn spawn_flusher(storage: Arc<Storage>) -> JoinHandle<()> {
    let this = storage.clone();

    thread::spawn(move || {
        loop {
            this.flush().expect("memtable to have been flushed");
            thread::sleep(Duration::from_millis(50));
        }
    })
}
