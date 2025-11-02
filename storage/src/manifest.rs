use std::io::Write;
use std::path::Path;
use std::sync::{Arc, MutexGuard};
use std::{fs::File, sync::Mutex};

use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};

pub struct Manifest {
    file: Arc<Mutex<File>>,
}

#[derive(Serialize, Deserialize)]
pub enum ManifestRecord {
    Flush(usize),
    NewMemtable(usize),
    Compaction(Vec<usize>),
}

impl Manifest {
    pub fn create(_path: impl AsRef<Path>) -> Result<Self> {
        unimplemented!()
    }

    pub fn recover(_path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        unimplemented!()
    }

    pub fn add_record(
        &self,
        _state_lock_observer: &MutexGuard<()>,
        record: ManifestRecord,
    ) -> Result<()> {
        self.add_record_when_init(record)
    }

    // TODO: compact the manifest when it gets too large
    pub fn add_record_when_init(&self, record: ManifestRecord) -> Result<()> {
        let mut file = self.file.lock().expect("acquired lock");
        let json_record = serde_json::to_vec(&record)?;

        file.write_all(&json_record)?;
        file.sync_all()?;
        Ok(())
    }
}
