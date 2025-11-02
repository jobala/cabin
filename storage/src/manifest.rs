use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::{Arc, MutexGuard};
use std::{fs::File, sync::Mutex};

use anyhow::Result;
use bytes::Buf;
use serde::{Deserialize, Serialize};

pub struct Manifest {
    file: Arc<Mutex<File>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ManifestRecord {
    Flush(usize),
    NewMemtable(usize),
    Compaction(usize),
}

impl Manifest {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let mut open_opts = OpenOptions::new();
        open_opts.read(true).write(true).create(true);

        let file = Mutex::new(open_opts.open(path)?);
        Ok(Self {
            file: Arc::new(file),
        })
    }

    pub fn recover(path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path)
            .unwrap();
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut buf_ptr = buf.as_slice();
        let mut records = Vec::new();
        while buf_ptr.has_remaining() {
            let len = buf_ptr.get_u64();
            let slice = &buf_ptr[..len as usize];
            let json = serde_json::from_slice::<ManifestRecord>(slice)?;
            buf_ptr.advance(len as usize);
            records.push(json);
        }
        Ok((
            Self {
                file: Arc::new(Mutex::new(file)),
            },
            records,
        ))
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
        let mut file = self.file.lock().expect("to have acquired lock");
        let json_record = serde_json::to_vec(&record)?;

        file.write_all(&(json_record.len() as u64).to_be_bytes())?;
        file.write_all(&json_record)?;
        file.sync_all()?;
        Ok(())
    }
}

#[cfg(test)]
mod test {

    use super::ManifestRecord::{Compaction, Flush};
    use super::*;
    use std::sync::Mutex;
    use tempfile::tempdir;

    #[test]
    fn test_manifest_recovery() {
        let dir = tempdir().unwrap();
        let manifest_file = dir.path().join("manifest");

        let manifest = Manifest::create(manifest_file.as_path()).unwrap();

        let lock = Mutex::default();
        manifest
            .add_record(&lock.lock().unwrap(), Flush(1))
            .unwrap();
        manifest
            .add_record(&lock.lock().unwrap(), Flush(2))
            .unwrap();
        manifest
            .add_record(&lock.lock().unwrap(), Compaction(3))
            .unwrap();

        let manifest_records = Manifest::recover(manifest_file.as_path()).unwrap();
        assert_eq!(manifest_records.1.len(), 3);
    }
}
