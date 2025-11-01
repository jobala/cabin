use std::sync::Arc;

use anyhow::Ok;

use crate::{SSTable, SSTableIterator, common::iterator::StorageIterator};

pub struct ConcatIterator {
    current: Option<SSTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SSTable>>,
}

impl ConcatIterator {
    fn create_and_seek_to_first(sstables: Vec<Arc<SSTable>>) -> Self {
        if sstables.is_empty() {
            return Self {
                current: None,
                next_sst_idx: 0,
                sstables,
            };
        }

        let current = sstables
            .first()
            .map(|table| SSTableIterator::create_and_seek_to_first(table.clone()).unwrap());

        Self {
            current,
            next_sst_idx: 1,
            sstables,
        }
    }

    fn create_and_seek_to_key(sstables: Vec<Arc<SSTable>>, key: &[u8]) -> Self {
        let mut idx = 0;
        for table in sstables.iter() {
            if key <= table.last_key() {
                break;
            }

            idx += 1
        }

        let current = sstables
            .get(idx)
            .map(|table| SSTableIterator::create_and_seek_to_key(table.clone(), key).unwrap());

        Self {
            current,
            next_sst_idx: idx + 1,
            sstables,
        }
    }
}

impl StorageIterator for ConcatIterator {
    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().value()
    }

    fn key(&self) -> &[u8] {
        self.current.as_ref().unwrap().key()
    }

    fn is_valid(&self) -> bool {
        self.current.is_some()
    }

    fn next(&mut self) -> anyhow::Result<()> {
        if self.current.as_ref().unwrap().is_valid() {
            self.current = self
                .sstables
                .get(self.next_sst_idx)
                .map(|sstable| SSTableIterator::create_and_seek_to_first(sstable.clone()).unwrap());

            self.next_sst_idx += 1;
            return Ok(());
        }

        self.current.as_mut().unwrap().next()
    }
}
