use std::{
    ops::Bound,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;

use crate::{SSTableBuilder, memtable::memtable_iterator::MemtableIterator, wal::Wal};

#[derive(Debug, Clone)]
pub struct Memtable {
    pub(crate) size: Arc<AtomicUsize>,
    pub(crate) id: usize,
    skip_map: Arc<SkipMap<Bytes, Bytes>>,
    wal: Option<Wal>,
}

impl Memtable {
    pub fn new_with_wal(id: usize, wal_path: &Path) -> Result<Memtable> {
        let skip_map = SkipMap::new();
        let (size, wal) = Wal::recover(wal_path, &skip_map)?;

        Ok(Self {
            id,
            wal: Some(wal),
            skip_map: Arc::new(skip_map),
            size: Arc::new(AtomicUsize::new(size as usize)),
        })
    }

    pub fn new(id: usize) -> Self {
        Self {
            wal: None,
            skip_map: Arc::new(SkipMap::new()),
            size: Arc::new(AtomicUsize::new(0)),
            id,
        }
    }
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        if let Some(wal) = &self.wal {
            wal.put(key, value)?;
        };

        self.skip_map
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));

        self.size
            .fetch_add(key.len() + value.len(), Ordering::Relaxed);

        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.skip_map.get(key).map(|k| k.value().clone())
    }

    pub fn get_size(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> MemtableIterator {
        MemtableIterator::create(self.skip_map.clone(), lower, upper)
    }

    pub fn sync_wal(&self) -> Result<()> {
        if let Some(ref wal) = self.wal {
            wal.sync()?;
        }
        Ok(())
    }

    pub fn flush(&self, builder: &mut SSTableBuilder) -> Result<()> {
        for entry in self.skip_map.iter() {
            if entry.value().is_empty() {
                continue;
            }

            builder.add(entry.key(), entry.value());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn can_put_and_get_items() {
        let memtable = Memtable::new(1);
        let _ = memtable.put(b"1", b"2");
        let out = &memtable.get(b"1").unwrap()[..];

        assert_eq!(b"2", out);
    }

    #[test]
    fn memtable_grows_in_size_after_put() {
        let memtable = Memtable::new(1);
        let _ = memtable.put(b"1", b"2");

        assert_eq!(2, memtable.get_size());
    }

    #[test]
    #[should_panic]
    fn key_not_found() {
        let memtable = Memtable::new(1);
        let _ = memtable.put(b"1", b"2");
        let out = &memtable.get(b"5").unwrap()[..];

        assert_eq!(b"-1", out);
    }
}
