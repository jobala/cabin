use std::{
    ops::Bound,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;

use crate::memtable::memtable_iterator::MemtableIterator;

impl Memtable {
    pub fn new() -> Self {
        Memtable {
            skip_map: Arc::new(SkipMap::new()),
            size: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
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
}

#[derive(Debug, Clone)]
pub struct Memtable {
    pub(crate) size: Arc<AtomicUsize>,
    skip_map: Arc<SkipMap<Bytes, Bytes>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn can_put_and_get_items() {
        let memtable = Memtable::new();
        let _ = memtable.put(b"1", b"2");
        let out = &memtable.get(b"1").unwrap()[..];

        assert_eq!(b"2", out);
    }

    #[test]
    fn memtable_grows_in_size_after_put() {
        let memtable = Memtable::new();
        let _ = memtable.put(b"1", b"2");

        assert_eq!(2, memtable.get_size());
    }

    #[test]
    #[should_panic]
    fn key_not_found() {
        let memtable = Memtable::new();
        let _ = memtable.put(b"1", b"2");
        let out = &memtable.get(b"5").unwrap()[..];

        assert_eq!(b"-1", out);
    }
}
