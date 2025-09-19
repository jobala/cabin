use std::{
    cmp::Ordering::{Greater, Less},
    rc::Rc,
    sync::atomic::{AtomicUsize, Ordering},
};

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;

use crate::memtable::iterator::MemtableIterator;

#[derive(Debug)]
pub struct Memtable {
    skip_map: Rc<SkipMap<Bytes, Bytes>>,
    size: AtomicUsize,
}

impl Memtable {
    pub fn new(size: usize) -> Self {
        Memtable {
            skip_map: Rc::new(SkipMap::new()),
            size: AtomicUsize::new(size),
        }
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.skip_map
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));

        self.size
            .fetch_add(key.len() + value.len(), Ordering::SeqCst);
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.skip_map.get(key).map(|k| k.value().clone())
    }

    pub fn iter(&self) -> MemtableIterator {
        let mut iter = self.skip_map.iter();
        let current = iter.next();

        MemtableIterator { iter, current }
    }
}

impl<'a> PartialOrd for MemtableIterator<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Ord for MemtableIterator<'a> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let v1 = self.current.as_ref().unwrap().key();
        let v2 = other.current.as_ref().unwrap().key();

        v1.cmp(v2)
    }
}

impl<'a> PartialEq for MemtableIterator<'a> {
    fn eq(&self, other: &Self) -> bool {
        let v1 = self.current.as_ref().unwrap().key();
        let v2 = other.current.as_ref().unwrap().key();

        v1 == v2
    }
}

impl<'a> Eq for MemtableIterator<'a> {}
