use std::rc::Rc;

use anyhow::Ok;
use bytes::Bytes;
use crossbeam_skiplist::map::{Entry, Iter};

pub struct MemtableIterator<'a> {
    pub(crate) iter: Iter<'a, Bytes, Bytes>,
    pub(crate) current: Option<Entry<'a, Bytes, Bytes>>,
}

pub trait StorageIterator {
    fn value(&self) -> &[u8];
    fn key(&self) -> &[u8];
    fn is_valid(&self) -> bool;
    fn next(&mut self) -> anyhow::Result<()>;
}

impl<'a> StorageIterator for MemtableIterator<'a> {
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
        self.current = self.iter.next();
        Ok(())
    }
}
