use std::ops::{Bound, RangeBounds};

use bytes::Bytes;
use crossbeam_skiplist::map::{Entry, Range};

use crate::common::errors::KeyNotFound;

#[derive(Debug)]
pub struct MemtableIterator<'a> {
    pub(crate) iter: Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>,
    pub(crate) current: Option<Entry<'a, Bytes, Bytes>>,
}

pub trait StorageIterator {
    fn value(&self) -> Option<&[u8]>;
    fn key(&self) -> Result<&[u8], KeyNotFound>;
    fn is_valid(&self) -> bool;
    fn next(&mut self) -> anyhow::Result<()>;
}

impl<'a> StorageIterator for MemtableIterator<'a> {
    fn value(&self) -> Option<&[u8]> {
        match self.current.as_ref() {
            Some(entry) => Some(entry.value()),
            None => None,
        }
    }

    fn key(&self) -> Result<&[u8], KeyNotFound> {
        if let Some(item) = self.current.as_ref() {
            return Ok(item.key());
        } else {
            Err(KeyNotFound)
        }
    }

    fn is_valid(&self) -> bool {
        self.current.is_some()
    }

    fn next(&mut self) -> anyhow::Result<()> {
        self.current = self.iter.next();
        Ok(())
    }
}
