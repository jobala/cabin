use std::ops::Bound;

use anyhow::{Ok, Result};
use bytes::Bytes;

use crate::{
    SSTableIterator,
    common::iterator::StorageIterator,
    iterators::{merged_iterator::MergedIterator, two_merge_iterator::TwoMergeIterator},
    memtable::memtable_iterator::MemtableIterator,
};

type LsmIteratorInner =
    TwoMergeIterator<MergedIterator<MemtableIterator>, MergedIterator<SSTableIterator>>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    end_bound: Bound<Bytes>,
    valid: bool,
}

impl LsmIterator {
    pub fn new(iter: LsmIteratorInner, upper: Bound<Bytes>) -> Self {
        let mut lsm_iterator = Self {
            inner: iter,
            end_bound: upper,
            valid: true,
        };
        lsm_iterator.skip_deleted();

        lsm_iterator
    }

    fn skip_deleted(&mut self) {
        while self.inner.is_valid() && self.inner.value().is_empty() {
            let _ = self.inner.next();
        }
    }
}

impl StorageIterator for LsmIterator {
    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn key(&self) -> &[u8] {
        self.inner.key()
    }

    fn is_valid(&self) -> bool {
        self.valid
    }

    fn next(&mut self) -> Result<()> {
        self.inner.next()?;

        if !self.inner.is_valid() {
            self.valid = false;
            return Ok(());
        }

        self.skip_deleted();

        match self.end_bound.as_ref() {
            Bound::Unbounded => {}
            Bound::Included(key) => self.valid = self.inner.key() <= &key[..],
            Bound::Excluded(key) => self.valid = self.inner.key() < &key[..],
        };
        Ok(())
    }
}
