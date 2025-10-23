use std::ops::Bound;

use anyhow::Ok;

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
}

impl LsmIterator {
    pub fn new(iter: LsmIteratorInner, upper: Bound<&[u8]>) -> Self {
        let mut lsm_iterator = Self { inner: iter };
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
        self.inner.is_valid()
    }

    fn next(&mut self) -> anyhow::Result<()> {
        let _ = self.inner.next();
        self.skip_deleted();
        Ok(())
    }
}
