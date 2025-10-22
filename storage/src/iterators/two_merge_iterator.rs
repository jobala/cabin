use std::{cmp::Ordering, str::from_utf8};

use anyhow::{Ok, Result};

use crate::common::iterator::StorageIterator;

/// Merges two iterators of different types (memtable & sst iterators) into one .
/// If the two iterators have the same key, only produce the key once and prefer the entry from `mem_iter`.
pub struct TwoMergeIterator<T: StorageIterator, W: StorageIterator> {
    mem_iter: T,
    sst_iter: W,
    is_mem: bool,
}

impl<T: 'static + StorageIterator, W: 'static + StorageIterator> TwoMergeIterator<T, W> {
    pub fn create(mem_iter: T, sst_iter: W) -> Result<Self> {
        let is_mem = {
            match mem_iter.key().cmp(sst_iter.key()) {
                Ordering::Equal => mem_iter.is_valid(),
                Ordering::Less => mem_iter.is_valid(),
                Ordering::Greater => false,
            }
        };

        Ok(TwoMergeIterator {
            mem_iter,
            sst_iter,
            is_mem,
        })
    }
}

impl<T: 'static + StorageIterator, W: 'static + StorageIterator> StorageIterator
    for TwoMergeIterator<T, W>
{
    fn key(&self) -> &[u8] {
        match self.is_mem {
            true => self.mem_iter.key(),
            _ => self.sst_iter.key(),
        }
    }

    fn value(&self) -> &[u8] {
        match self.is_mem {
            true => self.mem_iter.value(),
            _ => self.sst_iter.value(),
        }
    }

    fn is_valid(&self) -> bool {
        self.mem_iter.is_valid() || self.sst_iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        // skip duplicate key in sst_iter
        if self.is_mem && self.mem_iter.key().cmp(self.sst_iter.key()) == Ordering::Equal {
            self.sst_iter.next()?;
        }

        match self.is_mem {
            true => self.mem_iter.next()?,
            _ => self.sst_iter.next()?,
        };

        // determine which iterator to use
        self.is_mem = {
            match self.mem_iter.key().cmp(self.sst_iter.key()) {
                Ordering::Equal => self.mem_iter.is_valid(),
                Ordering::Less => self.mem_iter.is_valid(),
                Ordering::Greater => false,
            }
        };

        Ok(())
    }
}
