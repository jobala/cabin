use bytes::Bytes;

use crate::{
    common::errors::KeyNotFound,
    memtable::iterator::{MemtableIterator, StorageIterator},
};
use std::collections::BinaryHeap;

struct MergedIterator<'a> {
    heap: BinaryHeap<&'a mut MemtableIterator<'a>>,
}

impl<'a> MergedIterator<'a> {
    pub fn new(iterators: &'a mut [MemtableIterator<'a>]) -> Self {
        let mut heap = BinaryHeap::new();
        for iter in iterators.iter_mut() {
            heap.push(iter);
        }
        MergedIterator { heap }
    }
}

impl<'a> StorageIterator for MergedIterator<'a> {
    fn value(&self) -> Option<&[u8]> {
        match self.heap.peek()?.current.as_ref() {
            Some(item) => Some(item.value()),
            None => None,
        }
    }

    fn key(&self) -> Result<&[u8], KeyNotFound> {
        if let Some(heap_entry) = self.heap.peek() {
            return match heap_entry.current.as_ref() {
                Some(item) => Ok(item.key()),
                None => Err(KeyNotFound),
            };
        } else {
            Err(KeyNotFound)
        }
    }

    fn is_valid(&self) -> bool {
        self.heap.peek().is_some()
    }

    fn next(&mut self) -> anyhow::Result<()> {
        if self.heap.is_empty() {
            return Ok(());
        }

        let current_key = {
            let top = self.heap.peek().unwrap();
            Bytes::copy_from_slice(top.key().unwrap())
        };

        let mut entries_with_same_key = vec![];

        while let Some(top) = self.heap.peek() {
            if top.key().unwrap() == current_key {
                entries_with_same_key.push(self.heap.pop().unwrap());
            } else {
                break;
            }
        }

        for entry in entries_with_same_key {
            entry.next()?;

            if entry.is_valid() {
                self.heap.push(entry);
            }
        }
        Ok(())
    }
}
