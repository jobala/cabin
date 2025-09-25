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
        if let Some(top_iter) = self.heap.pop() {
            top_iter.next()?;

            if top_iter.is_valid() {
                self.heap.push(top_iter);
            }
        }
        Ok(())
    }
}
