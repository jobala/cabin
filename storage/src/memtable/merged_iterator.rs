use crate::memtable::iterator::{MemtableIterator, StorageIterator};
use anyhow::Ok;
use bytes::Bytes;
use crossbeam_skiplist::map::Entry;
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
    fn value(&self) -> &[u8] {
        self.heap.peek().unwrap().current.as_ref().unwrap().value()
    }

    fn key(&self) -> &[u8] {
        self.heap.peek().unwrap().current.as_ref().unwrap().key()
    }

    fn is_valid(&self) -> bool {
        !self.heap.is_empty() && self.heap.peek().unwrap().current.is_some()
    }

    fn next(&mut self) -> anyhow::Result<()> {
        if let Some(mut top_iter) = self.heap.pop() {
            top_iter.next()?;

            if top_iter.is_valid() {
                self.heap.push(top_iter);
            }
        }
        Ok(())
    }
}
