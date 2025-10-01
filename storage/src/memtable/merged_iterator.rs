use bytes::Bytes;

use crate::memtable::iterator::{MemtableIterator, StorageIterator};
use std::collections::BinaryHeap;

struct MergedIterator {
    heap: BinaryHeap<HeapEntry>,
}

impl MergedIterator {
    pub fn new(iterators: Vec<MemtableIterator>) -> Self {
        let mut heap = BinaryHeap::new();

        for iter in iterators {
            heap.push(HeapEntry::new(iter));
        }

        MergedIterator { heap }
    }
}

impl StorageIterator for MergedIterator {
    fn value(&self) -> &[u8] {
        self.heap.peek().unwrap().iter.value()
    }

    fn key(&self) -> &[u8] {
        self.heap.peek().unwrap().iter.key()
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
            Bytes::copy_from_slice(top.iter.key())
        };

        let mut entries_with_same_key = vec![];
        while let Some(top) = self.heap.peek() {
            if top.iter.key() == current_key {
                entries_with_same_key.push(self.heap.pop().unwrap());
            } else {
                break;
            }
        }

        for mut entry in entries_with_same_key {
            entry.iter.next()?;

            if entry.iter.is_valid() {
                self.heap.push(entry);
            }
        }
        Ok(())
    }
}

struct HeapEntry {
    iter: MemtableIterator,
}

impl HeapEntry {
    fn new(iter: MemtableIterator) -> Self {
        HeapEntry { iter }
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.iter.key().cmp(&self.iter.key())
    }
}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for HeapEntry {}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.iter.key() == other.iter.key()
    }
}

#[cfg(test)]
mod test {
    use crate::memtable::table::Memtable;
    use std::ops::Bound::Unbounded;

    use super::*;

    #[test]
    fn test_iterates_through_all_iterators() {
        let iter1 = Memtable::new();
        let iter2 = Memtable::new();
        let iter3 = Memtable::new();

        let _ = iter3.put(b"3", b"3");
        let _ = iter1.put(b"1", b"1");
        let _ = iter2.put(b"2", b"2");

        let iterators = vec![
            iter1.scan(Unbounded, Unbounded),
            iter2.scan(Unbounded, Unbounded),
            iter3.scan(Unbounded, Unbounded),
        ];

        // Pass as mutable slice
        let mut merged_iters = MergedIterator::new(iterators);

        while merged_iters.is_valid() {
            let (key, value) = (merged_iters.key(), merged_iters.value());

            println!(
                "{} {}",
                String::from_utf8_lossy(key),
                String::from_utf8_lossy(value)
            );
            let _ = merged_iters.next();
        }
    }

    #[test]
    #[ignore]
    fn handles_duplicate_entries_across_iterators() {}

    #[test]
    #[ignore]
    fn skips_deleted_keys() {}

    fn create_iterators(items: Vec<Vec<&[u8]>>) -> Vec<MemtableIterator> {
        let mut res = vec![];
        let memtable = Memtable::new();

        for item in items {
            for key in item {
                memtable.put(key, key);
            }

            res.push(memtable.scan(Unbounded, Unbounded));
        }

        res
    }
}
