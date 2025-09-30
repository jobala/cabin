use bytes::Bytes;

use crate::{
    common::errors::KeyNotFound,
    memtable::iterator::{MemtableIterator, StorageIterator},
};
use std::collections::BinaryHeap;

struct MergedIterator<'a> {
    heap: BinaryHeap<HeapEntry<'a>>,
}

impl<'a> MergedIterator<'a> {
    pub fn new(iterators: Vec<MemtableIterator<'a>>) -> Self {
        let mut heap = BinaryHeap::new();

        for iter in iterators {
            heap.push(HeapEntry::new(iter));
        }

        MergedIterator { heap }
    }
}

impl<'a> StorageIterator for MergedIterator<'a> {
    fn value(&self) -> Option<&[u8]> {
        match self.heap.peek()?.iter.current.as_ref() {
            Some(item) => Some(item.value()),
            None => None,
        }
    }

    fn key(&self) -> Result<&[u8], KeyNotFound> {
        if let Some(heap_entry) = self.heap.peek() {
            match heap_entry.iter.current.as_ref() {
                Some(item) => Ok(item.key()),
                None => Err(KeyNotFound),
            }
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
            Bytes::copy_from_slice(top.iter.key().unwrap())
        };

        let mut entries_with_same_key = vec![];
        while let Some(top) = self.heap.peek() {
            if top.iter.key().unwrap() == current_key {
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

struct HeapEntry<'a> {
    iter: MemtableIterator<'a>,
}

impl<'a> HeapEntry<'a> {
    fn new(iter: MemtableIterator<'a>) -> Self {
        HeapEntry { iter }
    }
}

impl<'a> Ord for HeapEntry<'a> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.iter.key().ok().cmp(&self.iter.key().ok())
    }
}

impl<'a> PartialOrd for HeapEntry<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Eq for HeapEntry<'a> {}

impl<'a> PartialEq for HeapEntry<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.iter.key().ok() == other.iter.key().ok()
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
            let (key, value) = (merged_iters.key().unwrap(), merged_iters.value().unwrap());

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
}
