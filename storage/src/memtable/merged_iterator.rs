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
    use std::{collections::HashMap, ops::Bound::Unbounded};

    use super::*;

    #[test]
    fn test_iterates_through_all_iterators() {
        let mut map1 = HashMap::new();
        map1.insert(b"b", b"2");

        let mut map2 = HashMap::new();
        map2.insert(b"a", b"1");

        let mut map3 = HashMap::new();
        map3.insert(b"e", b"4");

        let iterators = create_iterators(vec![map1, map2, map3]);
        let mut merged_iters = MergedIterator::new(iterators);

        let mut res = Vec::new();
        while merged_iters.is_valid() {
            let value = merged_iters.value().to_vec();
            res.push(value);

            let _ = merged_iters.next();
        }

        assert_eq!(res, vec![b"1", b"2", b"4"])
    }

    #[test]
    fn handles_duplicate_entries_across_iterators() {
        let mut map1 = HashMap::new();
        map1.insert(b"c", b"4");
        map1.insert(b"d", b"5");

        let mut map2 = HashMap::new();
        map2.insert(b"a", b"1");
        map2.insert(b"b", b"2");
        map2.insert(b"c", b"3");

        let mut map3 = HashMap::new();
        map3.insert(b"e", b"6");

        let iterators = create_iterators(vec![map1, map2, map3]);
        let mut merged_iters = MergedIterator::new(iterators);

        let mut res = Vec::new();
        while merged_iters.is_valid() {
            let value = merged_iters.value().to_vec();
            res.push(value);

            let _ = merged_iters.next();
        }

        assert_eq!(res, vec![b"1", b"2", b"4", b"5", b"6"])
    }

    fn create_iterators(items: Vec<HashMap<&[u8; 1], &[u8; 1]>>) -> Vec<MemtableIterator> {
        let mut res = vec![];

        for item in items {
            let memtable = Memtable::new();

            for (key, val) in item {
                let _ = memtable.put(key, val);
            }

            res.push(memtable.scan(Unbounded, Unbounded));
        }

        res
    }
}
