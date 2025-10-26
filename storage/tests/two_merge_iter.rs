use std::ops::Bound::Unbounded;
use std::str::from_utf8;
use std::{collections::HashMap, sync::Arc};

use cabin_storage::common::iterator::StorageIterator;
use cabin_storage::{
    SSTableBuilder, SSTableIterator,
    iterators::{merged_iterator::MergedIterator, two_merge_iterator::TwoMergeIterator},
    memtable::{memtable_iterator::MemtableIterator, table::Memtable},
};
use moka::sync::Cache;
use tempfile::NamedTempFile;

#[test]
fn test_merged_and_sst_iter() {
    let mem_iter = create_merged_iterator();
    let sst_iter = create_sst_iterator();
    let mut iter = TwoMergeIterator::create(mem_iter, sst_iter).unwrap();

    let mut keys = Vec::new();
    let mut values = Vec::new();
    while iter.is_valid() {
        keys.push(from_utf8(iter.key()).unwrap().to_string());
        values.push(from_utf8(iter.value()).unwrap().to_string());
        _ = iter.next();
    }

    assert_eq!(
        keys,
        vec!["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l"]
    );

    assert_eq!(
        values,
        vec![
            "1", "2", "4", "5", "4", "6", "7", "8", "9", "10", "11", "12"
        ]
    )
}

#[test]
fn test_with_empty_merged_iter() {
    let mem_iter = create_empty_merged_iter();
    let sst_iter = create_sst_iterator();
    let mut iter = TwoMergeIterator::create(mem_iter, sst_iter).unwrap();

    let mut keys = Vec::new();
    let mut values = Vec::new();
    while iter.is_valid() {
        keys.push(from_utf8(iter.key()).unwrap().to_string());
        values.push(from_utf8(iter.value()).unwrap().to_string());
        _ = iter.next();
    }

    assert_eq!(
        keys,
        vec!["b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l"]
    );

    assert_eq!(
        values,
        vec!["-2", "-3", "-4", "-5", "6", "7", "8", "9", "10", "11", "12"]
    )
}

fn create_sst_iterator() -> SSTableIterator {
    let mut sst_builder = SSTableBuilder::new(32); // block size of 32 bytes
    let block_cache = Arc::new(Cache::new(1));

    for (key, value) in get_sst_entries() {
        sst_builder.add(key, value);
    }

    let tmp_file = NamedTempFile::new().unwrap();
    let sst = sst_builder
        .build(1, block_cache.clone(), tmp_file.path())
        .unwrap();

    SSTableIterator::create_and_seek_to_first(Arc::new(sst)).unwrap()
}

fn create_merged_iterator() -> MergedIterator<MemtableIterator> {
    let mut map1 = HashMap::new();
    map1.insert(b"c", b"4");
    map1.insert(b"d", b"5");

    let mut map2 = HashMap::new();
    map2.insert(b"a", b"1");
    map2.insert(b"b", b"2");
    map2.insert(b"c", b"3");

    let mut map3 = HashMap::new();
    map3.insert(b"e", b"4");

    let iterators = create_iterators(vec![map1, map2, map3]);
    MergedIterator::new(iterators)
}

fn create_iterators(items: Vec<HashMap<&[u8; 1], &[u8; 1]>>) -> Vec<MemtableIterator> {
    let mut res = vec![];

    for item in items {
        let memtable = Memtable::new(1);

        for (key, val) in item {
            let _ = memtable.put(key, val);
        }

        res.push(memtable.scan(Unbounded, Unbounded));
    }

    res
}

fn create_empty_merged_iter() -> MergedIterator<MemtableIterator> {
    let iterators = create_iterators(vec![]);
    MergedIterator::new(iterators)
}

fn get_sst_entries() -> Vec<(&'static [u8], &'static [u8])> {
    vec![
        (b"b", b"-2"),
        (b"c", b"-3"),
        (b"d", b"-4"),
        (b"e", b"-5"),
        (b"f", b"6"),
        (b"g", b"7"),
        (b"h", b"8"),
        (b"i", b"9"),
        (b"j", b"10"),
        (b"k", b"11"),
        (b"l", b"12"),
    ]
}
