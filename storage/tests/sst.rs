use cabin_storage::{
    FileObject, SSTable, SSTableBuilder, SSTableIterator, common::iterator::StorageIterator,
};
use moka::sync::Cache;

use std::sync::Arc;
use tempfile::NamedTempFile;

#[test]
fn test_block_search_in_sst() {
    let mut sst_builder = SSTableBuilder::new(32); // block size of 32 bytes

    for (key, value) in get_sst_entries() {
        sst_builder.add(key, value);
    }

    let block_cache = Arc::new(Cache::new(1));
    let tmp_file = NamedTempFile::new().unwrap();
    let sst = sst_builder.build(1, block_cache, tmp_file).unwrap();

    assert_eq!(sst.find_block_idx(b"a"), 0);
    assert_eq!(sst.find_block_idx(b"b"), 0);
    assert_eq!(sst.find_block_idx(b"c"), 0);
    assert_eq!(sst.find_block_idx(b"d"), 0);
    assert_eq!(sst.find_block_idx(b"e"), 1);
    assert_eq!(sst.find_block_idx(b"f"), 1);
    assert_eq!(sst.find_block_idx(b"h"), 1);
    assert_eq!(sst.find_block_idx(b"i"), 1);
    assert_eq!(sst.find_block_idx(b"j"), 2);
    assert_eq!(sst.find_block_idx(b"k"), 2);
    assert_eq!(sst.find_block_idx(b"l"), 2);
}

#[test]
fn test_reading_sst_from_file() {
    let mut sst_builder = SSTableBuilder::new(32); // block size of 32 bytes
    let block_cache = Arc::new(Cache::new(1));

    for (key, value) in get_sst_entries() {
        sst_builder.add(key, value);
    }

    let tmp_file = NamedTempFile::new().unwrap();
    let _sst = sst_builder
        .build(1, block_cache.clone(), tmp_file.path())
        .unwrap();

    let file_obj = FileObject::open(tmp_file.path()).unwrap();
    let restored_sst = SSTable::open(1, block_cache.clone(), file_obj).unwrap();

    assert_eq!(restored_sst.find_block_idx(b"a"), 0);
    assert_eq!(restored_sst.find_block_idx(b"b"), 0);
    assert_eq!(restored_sst.find_block_idx(b"c"), 0);
    assert_eq!(restored_sst.find_block_idx(b"d"), 0);
    assert_eq!(restored_sst.find_block_idx(b"e"), 1);
    assert_eq!(restored_sst.find_block_idx(b"f"), 1);
    assert_eq!(restored_sst.find_block_idx(b"h"), 1);
    assert_eq!(restored_sst.find_block_idx(b"i"), 1);
    assert_eq!(restored_sst.find_block_idx(b"j"), 2);
    assert_eq!(restored_sst.find_block_idx(b"k"), 2);
    assert_eq!(restored_sst.find_block_idx(b"l"), 2);
}

#[test]
fn test_sst_iterator() {
    let mut sst_builder = SSTableBuilder::new(32); // block size of 32 bytes
    let block_cache = Arc::new(Cache::new(1));

    for (key, value) in get_sst_entries() {
        sst_builder.add(key, value);
    }

    let tmp_file = NamedTempFile::new().unwrap();
    let _sst = sst_builder
        .build(1, block_cache.clone(), tmp_file.path())
        .unwrap();

    let file_obj = FileObject::open(tmp_file.path()).unwrap();
    let restored_sst = Arc::new(SSTable::open(1, block_cache.clone(), file_obj).unwrap());

    let mut sst_iter = SSTableIterator::create_and_seek_to_first(restored_sst).unwrap();
    let mut res = vec![];

    while sst_iter.is_valid() {
        res.push((sst_iter.key().to_vec(), sst_iter.value().to_vec()));
        let _ = sst_iter.next();
    }

    let res = res
        .iter()
        .map(|(k, v)| (k.as_slice(), v.as_slice()))
        .collect::<Vec<(&[u8], &[u8])>>();

    assert_eq!(res, get_sst_entries())
}

#[test]
fn test_seek_to_key() {
    let mut sst_builder = SSTableBuilder::new(32); // block size of 32 bytes
    let block_cache = Arc::new(Cache::new(1));

    for (key, value) in get_sst_entries() {
        sst_builder.add(key, value);
    }

    let tmp_file = NamedTempFile::new().unwrap();
    let _sst = sst_builder
        .build(1, block_cache.clone(), tmp_file.path())
        .unwrap();

    let file_obj = FileObject::open(tmp_file.path()).unwrap();
    let restored_sst = Arc::new(SSTable::open(1, block_cache.clone(), file_obj).unwrap());

    let mut sst_iter = SSTableIterator::create_and_seek_to_first(restored_sst).unwrap();
    sst_iter.seek_to_key(b"i").unwrap();

    assert_eq!(sst_iter.key(), b"i");
    assert_eq!(sst_iter.value(), b"9")
}

#[test]
fn test_seek_to_invalid_key() {
    let mut sst_builder = SSTableBuilder::new(32); // block size of 32 bytes
    let block_cache = Arc::new(Cache::new(1));

    for (key, value) in get_sst_entries() {
        sst_builder.add(key, value);
    }

    let tmp_file = NamedTempFile::new().unwrap();
    let _sst = sst_builder
        .build(1, block_cache.clone(), tmp_file.path())
        .unwrap();

    let file_obj = FileObject::open(tmp_file.path()).unwrap();
    let restored_sst = Arc::new(SSTable::open(1, block_cache.clone(), file_obj).unwrap());

    let mut sst_iter = SSTableIterator::create_and_seek_to_key(restored_sst, b"g").unwrap();
    let mut res = vec![];

    while sst_iter.is_valid() {
        res.push((sst_iter.key().to_vec(), sst_iter.value().to_vec()));
        let _ = sst_iter.next();
    }

    let res = res
        .iter()
        .map(|(k, v)| (k.as_slice(), v.as_slice()))
        .collect::<Vec<(&[u8], &[u8])>>();

    assert_eq!(res, get_sst_entries()[6..])
}

fn get_sst_entries() -> Vec<(&'static [u8], &'static [u8])> {
    vec![
        (b"a", b"1"),
        (b"b", b"2"),
        (b"c", b"3"),
        (b"d", b"4"),
        (b"e", b"5"),
        (b"f", b"6"),
        (b"h", b"8"),
        (b"i", b"9"),
        (b"j", b"10"),
        (b"k", b"11"),
        (b"l", b"12"),
    ]
}
