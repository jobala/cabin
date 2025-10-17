use cabin_storage::common::iterator::StorageIterator;
use cabin_storage::Config;
use std::ops::Bound::Unbounded;

#[test]
fn get_returns_latest_entry() {
    let config = Config { sst_size: 2 };
    let storage = cabin_storage::new(config);
    let entries = vec![
        (b"age", b"20"),
        (b"age", b"21"),
        (b"age", b"22"),
        (b"age", b"23"),
    ];

    for (k, v) in entries {
        let _ = storage.put(k, v);
    }

    let res = &storage.get(b"age").unwrap()[..];
    assert_eq!(res, b"23");
}

#[test]
fn can_read_frozen_memtable() {
    let config = Config { sst_size: 2 };
    let storage = cabin_storage::new(config);
    let entries = vec![(b"1", b"20"), (b"2", b"21"), (b"3", b"22"), (b"4", b"23")];

    for (k, v) in entries {
        let _ = storage.put(k, v);
    }

    let res = &storage.get(b"1").unwrap()[..];
    assert_eq!(res, b"20");
}

#[test]
#[should_panic]
fn get_invalid_key() {
    let config = Config { sst_size: 2 };
    let storage = cabin_storage::new(config);

    storage.get(b"1").unwrap();
}

#[test]
fn scan_items() {
    let config = Config { sst_size: 10 };
    let storage = cabin_storage::new(config);
    let entries = vec![
        (b"e", b"4"),
        (b"a", b"1"),
        (b"b", b"2"),
        (b"c", b"3"),
        (b"c", b"4"),
        (b"d", b"5"),
    ];

    for (k, v) in entries {
        let _ = storage.put(k, v);
    }

    // delete b
    let _ = storage.put(b"b", b"");

    let mut expected_keys = vec![];
    let mut expected_values = vec![];

    let mut iter = storage.scan(Unbounded, Unbounded);
    while iter.is_valid() {
        expected_keys.push(iter.key().to_vec());
        expected_values.push(iter.value().to_vec());

        let _ = iter.next();
    }

    assert_eq!(expected_keys, vec![b"a", b"c", b"d", b"e",]);
    assert_eq!(expected_values, vec![b"1", b"4", b"5", b"4",]);
}

#[test]
#[ignore]
fn test_delete_non_existent_key() {
    todo!()
}
