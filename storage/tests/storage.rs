use std::sync::Arc;

use cabin_storage::Config;

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
#[ignore]
fn test_delete_non_existent_key() {
    todo!()
}
