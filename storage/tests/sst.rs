use bytes::Bytes;
use cabin_storage::SSTableBuilder;
use tempfile::NamedTempFile;

#[test]
fn test_sst() {
    let mut sst_builder = SSTableBuilder::new(32); // block size of 4 bytes

    let entries = [
        (b"a", b"1"),
        (b"b", b"2"),
        (b"c", b"3"),
        (b"d", b"4"),
        (b"e", b"5"),
        (b"f", b"6"),
        (b"g", b"7"),
        (b"h", b"8"),
        (b"i", b"5"),
        (b"j", b"6"),
        (b"k", b"7"),
        (b"l", b"8"),
    ];

    for (key, value) in entries {
        sst_builder.add(key, value);
    }

    let tmp_file = NamedTempFile::new().unwrap();
    let sst = sst_builder.build(1, tmp_file).unwrap();

    assert_eq!(sst.find_block_idx(b"a"), 0);
    assert_eq!(sst.find_block_idx(b"b"), 0);
    assert_eq!(sst.find_block_idx(b"c"), 0);
    assert_eq!(sst.find_block_idx(b"d"), 0);
    assert_eq!(sst.find_block_idx(b"e"), 1);
    assert_eq!(sst.find_block_idx(b"f"), 1);
    assert_eq!(sst.find_block_idx(b"g"), 1);
    assert_eq!(sst.find_block_idx(b"h"), 1);
    assert_eq!(sst.find_block_idx(b"i"), 2);
    assert_eq!(sst.find_block_idx(b"j"), 2);
    assert_eq!(sst.find_block_idx(b"k"), 2);
    assert_eq!(sst.find_block_idx(b"l"), 2);
}
