use anyhow::Result;
use std::{cmp::Reverse, collections::HashMap, fs, io, path::Path, sync::Arc, time::SystemTime};

use crate::{FileObject, SSTable, sst::BlockCache};

type LoadedSstables = (Vec<usize>, HashMap<usize, Arc<SSTable>>);

fn read_dir_sorted<P: AsRef<Path>>(path: P) -> io::Result<Vec<fs::DirEntry>> {
    let mut entries: Vec<fs::DirEntry> = fs::read_dir(path)?.filter_map(Result::ok).collect();
    entries.sort_by_key(|entry| {
        Reverse(
            entry
                .metadata()
                .and_then(|m| m.modified())
                .unwrap_or(SystemTime::UNIX_EPOCH),
        )
    });
    Ok(entries)
}

pub(crate) fn load_sstables(path: &Path, block_cache: Arc<BlockCache>) -> Result<LoadedSstables> {
    let mut l0_sstables = vec![];
    let mut sstables = HashMap::new();

    for entry in read_dir_sorted(path.join("sst")).unwrap() {
        let sst_path = entry.path();
        let filename = sst_path.file_name().unwrap().to_str().unwrap();
        let sst_id = filename.rsplit_once(".").unwrap().0.parse().unwrap();

        let file = FileObject::open(sst_path.as_path()).expect("failed to open file");
        let sst = SSTable::open(sst_id, block_cache.clone(), file).expect("failed to open sstable");

        l0_sstables.push(sst.id);
        sstables.insert(sst.id, Arc::new(sst));
    }

    anyhow::Ok((l0_sstables, sstables))
}

pub(crate) fn create_db_dir(path: &Path) {
    fs::create_dir_all(path.join("sst")).expect("failed to create db dir");
}

pub fn get_entries() -> Vec<(&'static [u8], &'static [u8])> {
    vec![
        (b"a", b"1"),
        (b"b", b"2"),
        (b"c", b"3"),
        (b"d", b"4"),
        (b"e", b"5"),
        (b"f", b"6"),
        (b"g", b"7"),
        (b"h", b"8"),
        (b"i", b"9"),
        (b"j", b"10"),
        (b"k", b"11"),
        (b"l", b"12"),
    ]
}
