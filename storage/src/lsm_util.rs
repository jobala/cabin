use std::{collections::HashMap, fs, path::Path, sync::Arc};

use anyhow::{Result, anyhow};

use crate::{FileObject, SSTable, sst::BlockCache};

pub(crate) fn load_sstables(
    path: &Path,
    block_cache: Arc<BlockCache>,
) -> Result<(Vec<usize>, HashMap<usize, Arc<SSTable>>)> {
    let mut l0_sstables = vec![];
    let mut sstables = HashMap::new();

    for entry in fs::read_dir(path.join("sst")).unwrap() {
        match entry {
            Ok(dir_entry) => {
                let sst_path = dir_entry.path();
                let split_path = sst_path
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .split(".")
                    .collect::<Vec<&str>>();

                let sst_id = split_path.first().unwrap().parse().unwrap();
                let file = FileObject::open(sst_path.as_path()).expect("failed to open file");
                let sst = SSTable::open(sst_id, block_cache.clone(), file)
                    .expect("failed to open sstable");

                l0_sstables.push(sst.id);
                sstables.insert(sst.id, Arc::new(sst));
            }
            Err(err) => return Err(anyhow!("{:?}", err)),
        }
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
