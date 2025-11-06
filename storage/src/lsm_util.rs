use anyhow::Result;
use std::{
    collections::{HashMap, HashSet},
    fs,
    path::Path,
    sync::Arc,
};

use crate::{FileObject, SSTable, manifest::ManifestRecord, sst::BlockCache};

type RecoveredState = (
    Vec<usize>,
    Vec<usize>,
    Vec<usize>,
    HashMap<usize, Arc<SSTable>>,
);

pub(crate) fn load_sstables(
    path: &Path,
    block_cache: Arc<BlockCache>,
    manifest_recs: Vec<ManifestRecord>,
) -> Result<RecoveredState> {
    let mut l0 = vec![];
    let mut l1 = vec![];
    let mut memtables = vec![];
    let mut sstables = HashMap::new();

    for record in manifest_recs {
        match record {
            ManifestRecord::NewMemtable(id) => {
                memtables.insert(0, id);
            }
            ManifestRecord::Flush(sst_id) => {
                l0.insert(0, sst_id);
            }
            ManifestRecord::Compaction(sst_id) => {
                // during compaction all l0 sstables are compacted to a single l1 sstable
                l0.clear();

                // we only support full compaction which means there's only one l1 sstable
                l1 = vec![sst_id]
            }
        }

        // filter out memtables flushed to l0
        let l0_set: HashSet<_> = l0.iter().collect();
        memtables.retain(|x| !l0_set.contains(x));
    }

    for l0_sst_id in &l0 {
        let sst_path = path.join(format!("{}.sst", l0_sst_id));
        let file = FileObject::open(&sst_path).expect("failed to open file");
        let sst =
            SSTable::open(*l0_sst_id, block_cache.clone(), file).expect("failed to open sstable");
        sstables.insert(sst.id, Arc::new(sst));
    }

    for l1_sst_id in &l1 {
        let sst_path = path.join(format!("{}.sst", l1_sst_id));
        let file = FileObject::open(&sst_path).expect("failed to open file");
        let sst =
            SSTable::open(*l1_sst_id, block_cache.clone(), file).expect("failed to open sstable");
        sstables.insert(sst.id, Arc::new(sst));
    }

    anyhow::Ok((memtables, l0, l1, sstables))
}

pub(crate) fn create_db_dir(path: &Path) {
    fs::create_dir_all(path).expect("failed to create db dir");
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
