use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;

use crate::{
    block::Block,
    sst::{block_meta::BlockMeta, file::FileObject},
};

pub struct SSTable {
    pub(crate) file: FileObject,
    pub(crate) block_meta: Vec<BlockMeta>,
    pub(crate) block_meta_offset: usize,
    pub(crate) id: usize,
    pub(crate) first_key: Vec<u8>,
    pub(crate) last_key: Vec<u8>,
    pub(crate) max_ts: u64,
}

impl SSTable {
    pub fn open(id: usize, file: FileObject) -> Result<Self> {
        unimplemented!()
    }

    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: Vec<u8>,
        last_key: Vec<u8>,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            first_key,
            last_key,
            max_ts: 0,
        }
    }

    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        unimplemented!()
    }

    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        todo!()
    }

    pub fn find_block_idx(&self, key: &[u8]) -> usize {
        unimplemented!()
    }

    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> Bytes {
        Bytes::copy_from_slice(&self.first_key[..])
    }

    pub fn last_key(&self) -> Bytes {
        Bytes::copy_from_slice(&self.last_key[..])
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }
}
