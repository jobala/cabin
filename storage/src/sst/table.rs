use std::{cmp::Ordering, sync::Arc};

use anyhow::{Ok, Result, anyhow};
use bytes::{Buf, Bytes};
use moka::sync::Cache;

use crate::{
    block::{Block, SIZEOF_U16},
    sst::{block_meta::BlockMeta, file::FileObject},
};

pub type BlockCache = Cache<(usize, usize), Arc<Block>>;

#[derive(Debug)]
pub struct SSTable {
    pub(crate) file: FileObject,
    pub(crate) block_index: Vec<BlockMeta>,
    pub(crate) block_index_offset: usize,
    pub(crate) id: usize,
    pub(crate) first_key: Vec<u8>,
    pub(crate) last_key: Vec<u8>,
    pub(crate) max_ts: u64,
    pub(crate) block_cache: Arc<BlockCache>,
}

impl SSTable {
    pub fn open(id: usize, block_cache: Arc<BlockCache>, file: FileObject) -> Result<Self> {
        let last_key_size_offset = file.size() - SIZEOF_U16 as u64;
        let last_key_size = file.read(last_key_size_offset, 2).unwrap();
        let last_key_size = (&last_key_size[..]).get_u16() as u64;
        let last_key = file.read(last_key_size_offset - last_key_size, last_key_size)?;

        let first_key_size_offset = last_key_size_offset - last_key_size - SIZEOF_U16 as u64;
        let first_key_size = file.read(first_key_size_offset, 2)?;
        let first_key_size = (&first_key_size[..]).get_u16() as u64;
        let first_key = file.read(
            file.size() - (4 + last_key_size + first_key_size),
            first_key_size,
        )?;

        // block_meta_offset is 4 bytes wide
        let block_index_offset = file.read(first_key_size_offset - first_key_size - 4, 4)?;
        let block_index_offset = (&block_index_offset[..]).get_u32() as usize;
        let block_index_len =
            (first_key_size_offset - first_key_size - 4) - block_index_offset as u64;
        let block_meta = file.read(block_index_offset as u64, block_index_len)?;

        Ok(SSTable {
            id,
            file,
            block_index: BlockMeta::decode_block_meta(Bytes::copy_from_slice(&block_meta)),
            block_index_offset,
            first_key,
            last_key,
            max_ts: 0,
            block_cache,
        })
    }

    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let this_block_offset = self.block_index[block_idx].offset;
        let next_block_offset = self
            .block_index
            .get(block_idx + 1)
            .map_or(self.block_index_offset, |meta| meta.offset);
        let block_size = (next_block_offset - this_block_offset) as u64;

        let block_data = self
            .file
            .read(this_block_offset as u64, block_size)
            .unwrap();
        let block = Block::decode(&block_data);

        Ok(Arc::new(block))
    }

    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        self.block_cache
            .try_get_with((self.id, block_idx), || self.read_block(block_idx))
            .map_err(|e| anyhow!("{}", e))
    }

    pub fn find_block_idx(&self, key: &[u8]) -> usize {
        let mut l = 0usize;
        let mut r = self.block_index.len() - 1;

        while l < r {
            let mid = (l + r) / 2;
            let mid_block = &self.block_index[mid];

            match key.cmp(&mid_block.last_key) {
                Ordering::Less | Ordering::Equal => r = mid,
                _ => l = mid + 1,
            }
        }

        l
    }

    pub fn num_of_blocks(&self) -> usize {
        self.block_index.len()
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
