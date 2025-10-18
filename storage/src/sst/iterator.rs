use std::sync::Arc;

use anyhow::{Ok, Result};

use crate::{
    SSTable,
    block::iterator::{self, BlockIterator},
    common::iterator::StorageIterator,
};

pub struct SSTableIterator {
    table: Arc<SSTable>,
    block_iter: BlockIterator,
    block_idx: usize,
}

impl SSTableIterator {
    pub fn create_and_seek_to_first(table: Arc<SSTable>) -> Result<Self> {
        let block = table.read_block(0)?;

        Ok(SSTableIterator {
            table: table.clone(),
            block_iter: BlockIterator::create_and_seek_to_first(block),
            block_idx: 0,
        })
    }

    pub fn create_and_seek_to_key(table: Arc<SSTable>, key: &[u8]) -> Result<Self> {
        let block_idx = table.find_block_idx(key);
        let block = table.read_block(block_idx)?;

        Ok(SSTableIterator {
            table: table.clone(),
            block_iter: BlockIterator::create_and_seek_to_key(block, key),
            block_idx,
        })
    }

    pub fn seek_to_first(&mut self) -> Result<()> {
        let _ = self.block_iter.seek_to_first();
        Ok(())
    }

    pub fn seek_to_key(&mut self, key: &[u8]) -> Result<()> {
        let block_idx = self.table.find_block_idx(key);
        let block = self.table.read_block(block_idx)?;
        let block_iter = BlockIterator::create_and_seek_to_key(block, key);

        self.block_iter = block_iter;
        self.block_idx = block_idx;
        Ok(())
    }
}

impl StorageIterator for SSTableIterator {
    fn key(&self) -> &[u8] {
        self.block_iter.key()
    }

    fn value(&self) -> &[u8] {
        self.block_iter.value()
    }

    fn is_valid(&self) -> bool {
        self.block_iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.block_iter.next();

        if !self.block_iter.is_valid() {
            self.block_idx += 1;
            if self.block_idx < self.table.block_index.len() {
                let block = self.table.read_block(self.block_idx)?;
                self.block_iter = BlockIterator::create_and_seek_to_first(block)
            }
        }

        Ok(())
    }
}
