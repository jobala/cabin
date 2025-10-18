use std::mem;
use std::path::Path;

use anyhow::{Ok, Result};
use bytes::BufMut;

use crate::block::builder::BlockBuilder;
use crate::sst::block_meta::BlockMeta;
use crate::sst::file::FileObject;
use crate::sst::table::SSTable;

#[derive(Debug)]
pub struct SSTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
}

impl SSTableBuilder {
    pub fn new(block_size: usize) -> Self {
        SSTableBuilder {
            builder: BlockBuilder::new(block_size),
            first_key: vec![],
            last_key: vec![],
            data: vec![],
            meta: vec![],
            block_size,
        }
    }

    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        if self.first_key.is_empty() {
            self.first_key = key.to_vec();
        }

        if self.builder.add(key, value) {
            self.last_key = key.to_vec();
            return;
        }

        self.finalize_block();
        self.builder.add(key, value);
        self.last_key = key.to_vec();
    }

    fn finalize_block(&mut self) {
        let block_meta = BlockMeta {
            offset: self.data.len(),
            first_key: self.builder.first_key.clone(),
            last_key: self.last_key.clone(),
        };

        let builder = mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        let block = builder.build();
        self.data.extend(&block.encode());
        self.meta.push(block_meta);
    }

    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    pub fn build(&mut self, id: usize, path: impl AsRef<Path>) -> Result<SSTable> {
        self.finalize_block();

        let meta_offset = self.data.len();
        let mut buf = self.data.clone();
        BlockMeta::encode_block_meta(&self.meta, &mut buf);
        buf.put_u32(meta_offset as u32);
        buf.extend(&self.first_key);
        buf.put_u16(self.first_key.len() as u16);
        buf.extend(&self.last_key);
        buf.put_u16(self.last_key.len() as u16);
        let file = FileObject::create(path.as_ref(), buf)?;

        Ok(SSTable {
            id,
            file,
            first_key: self.first_key.to_vec(),
            last_key: self.last_key.to_vec(),
            block_index: self.meta.clone(),
            block_index_offset: meta_offset,
            max_ts: 0,
        })
    }
}
