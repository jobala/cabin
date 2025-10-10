use bytes::BufMut;

use crate::block::{Block, SIZEOF_U16};

pub struct BlockBuilder {
    data: Vec<u8>,
    offsets: Vec<u16>,
    block_size: usize,
    first_key: Vec<u8>,
}

impl BlockBuilder {
    pub fn new(block_size: usize) -> Self {
        BlockBuilder {
            data: vec![],
            offsets: vec![],
            block_size,
            first_key: vec![],
        }
    }

    pub fn add(&mut self, key: &[u8], value: &[u8]) -> bool {
        let new_entry_size = (2 * SIZEOF_U16) + key.len() + value.len();

        if self.data.len() + new_entry_size + (self.offsets.len() + 1) * SIZEOF_U16
            > self.block_size
        {
            return false;
        }

        if self.offsets.is_empty() {
            self.first_key.extend_from_slice(key);
        }
        self.offsets.push(self.data.len() as u16);

        self.data.put_u16(key.len() as u16);
        self.data.extend_from_slice(key);
        self.data.put_u16(value.len() as u16);
        self.data.extend_from_slice(value);

        true
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
