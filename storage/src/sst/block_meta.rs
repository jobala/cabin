use std::io::Read;

use bytes::{Buf, BufMut};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    pub offset: usize,
    pub first_key: Vec<u8>,
    pub last_key: Vec<u8>,
}

impl BlockMeta {
    pub fn encode_block_meta(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
        for block in block_meta {
            buf.put_u32(block.offset as u32);
            buf.put_u16(block.first_key.len() as u16);
            buf.extend_from_slice(&block.first_key);
            buf.put_u16(block.last_key.len() as u16);
            buf.extend_from_slice(&block.last_key);
        }
    }

    pub fn decode_block_meta(mut buf: impl Buf) -> Vec<BlockMeta> {
        let mut res = vec![];

        while buf.remaining() > 0 {
            let block_offset = buf.get_u32();
            let first_key_len = buf.get_u16();
            let first_key = buf.copy_to_bytes(first_key_len as usize).to_vec();
            let last_key_len = buf.get_u16();
            let last_key = buf.copy_to_bytes(last_key_len as usize).to_vec();

            res.push(BlockMeta {
                offset: block_offset as usize,
                first_key,
                last_key,
            });
        }

        res
    }
}
