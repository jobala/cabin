use std::sync::Arc;

use bytes::Buf;

use crate::block::Block;

pub struct BlockIterator {
    block: Arc<Block>,
    key: Vec<u8>,
    value: Vec<u8>,
    idx: usize,
    first_key: Vec<u8>,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: vec![],
            value: vec![],
            idx: 0,
            first_key: vec![],
        }
    }

    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut block_iter = BlockIterator::new(block);
        block_iter
            .seek_to(0)
            .expect("failed to seek to first index in BlockIterator");

        block_iter
    }

    pub fn create_and_seek_to_key(block: Arc<Block>, key: &[u8]) -> Self {
        let mut block_iter = BlockIterator::create_and_seek_to_first(block);
        block_iter.seek_to_key(key);

        block_iter
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn value(&self) -> &[u8] {
        &self.value
    }

    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    pub fn seek_to_first(&mut self) -> Result<(), String> {
        self.seek_to(0)?;
        self.first_key = self.key().to_vec();

        Ok(())
    }

    pub fn seek_to(&mut self, idx: usize) -> Result<(), String> {
        if idx >= self.block.offsets.len() {
            return Err(format!(
                "idx {} out of bounds (max: {})",
                idx,
                self.block.offsets.len()
            ));
        }

        let pos = self.block.offsets[idx] as usize;
        let mut key_len = &self.block.data[pos..pos + 2];
        let key_len = key_len.get_u16();

        let key_start = pos + 2;
        let key_end = key_start + key_len as usize;
        let key = &self.block.data[key_start..key_end];

        let mut value_len = &self.block.data[key_end..key_end + 2];
        let value_len = value_len.get_u16();

        let value_start = key_end + 2;
        let value_end = value_start + value_len as usize;
        let value = &self.block.data[value_start..value_end];

        self.key = key.to_vec();
        self.value = value.to_vec();
        Ok(())
    }

    pub fn next(&mut self) {
        if self.idx < self.block.offsets.len() - 1 {
            self.idx += 1;
            self.seek_to(self.idx)
                .expect("failed to seek to next index in BlockIterator");
        } else {
            self.key.clear();
            self.value.clear();
        }
    }

    pub fn seek_to_key(&mut self, key: &[u8]) {
        while self.is_valid() && self.key() < key {
            self.next();
        }
    }
}

#[cfg(test)]
mod test {

    use std::str::from_utf8;

    use super::*;
    use crate::block::builder::BlockBuilder;
    #[test]
    fn test_block_iterator() {
        let entries: Vec<(u8, String)> = vec![
            (1, "Japheth".to_string()),
            (2, "Obala".to_string()),
            (3, "John".to_string()),
            (4, "Doe".to_string()),
            (5, "Jane".to_string()),
            (6, "Doe".to_string()),
        ];

        let block = Arc::new(create_block(&entries));
        let mut block_iterator = BlockIterator::create_and_seek_to_first(block);

        let mut keys_res: Vec<u8> = Vec::new();
        let mut values_res: Vec<String> = Vec::new();

        while block_iterator.is_valid() {
            let key = block_iterator.key();
            let value = block_iterator.value();

            keys_res.push(key[0]);
            values_res.push(from_utf8(value).unwrap().to_string());

            block_iterator.next();
        }

        assert_eq!(
            keys_res,
            entries.iter().map(|entry| entry.0).collect::<Vec<u8>>()
        );

        assert_eq!(
            values_res,
            entries
                .iter()
                .map(|entry| entry.1.clone())
                .collect::<Vec<String>>()
        )
    }

    #[test]
    fn test_block_iterator_from_key() {
        let entries: Vec<(u8, String)> = vec![
            (1, "Japheth".to_string()),
            (2, "Obala".to_string()),
            (3, "John".to_string()),
            (4, "Doe".to_string()),
            (5, "Jane".to_string()),
            (6, "Doe".to_string()),
            (255, "Wantam".to_string()),
        ];

        let block = Arc::new(create_block(&entries));
        let mut block_iterator = BlockIterator::create_and_seek_to_key(block, &[4]);

        let mut keys_res: Vec<u8> = Vec::new();
        let mut values_res: Vec<String> = Vec::new();

        while block_iterator.is_valid() {
            let key = block_iterator.key();
            let value = block_iterator.value();

            keys_res.push(key[0]);
            values_res.push(from_utf8(value).unwrap().to_string());

            block_iterator.next();
        }

        assert!(!block_iterator.is_valid());
        assert_eq!(keys_res, vec![4, 5, 6, 255]);
        assert_eq!(
            values_res,
            vec![
                "Doe".to_string(),
                "Jane".to_string(),
                "Doe".to_string(),
                "Wantam".to_string()
            ]
        )
    }

    fn create_block(entries: &Vec<(u8, String)>) -> Block {
        let mut block_builder = BlockBuilder::new(4096);

        for (id, value) in entries {
            block_builder.add(&id.to_be_bytes()[..], value.as_bytes());
        }

        block_builder.build()
    }
}
