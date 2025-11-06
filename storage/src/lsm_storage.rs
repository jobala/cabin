use std::ops::Bound;
use std::sync::Arc;
use std::thread::JoinHandle;

use anyhow::Result;
use bytes::Bytes;

use crate::common::errors::KeyNotFound;
use crate::iterators::lsm_iterator::LsmIterator;
use crate::lsm_storage_inner;
use crate::{Config, lsm_storage_inner::StorageInner};

pub struct Storage {
    inner: Arc<StorageInner>,
    compacter: JoinHandle<()>,
    flusher: JoinHandle<()>,
}

pub fn new(config: Config) -> Result<Arc<Storage>> {
    let inner = lsm_storage_inner::new(config)?;
    let compacter = inner.spawn_compacter();
    let flusher = inner.spawn_flusher();

    Ok(Arc::new(Storage {
        inner,
        compacter,
        flusher,
    }))
}

impl Storage {
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn get(&self, key: &[u8]) -> Result<Bytes, KeyNotFound> {
        self.inner.get(key)
    }

    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<LsmIterator> {
        self.inner.scan(lower, upper)
    }

    pub fn close(&self) -> Result<()> {
        self.inner.sync()?;
        Ok(())
    }
}
