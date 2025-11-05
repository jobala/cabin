use anyhow::{Ok, Result};
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let mut open_opts = OpenOptions::new();
        open_opts.read(true).write(true).create(true);
        let mut file = open_opts.open(path)?;

        let buf_writer = BufWriter::new(file);
        Ok(Wal {
            file: Arc::new(Mutex::new(buf_writer)),
        })
    }

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<Bytes, Bytes>) -> Result<Self> {
        let mut open_opts = OpenOptions::new();
        open_opts.read(true).write(true).create(true);
        let mut file = open_opts.open(path)?;

        let mut buf = vec![];
        file.read_to_end(&mut buf)?;
        let mut buf_ptr = buf.as_slice();

        while buf_ptr.has_remaining() {
            let len = buf_ptr.get_u16();
            let key = &buf_ptr[..len as usize];
            buf_ptr.advance(len as usize);

            let len = buf_ptr.get_u16();
            let value = &buf_ptr[..len as usize];
            buf_ptr.advance(len as usize);

            skiplist.insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
        }

        let buf_writer = BufWriter::new(file);
        Ok(Wal {
            file: Arc::new(Mutex::new(buf_writer)),
        })
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut file = self.file.lock().unwrap();
        let mut buf = vec![];
        buf.put_u16(key.len() as u16);
        buf.extend(key);
        buf.put_u16(value.len() as u16);
        buf.extend(value);

        file.write_all(&buf)?;
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut guard = self.file.lock().unwrap();
        Ok(guard.flush()?)
    }
}
