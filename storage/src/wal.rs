use anyhow::{Ok, Result};
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug)]
pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let mut open_opts = OpenOptions::new();
        open_opts.read(true).write(true).create(true);
        let file = open_opts.open(path)?;

        let buf_writer = BufWriter::new(file);
        Ok(Wal {
            file: Arc::new(Mutex::new(buf_writer)),
        })
    }

    pub fn recover(
        path: impl AsRef<Path>,
        skiplist: &SkipMap<Bytes, Bytes>,
    ) -> Result<(u16, Self)> {
        let mut open_opts = OpenOptions::new();
        let mut size = 0;
        open_opts.read(true).write(true).create(true);
        let mut file = open_opts.open(path)?;

        let mut buf = vec![];
        file.read_to_end(&mut buf)?;
        let mut buf_ptr = buf.as_slice();

        while buf_ptr.has_remaining() {
            let len = buf_ptr.get_u16();
            let key = &buf_ptr[..len as usize];
            buf_ptr.advance(len as usize);
            size += len;

            let len = buf_ptr.get_u16();
            let value = &buf_ptr[..len as usize];
            buf_ptr.advance(len as usize);
            size += len;

            skiplist.insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
        }

        let buf_writer = BufWriter::new(file);
        Ok((
            size,
            Wal {
                file: Arc::new(Mutex::new(buf_writer)),
            },
        ))
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
        let mut file = self.file.lock().unwrap();
        file.flush()?;
        file.get_mut().sync_all()?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crossbeam_skiplist::SkipMap;
    use tempfile::tempdir;

    use crate::{lsm_util::get_entries, wal::Wal};

    #[test]
    fn test_wal_recovery() {
        let temp_dir = tempdir().unwrap();
        let wal_path = temp_dir.path().join("wal");
        let wal = Wal::create(wal_path.as_path()).unwrap();

        let entries = get_entries();
        for (key, value) in &entries {
            wal.put(key, value).unwrap();
        }
        wal.sync().unwrap();

        let memtable = SkipMap::new();
        let _ = Wal::recover(wal_path.as_path(), &memtable).unwrap();
        let mut res = vec![];

        for entry in memtable.iter() {
            res.push((entry.key().to_vec(), entry.value().to_vec()));
        }

        assert_eq!(memtable.len(), res.len());
        let res_slices: Vec<(&[u8], &[u8])> = res
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
            .collect();
        assert_eq!(res_slices, entries);
    }
}
