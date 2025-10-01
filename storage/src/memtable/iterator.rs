use std::{ops::Bound, sync::Arc};

use bytes::Bytes;
use crossbeam_skiplist::{
    SkipMap,
    map::{Entry, Range},
};
use ouroboros::self_referencing;

#[self_referencing]
pub struct MemtableIterator {
    pub(crate) map: Arc<SkipMap<Bytes, Bytes>>,
    pub(crate) item: (Bytes, Bytes),

    #[borrows(map)]
    #[not_covariant]
    pub(crate) iter: Range<'this, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>,
}

pub trait StorageIterator {
    fn value(&self) -> &[u8];
    fn key(&self) -> &[u8];
    fn is_valid(&self) -> bool;
    fn next(&mut self) -> anyhow::Result<()>;
}

impl StorageIterator for MemtableIterator {
    fn value(&self) -> &[u8] {
        &self.borrow_item().1[..]
    }

    fn key(&self) -> &[u8] {
        &self.borrow_item().0[..]
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> anyhow::Result<()> {
        let entry = self.with_iter_mut(|iter| MemtableIterator::entry_to_item(iter.next()));
        self.with_mut(|x| *x.item = entry);
        Ok(())
    }
}

impl MemtableIterator {
    pub fn create(
        skip_map: Arc<SkipMap<Bytes, Bytes>>,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Self {
        let (lower, upper) = (map_bound(lower), map_bound(upper));
        let mut iter = MemtableIteratorBuilder {
            map: skip_map,
            iter_builder: |map| map.range((lower, upper)),
            item: (Bytes::new(), Bytes::new()),
        }
        .build();
        iter.next().unwrap();

        iter
    }

    fn entry_to_item(entry: Option<Entry<'_, Bytes, Bytes>>) -> (Bytes, Bytes) {
        entry
            .map(|x| (x.key().clone(), x.value().clone()))
            .unwrap_or_else(|| (Bytes::from_static(&[]), Bytes::from_static(&[])))
    }
}

pub(crate) fn map_bound(bound: Bound<&[u8]>) -> Bound<Bytes> {
    match bound {
        Bound::Included(x) => Bound::Included(Bytes::copy_from_slice(x)),
        Bound::Excluded(x) => Bound::Excluded(Bytes::copy_from_slice(x)),
        Bound::Unbounded => Bound::Unbounded,
    }
}
