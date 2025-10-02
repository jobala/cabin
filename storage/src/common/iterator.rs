pub trait StorageIterator {
    fn value(&self) -> &[u8];
    fn key(&self) -> &[u8];
    fn is_valid(&self) -> bool;
    fn next(&mut self) -> anyhow::Result<()>;
}
