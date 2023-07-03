//! bs_reader provides all tools for reading bytestacks

use crate::types::{IndexRecord, MetaRecord, Stack};
/// BytestackReader is tool for reading the bytestack
pub struct BytestackOpendalReader {}

/// BytestackOpendalIterator is helper to iterator index items and meta items in opendal way;
pub struct BytestackOpendalIterator {}

impl BytestackOpendalIterator {
    /// next work like iterator but async version
    /// return (ir, mr) if there is, and return None if there is not.
    pub async fn next(&self) -> Option<(IndexRecord, MetaRecord)> {
        todo!()
    }
}

/// Fetcher is return by batch_fetch api
pub struct OpendalFetcher {}

impl OpendalFetcher {
    /// do_fetch fetch the data for user
    pub async fn do_fetch()->Result<Vec<u8>, opendal::Error>{
        todo!()
    }
}

impl BytestackOpendalReader {
    /// list return all stack(stack_id only) under this path
    pub async fn list(&self) -> Result<Vec<u64>, opendal::Error> {
        todo!()
    }

    /// list_al return all stack(full stack info) under this path
    pub async fn list_al(&self) -> Result<Vec<Stack>, opendal::Error> {
        todo!()
    }

    /// list_stack return all record(index_id) in giving stack_id.
    /// return with list of format!({:x}{:08x}, data_offset, cookie) which can be used to fetch single or batch data.
    pub async fn list_stack(&self, stack_id: u64) -> Result<Vec<String>, opendal::Error> {
        todo!()
    }

    /// list_stack_al_iter return BytestackOpendalIterator which work like an iterator for IndexRecord and MetaRecord
    pub async fn list_stack_al_iter(&self, stack_id: u64) -> Result<BytestackOpendalIterator, opendal::Error> {
        todo!()
    }

    /// fetch data by index_id
    pub async fn fetch(
        &self,
        index_id: String,
        check_crc: bool,
    ) -> Result<Vec<u8>, opendal::Error> {
        todo!()
    }

    /// batch_fetch can fetch data for giving a batch of index_id
    pub async fn batch_fetch(
        &self,
        index_ids: Vec<String>,
        check_crc: bool,
    ) -> Result<Vec<OpendalFetcher>, opendal::Error> {
        todo!()
    }
}
