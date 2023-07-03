//! bs_writer provides all tools for writing bytestacks

use super::err;
/// BytestacOpendalkWriter is tool for writing the bytestack
pub struct BytestacOpendalkWriter {}

impl BytestacOpendalkWriter {
    /// put puts data, filename and meta_info to server.
    pub async fn put(
        &mut self,
        buf: Vec<u8>,
        filename: String,
        meta: Option<Vec<u8>>,
    ) -> Result<String, err::ErrorKind> {
        todo!()
    }

    /// close flush and close all writer.
    pub async fn close(&self) -> Result<(), err::ErrorKind> {
        todo!()
    }
}
