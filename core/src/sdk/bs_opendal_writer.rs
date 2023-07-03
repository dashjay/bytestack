//! bs_writer provides all tools for writing bytestacks

use super::err::{CustomError, ErrorKind};
use crate::types::{
    data::{DataMagicHeader, DataRecord},
    index::{IndexMagicHeader, IndexRecord},
    meta::{MetaMagicHeader, MetaRecord},
    DataRecordHeader,
};
use crc::{Crc, CRC_32_ISCSI};
pub const CASTAGNOLI: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);
use bincode;

use crate::utils;
use opendal::Operator;
use rand::rngs::ThreadRng;
use rand::Rng;
use serde_json;

use std::sync::Mutex;

use opendal::Writer;

const _MAX_DATA_BYTES: u64 = 10 * 1024 * 1024 * 1024;

/// InnnerWriter is the real one who write data.
/// User may write data all the timem but we divided billions of data by every 10GB
struct InnerWriter {
    data_offset: u64,
    meta_offset: u64,
    rng: ThreadRng,
    stack_id: u64,
    _current_index_writer: Writer,
    _current_meta_writer: Writer,
    _current_data_writer: Writer,
}

impl InnerWriter {
    async fn close(mut self) -> Result<(), ErrorKind> {
        if let Err(err) = self._current_data_writer.close().await {
            return Err(ErrorKind::CloseError(CustomError::new(err.to_string())));
        }
        if let Err(err) = self._current_meta_writer.close().await {
            return Err(ErrorKind::CloseError(CustomError::new(err.to_string())));
        }
        if let Err(err) = self._current_index_writer.close().await {
            return Err(ErrorKind::CloseError(CustomError::new(err.to_string())));
        }
        Ok(())
    }

    async fn write_index(&mut self, ir: IndexRecord) -> Result<usize, ErrorKind> {
        let data_bytes = bincode::serialize(&ir).unwrap();
        let index_bytes_length = data_bytes.len();
        match self._current_index_writer.write(data_bytes).await {
            Ok(_) => return Ok(index_bytes_length),
            Err(err) => return Err(ErrorKind::IOError(CustomError::new(err.to_string()))),
        }
    }
    async fn write_meta(&mut self, mr: MetaRecord) -> Result<usize, ErrorKind> {
        let mut data_bytes = serde_json::to_vec(&mr).unwrap();
        data_bytes.push(b'\n');
        let meta_bytes_length = data_bytes.len();
        match self._current_meta_writer.write(data_bytes).await {
            Ok(_) => return Ok(meta_bytes_length),
            Err(err) => return Err(ErrorKind::IOError(CustomError::new(err.to_string()))),
        }
    }
    async fn write_data(&mut self, dr: DataRecord) -> Result<usize, ErrorKind> {
        let data_bytes = bincode::serialize(&dr.header).unwrap();
        let data_bytes_length = dr.size();
        match self._current_data_writer.write(data_bytes).await {
            Ok(_) => {}
            Err(err) => return Err(ErrorKind::IOError(CustomError::new(err.to_string()))),
        }
        match self._current_data_writer.write(dr.data).await {
            Ok(_) => {}
            Err(err) => return Err(ErrorKind::IOError(CustomError::new(err.to_string()))),
        }
        match self._current_data_writer.write(dr.padding).await {
            Ok(_) => Ok(data_bytes_length),
            Err(err) => return Err(ErrorKind::IOError(CustomError::new(err.to_string()))),
        }
    }

    async fn write(
        &mut self,
        buf: Vec<u8>,
        filename: String,
        meta: Option<Vec<u8>>,
    ) -> Result<String, ErrorKind> {
        let meta = match meta {
            Some(meta) => meta,
            None => Vec::new(),
        };
        let crc_sum = CASTAGNOLI.checksum(&buf);
        let cookie: u32 = self.rng.gen();

        let mr = MetaRecord::new(
            utils::current_time(),
            self.data_offset,
            cookie,
            buf.len() as u32,
            filename,
            meta,
        );
        let mr_size = mr.size();
        let ir = IndexRecord::new(
            cookie,
            self.data_offset,
            buf.len() as u32,
            self.meta_offset,
            mr_size as u32,
        );
        let index_id = ir.index_id();
        let dr = DataRecord::new(cookie, buf.len() as u32, crc_sum, buf);

        match self.write_index(ir).await {
            Ok(_) => {}
            Err(e) => return Err(e),
        }
        match self.write_meta(mr).await {
            Ok(n) => {
                assert!(n == mr_size, "meta size mismatch");
                self.meta_offset += n as u64;
            }
            Err(e) => return Err(e),
        }
        match self.write_data(dr).await {
            Ok(n) => {
                self.data_offset += n as u64;
            }
            Err(e) => return Err(e),
        }

        Ok(format!("{},{}", self.stack_id, index_id))
    }
}

/// BytestackOpendalWriter is tool for writing the bytestack
pub struct BytestackOpendalWriter {
    id: u64,
    operator: Operator,
    prefix: String,
    inner_writer: Mutex<Option<InnerWriter>>,
}

impl BytestackOpendalWriter {
    pub fn new(operator: Operator, prefix: String) -> Self {
        BytestackOpendalWriter {
            id: 1,
            operator: operator,
            prefix: prefix,
            inner_writer: Mutex::<Option<InnerWriter>>::new(None),
        }
    }
    /// put puts data, filename and meta_info to server.
    pub async fn put(
        &mut self,
        buf: Vec<u8>,
        filename: String,
        meta: Option<Vec<u8>>,
    ) -> Result<String, ErrorKind> {
        let mut inner_writer = self.inner_writer.lock().unwrap();
        let mut writer = match inner_writer.take() {
            Some(writer) => writer,
            None => {
                let inner_new_writer = self.create_new_writers(self.id).await.unwrap();
                self.id += 1;
                inner_new_writer
            }
        };
        let id = match writer.write(buf, filename, meta).await {
            Ok(id) => id,
            Err(e) => return Err(e),
        };
        inner_writer.replace(writer);
        Ok(id)
    }

    async fn create_new_writers(&self, stack_id: u64) -> Result<InnerWriter, ErrorKind> {
        let index_file_path = utils::get_index_file_path(&self.prefix, stack_id);
        let mut index_writer = match self.operator.writer_with(&index_file_path).await {
            Ok(writer) => writer,
            Err(e) => return Err(ErrorKind::IOError(CustomError::new(e.to_string()))),
        };
        let meta_file_path = utils::get_meta_file_path(&self.prefix, stack_id);
        let mut meta_writer = match self.operator.writer_with(&meta_file_path).await {
            Ok(writer) => writer,
            Err(e) => return Err(ErrorKind::IOError(CustomError::new(e.to_string()))),
        };
        let data_file_path = utils::get_data_file_path(&self.prefix, stack_id);
        let mut data_writer = match self.operator.writer_with(&data_file_path).await {
            Ok(writer) => writer,
            Err(e) => return Err(ErrorKind::IOError(CustomError::new(e.to_string()))),
        };

        let ih = IndexMagicHeader::new(stack_id);
        let mh = MetaMagicHeader::new(stack_id);
        let dh = DataMagicHeader::new(stack_id);
        let ih_bytes = bincode::serialize(&ih).unwrap();
        let mut mh_bytes = serde_json::to_vec(&mh).unwrap();
        mh_bytes.push(b'\n');
        let mh_bytes_length = mh_bytes.len();
        let mut dh_bytes = bincode::serialize(&dh).unwrap();
        dh_bytes.resize(4096, 0);

        match index_writer.write(ih_bytes).await {
            Ok(_) => {}
            Err(e) => return Err(ErrorKind::IOError(CustomError::new(e.to_string()))),
        }
        match meta_writer.write(mh_bytes).await {
            Ok(_) => {}
            Err(e) => return Err(ErrorKind::IOError(CustomError::new(e.to_string()))),
        }
        match data_writer.write(dh_bytes).await {
            Ok(_) => {}
            Err(e) => return Err(ErrorKind::IOError(CustomError::new(e.to_string()))),
        }

        Ok(InnerWriter {
            data_offset: 4096,
            meta_offset: mh_bytes_length as u64,
            stack_id: stack_id,
            rng: rand::thread_rng(),
            _current_index_writer: index_writer,
            _current_meta_writer: meta_writer,
            _current_data_writer: data_writer,
        })
    }
    /// close flush and close all writer.
    pub async fn close(&self) -> Result<(), ErrorKind> {
        if let Ok(mut mu) = self.inner_writer.lock() {
            if let Some(writer) = mu.take() {
                writer.close().await?
            }
        }
        Ok(())
    }
}
