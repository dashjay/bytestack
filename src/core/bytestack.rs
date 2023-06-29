use super::err::{CustomError, ErrorKind};
use crate::types::{
    data::{DataMagicHeader, DataRecord, DataRecordHeader, _DATA_HEADER_MAGIC},
    index::{IndexMagicHeader, IndexRecord, _INDEX_HEADER_MAGIC},
    meta::{MetaMagicHeader, MetaRecord, _META_HEADER_MAGIC},
};
use crc::{Crc, CRC_32_ISCSI};
use rand::Rng;
pub const CASTAGNOLI: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);
use rand::rngs::ThreadRng;

type Item = (IndexRecord, MetaRecord, DataRecord);

use opendal::{Reader, Writer};

pub struct ByteStackReader {
    stack_id: i64,
    _index_reader: Reader,
    _meta_reader: Reader,
    _data_reader: Reader,
}

impl ByteStackReader {
    pub fn new(stack_id: i64, i: Reader, m: Reader, d: Reader) -> Self {
        ByteStackReader {
            stack_id: stack_id,
            _index_reader: i,
            _meta_reader: m,
            _data_reader: d,
        }
    }

    pub async fn next(&mut self) -> Option<Item> {
        let result_ir = IndexRecord::new_from_future_reader(&mut self._index_reader).await;
        let result_mr = MetaRecord::new_from_future_reader(&mut self._meta_reader).await;
        let result_dr = DataRecord::new_from_future_reader(&mut self._data_reader).await;

        if let Ok(ir) = result_ir {
            if let Ok(mr) = result_mr {
                if let Ok(dr) = result_dr {
                    return Some((ir, mr, dr));
                }
            }
        }
        return None;
    }
}

pub struct ByteStackWriter {
    stack_id: u64,
    _index_writer: Writer,
    _meta_writer: Writer,
    _data_writer: Writer,
    rng: ThreadRng,
    data_cursor: u64,
    meta_cursor: u64,
}

impl ByteStackWriter {
    pub fn new(stack_id: u64, i: Writer, m: Writer, d: Writer) -> Self {
        ByteStackWriter {
            stack_id: stack_id,
            _index_writer: i,
            _meta_writer: m,
            _data_writer: d,
            data_cursor: 0,
            meta_cursor: 0,
            rng: rand::thread_rng(),
        }
    }

    pub async fn write_files_magic_header(&mut self) {
        self.write_data_header().await.unwrap();
        self.write_index_header().await.unwrap();
        self.write_meta_header().await.unwrap();
    }

    async fn write_index_header(&mut self) -> Result<(), ErrorKind> {
        let index_file_header = IndexMagicHeader::new(self.stack_id);
        match bincode::serialize(&index_file_header) {
            Ok(data) => match self._index_writer.write(data).await {
                Ok(_) => Ok(()),
                Err(e) => {
                    return Err(ErrorKind::WriteError(CustomError::new(e.to_string())));
                }
            },
            Err(e) => {
                return Err(ErrorKind::WriteError(CustomError::new(e.to_string())));
            }
        }
    }
    async fn write_data_header(&mut self) -> Result<(), ErrorKind> {
        let data_file_header = DataMagicHeader::new(self.stack_id);
        match bincode::serialize(&data_file_header) {
            Ok(data) => match self._data_writer.write(data).await {
                Ok(_) => Ok(()),
                Err(e) => {
                    return Err(ErrorKind::WriteError(CustomError::new(e.to_string())));
                }
            },
            Err(e) => {
                return Err(ErrorKind::WriteError(CustomError::new(e.to_string())));
            }
        }
    }
    async fn write_meta_header(&mut self) -> Result<(), ErrorKind> {
        let meta_header = MetaMagicHeader::new(self.stack_id);
        match bincode::serialize(&meta_header) {
            Ok(data) => match self._meta_writer.write(data).await {
                Ok(_) => Ok(()),
                Err(e) => {
                    return Err(ErrorKind::WriteError(CustomError::new(e.to_string())));
                }
            },
            Err(e) => {
                return Err(ErrorKind::WriteError(CustomError::new(e.to_string())));
            }
        }
    }

    async fn write_index(&mut self, ir: IndexRecord) -> Result<(), ErrorKind> {
        match bincode::serialize(&ir) {
            Ok(data) => match self._index_writer.write(data).await {
                Ok(_) => {
                    self.meta_cursor += IndexRecord::size() as u64;
                    Ok(())
                }
                Err(e) => {
                    return Err(ErrorKind::WriteError(CustomError::new(e.to_string())));
                }
            },
            Err(e) => {
                return Err(ErrorKind::WriteError(CustomError::new(e.to_string())));
            }
        }
    }
    async fn write_data(&mut self, dr: DataRecord) -> Result<(), ErrorKind> {
        match bincode::serialize(&dr.header) {
            Ok(data) => match self._data_writer.write(data).await {
                Ok(_) => {
                    self.data_cursor += DataRecordHeader::size() as u64;
                }
                Err(e) => {
                    return Err(ErrorKind::WriteError(CustomError::new(e.to_string())));
                }
            },
            Err(e) => {
                return Err(ErrorKind::WriteError(CustomError::new(e.to_string())));
            }
        }
        let data_len = dr.data.len();
        match self._data_writer.write(dr.data).await {
            Ok(n) => {
                self.data_cursor += data_len as u64;
            }
            Err(e) => {
                return Err(ErrorKind::WriteError(CustomError::new(e.to_string())));
            }
        }

        let padding_len = dr.padding.len();
        match self._data_writer.write(dr.padding).await {
            Ok(n) => {
                self.data_cursor += padding_len as u64;
                Ok(())
            }
            Err(e) => {
                return Err(ErrorKind::WriteError(CustomError::new(e.to_string())));
            }
        }
    }
    async fn write_meta(&mut self, mr: MetaRecord) -> Result<(), ErrorKind> {
        match bincode::serialize(&mr) {
            Ok(data) => match self._meta_writer.write(data).await {
                Ok(_) => Ok(()),
                Err(e) => {
                    return Err(ErrorKind::WriteError(CustomError::new(e.to_string())));
                }
            },
            Err(e) => {
                return Err(ErrorKind::WriteError(CustomError::new(e.to_string())));
            }
        }
    }

    pub async fn put(&mut self, data: Vec<u8>, filename: String) -> Result<(), ErrorKind> {
        let cookie: u32 = self.rng.gen();
        let data_len = data.len() as u32;
        let crc_sum = CASTAGNOLI.checksum(&data);

        let ir = IndexRecord::new(cookie, data_len, self.data_cursor, self.meta_cursor);
        let dr = DataRecord::new(cookie, data_len, crc_sum, data);
        let mr = MetaRecord::new(self.data_cursor, cookie, data_len, filename.into_bytes());
        match self.write_index(ir).await {
            Ok(_) => {}
            Err(e) => {
                return Err(e);
            }
        }
        match self.write_data(dr).await {
            Ok(_) => {}
            Err(e) => {
                return Err(e);
            }
        }
        match self.write_meta(mr).await {
            Ok(_) => Ok(()),
            Err(e) => {
                return Err(e);
            }
        }
    }
    pub async fn close(&mut self) {
        self._index_writer.close().await.unwrap();
        self._meta_writer.close().await.unwrap();
        self._data_writer.close().await.unwrap();
    }
}
