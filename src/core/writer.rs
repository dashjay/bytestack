use crate::types::data::{DataMagicHeader, DataRecordHeader};
use crate::types::meta::MetaMagicHeader;
use crate::types::{data::DataRecord, index::*, meta::MetaRecord};
use crc::{Crc, CRC_32_ISCSI};
use rand::rngs::ThreadRng;
use rand::Rng;
pub const CASTAGNOLI: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);
use super::err::ErrorKind;
use crate::core::err::CustomError;

pub struct Writer<T>
where
    T: std::io::Read + std::io::Seek + std::io::Write,
{
    index_writer: T,
    data_writer: T,
    meta_writer: T,

    data_cursor: u64,
    meta_cursor: u64,
    rng: ThreadRng,
    stack_id: u64,
}

impl<T> Writer<T>
where
    T: std::io::Read + std::io::Seek + std::io::Write,
{
    pub fn new(stack_id: u64, iw: T, dw: T, mw: T) -> Self {
        Writer {
            index_writer: iw,
            data_writer: dw,
            meta_writer: mw,
            data_cursor: 0,
            meta_cursor: 0,
            rng: rand::thread_rng(),
            stack_id: stack_id,
        }
    }

    pub fn write_files_magic_header(&mut self) {
        self.write_data_header().unwrap();
        self.write_index_header().unwrap();
        self.write_meta_header().unwrap();
    }

    fn write_index_header(&mut self) -> Result<(), ErrorKind> {
        let index_file_header = IndexMagicHeader::new(self.stack_id);
        match bincode::serialize_into(&mut self.index_writer, &index_file_header) {
            Ok(_) => return Ok(()),
            Err(e) => {
                return Err(ErrorKind::WriteError(CustomError::new(e.to_string())));
            }
        }
    }
    fn write_data_header(&mut self) -> Result<(), ErrorKind> {
        let data_file_header = DataMagicHeader::new(self.stack_id);
        match bincode::serialize_into(&mut self.data_writer, &data_file_header) {
            Ok(_) => return Ok(()),
            Err(e) => {
                return Err(ErrorKind::WriteError(CustomError::new(e.to_string())));
            }
        }
    }
    fn write_meta_header(&mut self) -> Result<(), ErrorKind> {
        let meta_header = MetaMagicHeader::new(self.stack_id);
        match bincode::serialize_into(&mut self.meta_writer, &meta_header) {
            Ok(_) => return Ok(()),
            Err(e) => {
                return Err(ErrorKind::WriteError(CustomError::new(e.to_string())));
            }
        }
    }

    fn write_index(&mut self, ir: IndexRecord) -> Result<(), ErrorKind> {
        match bincode::serialize_into(&mut self.index_writer, &ir) {
            Ok(_) => return Ok(()),
            Err(e) => {
                return Err(ErrorKind::WriteError(CustomError::new(e.to_string())));
            }
        }
    }
    fn write_data(&mut self, dr: DataRecord) -> Result<(), ErrorKind> {
        match bincode::serialize_into(&mut self.data_writer, &dr.header) {
            Ok(_) => {
                self.data_cursor += DataRecordHeader::size() as u64;
            }
            Err(e) => {
                return Err(ErrorKind::WriteError(CustomError::new(e.to_string())));
            }
        }
        match self.data_writer.write(&dr.data) {
            Ok(n) => {
                self.data_cursor += n as u64;
            }
            Err(e) => {
                return Err(ErrorKind::WriteError(CustomError::new(e.to_string())));
            }
        }
        match self.data_writer.write(&dr.padding) {
            Ok(n) => {
                self.data_cursor += n as u64;
                Ok(())
            }
            Err(e) => {
                return Err(ErrorKind::WriteError(CustomError::new(e.to_string())));
            }
        }
    }

    fn write_meta(&mut self, mr: MetaRecord) -> Result<(), ErrorKind> {
        match bincode::serialize_into(&mut self.meta_writer, &mr) {
            Ok(()) => Ok(()),
            Err(e) => {
                return Err(ErrorKind::WriteError(CustomError::new(e.to_string())));
            }
        }
    }
    pub fn put(&mut self, data: Vec<u8>, filename: String) -> Result<(), ErrorKind> {
        let cookie: u32 = self.rng.gen();
        let data_len = data.len() as u32;
        let crc_sum = CASTAGNOLI.checksum(&data);

        let ir = IndexRecord::new(cookie, data_len, self.data_cursor, self.meta_cursor);
        let dr = DataRecord::new(cookie, data_len, crc_sum, data);
        let mr = MetaRecord::new(self.data_cursor, cookie, data_len, filename.into_bytes());
        match self.write_index(ir) {
            Ok(_) => {}
            Err(e) => {
                return Err(e);
            }
        }
        match self.write_data(dr) {
            Ok(_) => {}
            Err(e) => {
                return Err(e);
            }
        }
        match self.write_meta(mr) {
            Ok(_) => Ok(()),
            Err(e) => {
                return Err(e);
            }
        }
    }
}
