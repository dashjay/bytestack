use crate::types::data::{DataMagicHeader, DataRecordHeader};
use crate::types::meta::MetaMagicHeader;
use crate::types::{data::DataRecord, index::*, meta::MetaRecord};
use crc::{Crc, CRC_32_ISCSI};
use rand::rngs::ThreadRng;
use rand::Rng;
pub const CASTAGNOLI: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);
use super::err::ErrorKind;

pub struct Writer {
    index_writer: Box<dyn std::io::Write>,
    data_writer: Box<dyn std::io::Write>,
    meta_writer: Box<dyn std::io::Write>,

    data_cursor: u64,
    meta_cursor: u64,
    rng: ThreadRng,
    stack_id: u64,
}

impl Writer {
    pub fn new(
        stack_id: u64,
        iw: Box<dyn std::io::Write>,
        dw: Box<dyn std::io::Write>,
        mw: Box<dyn std::io::Write>,
    ) -> Self {
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
        self.write_data_header();
        self.write_index_header();
        self.write_meta_header();
    }

    fn write_index_header(&mut self) -> Result<(), ErrorKind> {
        let index_file_header = IndexMagicHeader::new(self.stack_id);
        match bincode::serialize_into(self.index_writer.as_mut(), &index_file_header) {
            Ok(_) => return Ok(()),
            Err(e) => {
                return Err(ErrorKind::WriteError);
            }
        }
    }
    fn write_data_header(&mut self) -> Result<(), ErrorKind> {
        let data_file_header = DataMagicHeader::new(self.stack_id);
        match bincode::serialize_into(self.data_writer.as_mut(), &data_file_header) {
            Ok(_) => return Ok(()),
            Err(e) => {
                return Err(ErrorKind::WriteError);
            }
        }
    }
    fn write_meta_header(&mut self) -> Result<(), ErrorKind> {
        let meta_header = MetaMagicHeader::new(self.stack_id);
        if let Ok(res) = serde_json::to_vec(&meta_header) {
            match self.meta_writer.write(&res) {
                Ok(_) => {return Ok(())},
                Err(_) => {
                    return Err(ErrorKind::WriteError);
                }
            }
        }
        return Err(ErrorKind::MarshalJsonError);
    }

    fn write_index(&mut self, ir: IndexRecord) -> Result<(), ErrorKind> {
        match bincode::serialize_into(self.index_writer.as_mut(), &ir) {
            Ok(_) => return Ok(()),
            Err(e) => {
                return Err(ErrorKind::WriteError);
            }
        }
    }
    fn write_data(&mut self, dr: DataRecord) -> Result<(), ErrorKind> {
        match bincode::serialize_into(self.data_writer.as_mut(), &dr.header) {
            Ok(_) => {
                println!("write data: {:?}", &dr.header);
                self.data_cursor += DataRecordHeader::size() as u64;
            }
            Err(e) => {
                return Err(ErrorKind::WriteError);
            }
        }
        match self.data_writer.write(&dr.data) {
            Ok(n) => {
                println!("write data len: {}", &dr.data.len());
                self.data_cursor += n as u64;
            }
            Err(_) => {
                return Err(ErrorKind::WriteError);
            }
        }
        match self.data_writer.write(&dr.padding) {
            Ok(n) => {
                println!("write data len: {}", &dr.padding.len());
                self.data_cursor += n as u64;
                Ok(())
            }
            Err(_) => {
                return Err(ErrorKind::WriteError);
            }
        }
    }

    fn write_meta(&mut self, mr: MetaRecord) -> Result<(), ErrorKind> {
        match bincode::serialize_into(&mut self.meta_writer, &mr) {
            Ok(()) => {Ok(())},
            Err(_) => return Err(ErrorKind::MarshalJsonError),
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
