use super::err::ErrorKind;
use crate::types::{
    data::{DataMagicHeader, DataRecord},
    index::{IndexMagicHeader, IndexRecord},
    meta::{MetaMagicHeader, MetaRecord},
};
pub struct Reader {
    index_reader: Box<dyn std::io::Read>,
    data_reader: Box<dyn std::io::Read>,
    meta_reader: Box<dyn std::io::Read>,
    stack_id: u64,
}

impl Iterator for Reader {
    type Item = (IndexRecord, MetaRecord, DataRecord);
    fn next(&mut self) -> Option<Self::Item> {
        let ir = IndexRecord::new_from_reader(&mut self.index_reader).unwrap();
        // let mr = MetaRecord::new_from_reader(&mut self.meta_reader).unwrap();
        let dr = DataRecord::new_from_reader(&mut self.data_reader).unwrap();
        Some((ir, MetaRecord::default(), dr))
    }
}

impl Reader {
    pub fn new(
        stack_id: u64,
        ir: Box<dyn std::io::Read>,
        dr: Box<dyn std::io::Read>,
        mr: Box<dyn std::io::Read>,
    ) -> Self {
        Reader {
            index_reader: ir,
            data_reader: dr,
            meta_reader: mr,
            stack_id: stack_id,
        }
    }

    pub fn init(&mut self) {
        {
            let mut buf: Vec<u8> = Vec::new();
            buf.resize(IndexMagicHeader::size(), 0);
            self.index_reader.read_exact(&mut buf).unwrap();
        }
        {
            let mut buf: Vec<u8> = Vec::new();
            buf.resize(DataMagicHeader::size(), 0);
            self.data_reader.read_exact(&mut buf).unwrap();
        }
        {
            let mut buf: Vec<u8> = Vec::new();
            buf.resize(MetaMagicHeader::size(), 0);
            self.meta_reader.read_exact(&mut buf).unwrap();
        }
    }
}
