use crate::types::{
    data::{DataMagicHeader, DataRecord, _DATA_HEADER_MAGIC},
    index::{IndexMagicHeader, IndexRecord, _INDEX_HEADER_MAGIC},
    meta::{MetaMagicHeader, MetaRecord, _META_HEADER_MAGIC},
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
        let result_ir = IndexRecord::new_from_reader(&mut self.index_reader);
        let result_mr = MetaRecord::new_from_reader(&mut self.meta_reader);
        let result_dr = DataRecord::new_from_reader(&mut self.data_reader);

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

    pub fn read_and_check_magic_header(&mut self) {
        {
            let mut buf: Vec<u8> = Vec::new();
            buf.resize(IndexMagicHeader::size(), 0);
            self.index_reader.read_exact(&mut buf).unwrap();
            assert!(buf.len() == IndexMagicHeader::size());
            let imh = bincode::deserialize::<IndexMagicHeader>(&buf).unwrap();
            assert!(imh.stack_id == self.stack_id);
            assert!(imh.index_header_magic == _INDEX_HEADER_MAGIC)
        }
        {
            let mut buf: Vec<u8> = Vec::new();
            buf.resize(DataMagicHeader::size(), 0);
            self.data_reader.read_exact(&mut buf).unwrap();
            assert!(buf.len() == DataMagicHeader::size());
            let dmh = bincode::deserialize::<DataMagicHeader>(&buf).unwrap();
            assert!(dmh.stack_id == self.stack_id);
            assert!(dmh.data_magic_number == _DATA_HEADER_MAGIC)
        }
        {
            let mut buf: Vec<u8> = Vec::new();
            buf.resize(MetaMagicHeader::size(), 0);
            self.meta_reader.read_exact(&mut buf).unwrap();
            assert!(buf.len() == MetaMagicHeader::size());
            let mmh = bincode::deserialize::<MetaMagicHeader>(&buf).unwrap();
            assert!(mmh.stack_id == self.stack_id);
            assert!(mmh.meta_magic_number == _META_HEADER_MAGIC);
        }
    }
}
