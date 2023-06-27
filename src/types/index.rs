use super::err::DecodeError;
use bincode;
use serde::{Deserialize, Serialize};

const _INDEX_HEADER_MAGIC: u64 = 5201314;

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct IndexMagicHeader {
    index_header_magic: u64,
    stack_id: u64,
}

impl IndexMagicHeader {
    pub fn new(stack_id: u64) -> Self {
        IndexMagicHeader {
            index_header_magic: _INDEX_HEADER_MAGIC,
            stack_id: stack_id,
        }
    }

    pub fn size() -> usize {
        let a = IndexMagicHeader::default();
        bincode::serialized_size(&a).unwrap() as usize
    }
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct IndexRecord {
    cookie: u32,
    size: u32,
    offset_data: u64,
    offset_meta: u64,
}

impl PartialEq<IndexRecord> for IndexRecord {
    fn eq(&self, other: &IndexRecord) -> bool {
        self.cookie == other.cookie
            && self.size == other.size
            && self.offset_data == other.offset_data
            && self.offset_meta == other.offset_meta
    }
}

impl IndexRecord {
    pub fn new(cookie: u32, size: u32, offset_data: u64, offset_meta: u64) -> Self {
        IndexRecord {
            cookie,
            size,
            offset_data,
            offset_meta,
        }
    }

    pub fn size() -> usize {
        let a = IndexRecord::default();
        bincode::serialized_size(&a).unwrap() as usize
    }

    pub fn new_from_bytes(data: &[u8]) -> Result<IndexRecord, Box<bincode::ErrorKind>> {
        return bincode::deserialize::<IndexRecord>(data);
    }

    pub fn new_from_reader(r: &mut dyn std::io::Read) -> Result<IndexRecord, DecodeError> {
        let mut buf = Vec::new();
        buf.resize(IndexRecord::size(), 0);
        if let Ok(n) = r.read(&mut buf) {
            if n != IndexRecord::size() {
                return Err(DecodeError::ShortRead);
            }
            if let Ok(rc) = bincode::deserialize(&buf) {
                return Ok(rc);
            }
            return Err(DecodeError::DeserializeError);
        } else {
            return Err(DecodeError::IOError);
        }
    }
}

#[test]
fn test_index_encode_and_decode() {
    use std::io::{Cursor, Write};
    let ir = IndexRecord::new(1, 2, 3, 4);
    let mut buffer = Cursor::new(Vec::new());
    let header_bytes = bincode::serialize(&ir).unwrap();
    let mut write_once = |input: &[u8]| match buffer.write(input) {
        Ok(n) => {
            assert!(n == input.len())
        }
        Err(err) => {
            eprintln!("Failed to read header: {}", err);
            return;
        }
    };
    write_once(&header_bytes);

    buffer.set_position(0);
    if let Ok(nir) = IndexRecord::new_from_reader(&mut buffer) {
        assert!(ir == nir);
    }
}