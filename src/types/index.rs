use super::err::{CustomError, DecodeError};
use bincode;
use futures::AsyncReadExt;
use opendal::Reader;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

pub const _INDEX_HEADER_MAGIC: u64 = 5201314;

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct IndexMagicHeader {
    pub index_header_magic: u64,
    pub stack_id: u64,
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

    pub fn index_id(&self) -> String {
        let mut temp = Vec::with_capacity(12);
        temp.extend_from_slice(&self.offset_data.to_le_bytes());
        temp.extend_from_slice(&self.cookie.to_le_bytes());
        temp.iter().map(|byte| format!("{:02x}", byte)).collect()
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
        match r.read_exact(&mut buf) {
            Ok(_) => match bincode::deserialize(&buf) {
                Ok(rc) => return Ok(rc),
                Err(e) => {
                    return Err(DecodeError::DeserializeError(CustomError::new(
                        e.to_string(),
                    )));
                }
            },
            Err(e) => {
                return Err(DecodeError::IOError(CustomError::new(e.to_string())));
            }
        }
    }

    pub async fn new_from_future_reader(r: &mut Reader) -> Result<IndexRecord, DecodeError> {
        let mut buf = Vec::new();
        buf.resize(IndexRecord::size(), 0);
        match r.read_exact(&mut buf).await {
            Ok(_) => match bincode::deserialize(&buf) {
                Ok(rc) => return Ok(rc),
                Err(e) => {
                    return Err(DecodeError::DeserializeError(CustomError::new(
                        e.to_string(),
                    )));
                }
            },
            Err(e) => {
                return Err(DecodeError::IOError(CustomError::new(e.to_string())));
            }
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
