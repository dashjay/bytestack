use super::err::DecodeError;
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

const _META_HEADER_MAGIC: u64 = 1314920;

#[derive(Serialize, Deserialize, Default)]
pub struct MetaMagicHeader {
    meta_magic_number: u64,
    stack_id: u64,
}

impl MetaMagicHeader {
    pub fn new(stack_id: u64) -> Self {
        MetaMagicHeader {
            meta_magic_number: _META_HEADER_MAGIC,
            stack_id: stack_id,
        }
    }
    pub fn size() -> usize {
        let a = MetaMagicHeader::default();
        bincode::serialized_size(&a).unwrap() as usize
    }
}

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct MetaRecord {
    create_time: u64,
    file_offset: u64,
    cookie: u32,
    file_size: u32,
    extra: Vec<u8>,
}

impl PartialEq<MetaRecord> for MetaRecord {
    fn eq(&self, other: &MetaRecord) -> bool {
        self.create_time == other.create_time
            && self.file_offset == other.file_offset
            && self.cookie == other.cookie
            && self.file_size == other.file_size
            && self.extra == other.extra
    }
}

fn current_time() -> u64 {
    return SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
}

impl MetaRecord {
    pub fn new(file_offset: u64, cookie: u32, file_size: u32, extra: Vec<u8>) -> Self {
        MetaRecord {
            create_time: current_time(),
            file_offset: file_offset,
            cookie: cookie,
            file_size: file_size,
            extra: extra,
        }
    }
    pub fn new_from_bytes(data: &[u8]) -> Result<MetaRecord, Box<bincode::ErrorKind>> {
        return bincode::deserialize::<MetaRecord>(data);
    }

    pub fn size() -> usize {
        let a = MetaRecord::default();
        bincode::serialized_size(&a).unwrap() as usize
    }

    pub fn new_from_reader(r: &mut dyn std::io::Read) -> Result<MetaRecord, DecodeError> {
        match bincode::deserialize_from(r) {
            Ok(mr) => return Ok(mr),
            Err(e) => {
                println!("deserialize_from error: {}", e);
                return Err(DecodeError::DeserializeError);
            }
        }
    }
}

#[test]
fn test_index_encode_and_decode() {
    use std::io::{Cursor, Write};
    let mr = MetaRecord::default();
    let mut buffer = Cursor::new(Vec::new());
    let header_bytes = bincode::serialize(&mr).unwrap();
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
    if let Ok(nmr) = MetaRecord::new_from_reader(&mut buffer) {
        assert!(mr == nmr);
    }
}
