
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

pub const _META_HEADER_MAGIC: u64 = 1314920;

#[derive(Serialize, Deserialize, Default)]
pub struct MetaMagicHeader {
    pub meta_magic_number: u64,
    pub stack_id: u64,
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
    filename: String,
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
    pub fn new(
        file_offset: u64,
        cookie: u32,
        file_size: u32,
        filename: String,
        extra: Vec<u8>,
    ) -> Self {
        MetaRecord {
            create_time: current_time(),
            file_offset: file_offset,
            cookie: cookie,
            file_size: file_size,
            filename: filename,
            extra: extra,
        }
    }
    pub fn new_from_bytes(data: &[u8]) -> Result<MetaRecord, Box<bincode::ErrorKind>> {
        return bincode::deserialize::<MetaRecord>(data);
    }

    pub fn size(&self) -> usize {
        serde_json::to_vec(&self).unwrap().len() + 1
    }
}

