//! meta will provide all data struct about meta file.

use serde::{Deserialize, Serialize};

/// _META_HEADER_MAGIC is a magic number which identify this is a meta file.
const _META_HEADER_MAGIC: u64 = 1314920;

/// MetaMagicHeader will be serialized with bincode and save to file header in every meta file, which is used to
/// identification this is an meta file, this struct SHOULD NOT BE MODIFIED!!!
#[derive(Serialize, Deserialize, Default)]
pub struct MetaMagicHeader {
    /// meta_magic_number should always be _META_HEADER_MAGIC
    pub meta_magic_number: u64,
    /// stack_id is used to identify which index or data are associated with this file
    pub stack_id: u64,
}

impl MetaMagicHeader {
    /// new return a MetaMagicHeader by stack_id
    pub fn new(stack_id: u64) -> Self {
        MetaMagicHeader {
            meta_magic_number: _META_HEADER_MAGIC,
            stack_id: stack_id,
        }
    }

    /// size return the size of MetaMagicHeader
    /// WARNING: because of marshaling by json, size differ from each other.
    pub fn size(&self) -> usize {
        serde_json::to_vec(&self).unwrap().len() + 1
    }
}

/// MetaRecord carries create_time, offset_data, size_data, cookie, filename and extra_info of data
/// # Note
/// MetaRecord will be marshaled to json
#[derive(Serialize, Deserialize, Debug)]
pub struct MetaRecord {
    create_time: u64,
    offset_data: u64,
    size_data: u32,
    cookie: u32,
    filename: String,
    extra: Vec<u8>,
}

impl PartialEq<MetaRecord> for MetaRecord {
    fn eq(&self, other: &MetaRecord) -> bool {
        self.create_time == other.create_time
            && self.offset_data == other.offset_data
            && self.cookie == other.cookie
            && self.size_data == other.size_data
            && self.extra == other.extra
    }
}

impl MetaRecord {
    /// new a MetaRecord
    pub fn new(
        create_time: u64,
        offset_data: u64,
        cookie: u32,
        size_data: u32,
        filename: String,
        extra: Vec<u8>,
    ) -> Self {
        MetaRecord {
            create_time,
            offset_data,
            cookie,
            size_data,
            filename,
            extra,
        }
    }

    /// new_from_bytes help read MetaRecord from json &[u8]
    pub fn new_from_bytes(data: &[u8]) -> Result<MetaRecord, serde_json::Error> {
        return serde_json::from_slice::<MetaRecord>(data);
    }

    /// size return the size of this instance
    pub fn size(&self) -> usize {
        serde_json::to_vec(&self).unwrap().len() + 1
    }
}
