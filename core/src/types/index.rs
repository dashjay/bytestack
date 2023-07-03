use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// _INDEX_HEADER_MAGIC is a magic number which identify this is a index file.
const _INDEX_HEADER_MAGIC: u64 = 5201314;

/// IndexMagicHeader will be serialized with bincode and save to file header in every index file, which is used to
/// identification this is an index file, this struct SHOULD NOT BE MODIFIED!!!
#[derive(Serialize, Deserialize, Debug)]
pub struct IndexMagicHeader {
    /// data_magic_number should always be _INDEX_HEADER_MAGIC
    index_header_magic: u64,
    /// stack_id is used to identify which meta or index are associated with this file
    pub stack_id: u64,
}

impl IndexMagicHeader {
    /// new return a IndexMagicHeader by stack_id
    pub fn new(stack_id: u64) -> Self {
        IndexMagicHeader {
            index_header_magic: _INDEX_HEADER_MAGIC,
            stack_id: stack_id,
        }
    }

    /// size return the size of IndexMagicHeader
    pub fn size() -> usize {
        16
    }

    /// valid check if index_header_magic is _INDEX_HEADER_MAGIC
    pub fn valid(&self) -> bool {
        return self.index_header_magic == _INDEX_HEADER_MAGIC;
    }
}

/// IndexRecord carries cookie, offset_data, size_data, offset_meta, size_meta of the data
/// # Note
/// Every index item will be like this: `| cookie: u32 | offset_data: u64 | size_data: u64 | offset_meta: u64 | size_meta: u32 | (30 bytes)`
#[derive(Serialize, Deserialize, Debug)]
pub struct IndexRecord {
    cookie: u32,
    offset_data: u64,
    size_data: u32,
    offset_meta: u64,
    size_meta: u32,
}

impl PartialEq<IndexRecord> for IndexRecord {
    fn eq(&self, other: &IndexRecord) -> bool {
        self.cookie == other.cookie
            && self.offset_data == other.offset_data
            && self.size_data == other.size_data
            && self.offset_meta == other.offset_meta
            && self.size_meta == other.size_meta
    }
}

impl IndexRecord {
    /// new IndexRecord
    pub fn new(
        cookie: u32,
        offset_data: u64,
        size_data: u32,
        offset_meta: u64,
        size_meta: u32,
    ) -> Self {
        IndexRecord {
            cookie,
            size_data,
            offset_data,
            offset_meta,
            size_meta,
        }
    }

    /// index_id is a fast key to access data, it like this:
    /// offset_data(hexString)cookie(hexString)
    /// 420fe000d0b8efae
    pub fn index_id(&self) -> String {
        format!("{:x}{:8x}", self.offset_data, self.cookie)
    }

    pub fn size() -> usize {
        28
    }

    pub fn new_from_bytes(data: &[u8]) -> Result<IndexRecord, Box<bincode::ErrorKind>> {
        return bincode::deserialize::<IndexRecord>(data);
    }
}

#[test]
fn test_index_record_size() {
    let ir = IndexRecord::new(0, 0, 0, 0, 0);
    assert!(bincode::serialized_size(&ir).unwrap() as usize == IndexRecord::size())
}
