const _index_header_magic: u64 = 5201314;

pub struct IndexMagicHeader {
    index_header_magic: u64,
    stack_id: u64,
}

impl IndexMagicHeader {
    pub fn new(stack_id: u64) -> Self {
        IndexMagicHeader { index_header_magic: _index_header_magic, stack_id: stack_id }
    }
}

pub struct IndexRecord {
    file_offset: u64,
    cookie: u32,
    size: u32,
    offset_data: u64,
    offset_meta: u64,
}

impl IndexRecord {
    pub fn new(file_offset: u64, cookie: u32, size: u32, offset_data: u64, offset_meta: u64) -> Self {
        IndexRecord { file_offset, cookie, size, offset_data, offset_meta }
    }
}

