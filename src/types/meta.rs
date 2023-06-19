use bytes::Bytes;

const _meta_header_magic: u64 = 1314920;


pub struct MetaMagicHeader {
    meta_magic_number: u64,
    stack_id: u64,
}

impl MetaMagicHeader {
    pub fn new(stack_id: u64) -> Self {
        MetaMagicHeader { meta_magic_number: _meta_header_magic, stack_id:stack_id }
    }
}

pub struct MetaRecord {
    create_time: i64,
    file_size: i64,
    extra: Bytes,
}

