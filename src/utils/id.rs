pub struct IndexID {
    stack_id: u64,
    file_offset: u64,
    cookie: u32,
}

impl IndexID {
   pub  fn stack_id(&self) -> u64 {
        self.stack_id
    }
    pub fn file_offset(&self) -> u64 {
        self.file_offset
    }
    pub fn cookie(&self) -> u32 {
        self.cookie
    }
}

const INDEX_ID_LENGTH: usize = 12;

pub fn parse_stack_id(id: &str) -> Option<IndexID> {
    if let Some((stack_id, index_id)) = id.split_once(",") {
        let stack_id_u64 = u64::from_str_radix(stack_id, 10).unwrap();

        let bytes_index_id: Vec<u8> = index_id
            .as_bytes()
            .chunks(2)
            .map(|chunk| u8::from_str_radix(std::str::from_utf8(chunk).unwrap(), 16).unwrap())
            .collect();
        assert_eq!(bytes_index_id.len(), INDEX_ID_LENGTH);

        let data_offset = u64::from_le_bytes(bytes_index_id[..8].try_into().unwrap());
        let cookie = u32::from_le_bytes(bytes_index_id[8..].try_into().unwrap());
        return Some(IndexID {
            stack_id: stack_id_u64,
            file_offset: data_offset,
            cookie: cookie,
        });
    }
    None
}
