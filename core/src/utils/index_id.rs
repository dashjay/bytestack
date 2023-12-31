//! index_id is a way to fetch data

/// IndexID is consist of stack_id, file_offset, cookie.
/// it shows like this format!({},{:x}{:08x}, stack_id, offset_data, cookie)
pub struct IndexID {
    /// stack_id of this index_id
    pub stack_id: u64,
    /// offset_data exists in index_id for fast access.
    pub offset_data: u64,
    /// cookie is needed for access data
    pub cookie: u32,
}

/// parse_index_id can help parse the index_id to struct IndexID
pub fn parse_index_id(id: &str) -> Option<IndexID> {
    if let Some((stack_id, index_id)) = id.split_once(",") {
        let stack_id = u64::from_str_radix(stack_id, 10).unwrap();
        let index_id_length = index_id.len();
        if index_id_length < 8 || index_id_length > 24 {
            return None;
        }
        let cookie_str = &index_id[index_id_length - 8..];
        assert!(cookie_str.len() == 8);
        let file_offset_str = &index_id[..index_id_length - 8];

        let cookie = u32::from_str_radix(cookie_str, 16).unwrap();
        let offset_data = u64::from_str_radix(file_offset_str, 16).unwrap();
        return Some(IndexID {
            stack_id,
            offset_data,
            cookie,
        });
    }
    None
}

#[test]
fn test_create_and_parse() {
    use crate::types::IndexRecord;
    let ir = IndexRecord::new(12345, 2004, 3, 4, 5);
    let index_id = format!("{},{}", 100, ir.index_id());
    let parse_index_id = parse_index_id(&index_id).unwrap();
    assert_eq!(parse_index_id.stack_id, 100);
    assert_eq!(parse_index_id.cookie, 12345);
    assert_eq!(parse_index_id.offset_data, 2004);
}
