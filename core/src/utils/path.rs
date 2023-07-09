//! path provides tools for handling all kinds of path
//! bytestack is built on opendal which support many kinds of storage backends.
//!

/// get_data_file_path return data path for giving prefix and stack_id
pub fn get_data_file_path(prefix: &str, stack_id: u64) -> String {
    format!("{}0x{:04x}.data", prefix, stack_id)
}
/// get_index_file_path return index path for giving prefix and stack_id
pub fn get_index_file_path(prefix: &str, stack_id: u64) -> String {
    format!("{}0x{:04x}.idx", prefix, stack_id)
}
/// get_meta_file_path return meta path for giving prefix and stack_id
pub fn get_meta_file_path(prefix: &str, stack_id: u64) -> String {
    format!("{}0x{:04x}.meta", prefix, stack_id)
}

pub fn parse_index_stack_id(file_name: &str) -> Option<u64> {
    return parse_file_stack_id(file_name, ".idx");
}

pub fn parse_meta_stack_id(file_name: &str) -> Option<u64> {
    return parse_file_stack_id(file_name, ".meta");
}

pub fn parse_data_stack_id(file_name: &str) -> Option<u64> {
    return parse_file_stack_id(file_name, ".data");
}

fn parse_file_stack_id(file_name: &str, typ: &str) -> Option<u64> {
    if file_name.starts_with("0x") && file_name.ends_with(typ) {
        return Some(
            u64::from_str_radix(
                file_name
                    .strip_prefix("0x")
                    .unwrap()
                    .strip_suffix(typ)
                    .unwrap(),
                16,
            )
            .unwrap(),
        );
    }
    return None;
}
