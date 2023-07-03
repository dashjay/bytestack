//! path provides tools for handling all kinds of path
//! bytestack is built on opendal which support many kinds of storage backends.
//! 

/// get_data_file_path return data path for giving prefix and stack_id
pub fn get_data_file_path(prefix: &str, stack_id: u64) -> String {
    format!("{}{:04x}.data", prefix, stack_id)
}
/// get_index_file_path return index path for giving prefix and stack_id
pub fn get_index_file_path(prefix: &str, stack_id: u64) -> String {
    format!("{}{:04x}.idx", prefix, stack_id)
}
/// get_meta_file_path return meta path for giving prefix and stack_id
pub fn get_meta_file_path(prefix: &str, stack_id: u64) -> String {
    format!("{}{:04x}.meta", prefix, stack_id)
}
