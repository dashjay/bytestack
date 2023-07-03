pub fn get_data_file_path(prefix: &str, stack_id: u64) -> String {
    format!("{}{:04x}.data", prefix, stack_id)
}
pub fn get_index_file_path(prefix: &str, stack_id: u64) -> String {
    format!("{}{:04x}.idx", prefix, stack_id)
}
pub fn get_meta_file_path(prefix: &str, stack_id: u64) -> String {
    format!("{}{:04x}.meta", prefix, stack_id)
}
