
//! stack describe a bytestack abostractly
use tabled::Tabled;

#[derive(Tabled)]
/// Stack hold some stack info: stack_id, last_modified, full_size, etc..
pub struct Stack {
    /// stack_id of Stack
    pub stack_id: u64,
    /// last_modified record the last_modified of index file.
    pub last_modified: chrono::DateTime<chrono::Utc>,
    /// full_size sums all data
    pub full_size: u64,
}
