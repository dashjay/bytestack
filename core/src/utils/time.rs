use std::time::{SystemTime, UNIX_EPOCH};

/// current_time return the unix_timestamp by now.
pub fn current_time() -> u64 {
    return SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
}
