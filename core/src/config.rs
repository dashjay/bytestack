use super::sdk::err::{CustomError, ErrorKind};

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct S3 {
    pub region: String,
    pub aws_access_key_id: String,
    pub aws_secret_access_key: String,
    pub endpoint: String,
}

impl S3 {
    pub fn new_from_bytes(input: &[u8]) -> Result<Self, ErrorKind> {
        match bincode::deserialize(input) {
            Ok(res) => Ok(res),
            Err(e) => return Err(ErrorKind::InvalidArgument(CustomError::new(e.to_string()))),
        }
    }
}
