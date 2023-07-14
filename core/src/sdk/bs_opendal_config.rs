use crate::config::S3;
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Config {
    pub controller: String,
    pub s3: S3,
}
