use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize)]
pub struct Config {
    pub s3: S3Config,
}

#[derive(Serialize, Deserialize)]
pub struct S3Config {
    pub region: String,
    pub aws_access_key_id: String,
    pub aws_secret_access_key: String,
    pub endpoint: String,
}
