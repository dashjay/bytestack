use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Config {
    pub s3: S3,
}

#[derive(Serialize, Deserialize, Debug, Default)]

pub struct S3 {
    pub region: String,
    pub aws_access_key_id: String,
    pub aws_secret_access_key: String,
    pub endpoint: String,
}
