use super::err::{CustomError, ErrorKind};
use super::StackReader;
use super::StackWriter;
use opendal::services::S3;
use opendal::Operator;
use std::env;
use url::Url;

pub struct BytestackHandler;

const _ENV_OSS_ENDPOINT: &str = "OSS_ENDPOINT";

fn get_default_endpoint() -> String {
    if let Ok(path_value) = env::var(_ENV_OSS_ENDPOINT) {
        path_value
    } else {
        String::new()
    }
}
impl BytestackHandler {
    pub fn new() -> Self {
        BytestackHandler {}
    }

    fn get_operator_by_path(&self, path: &str) -> Operator {
        let url = Url::parse(path).expect(format!("Failed to parse URL {}", path).as_str());
        match url.scheme() {
            "s3" => {
                let res = parse_s3_url(path).unwrap();
                return init_s3_operator_via_builder(
                    res.0.as_str(),
                    "default",
                    "minioadmin",
                    "minioadmin",
                );
            }
            _ => {
                panic!("unknown scheme: {}, url: {}", url.scheme(), path)
            }
        }
    }

    pub fn open_reader(&self, path: &str) -> Result<StackReader, ErrorKind> {
        let operator = self.get_operator_by_path(path);
        let (_, prefix) = match parse_s3_url(path) {
            Ok(a) => a,
            Err(e) => {
                return Err(e);
            }
        };
        Ok(StackReader::new(operator, prefix))
    }
    pub fn open_writer(&self, path: &str) -> Result<StackWriter, ErrorKind> {
        let operator = self.get_operator_by_path(path);
        let (_, prefix) = match parse_s3_url(path) {
            Ok(a) => a,
            Err(e) => {
                return Err(e);
            }
        };
        Ok(StackWriter::new(operator, prefix))
    }
    // pub fn open_appender(&self, path: &str) {}
}

fn init_s3_operator_via_builder(
    bucket: &str,
    region: &str,
    accesskey: &str,
    secretkey: &str,
) -> Operator {
    let mut builder = S3::default();
    builder.endpoint(get_default_endpoint().as_str());
    builder.bucket(bucket);
    builder.region(region);
    builder.access_key_id(accesskey);
    builder.secret_access_key(secretkey);
    let op = Operator::new(builder).unwrap().finish();
    op
}

fn parse_s3_url(path: &str) -> Result<(String, String), ErrorKind> {
    let re = regex::Regex::new(r"s3://([^/]+)/(.*)").unwrap();
    if let Some(captures) = re.captures(path) {
        let bucket = captures.get(1).unwrap().as_str();
        let prefix = captures.get(2).unwrap().as_str();
        Ok((bucket.to_string(), prefix.to_string()))
    } else {
        Err(ErrorKind::ConfigError(CustomError::new(format!(
            "invalid s3 url: {}",
            path
        ))))
    }
}
