use std::str::FromStr;

use super::err::{CustomError, ErrorKind};
use super::BytestackOpendalReader;
use super::BytestackOpendalWriter;
use super::Config;
use log::info;
use opendal::services::S3;
use opendal::Operator;
use proto::controller::controller_client::ControllerClient;
use tonic::transport::{Channel, Endpoint};
use url::Url;

/// BytestackOpendalHandler is entrance of sdk
pub struct BytestackOpendalHandler {
    cfg: Config,
    controller_cli: Option<ControllerClient<Channel>>,
}

impl BytestackOpendalHandler {
    /// new BytestackOpendalHandler
    pub async fn new(cfg: Config) -> Self {
        let channel = {
            if cfg.controller == "" {
                info!(target: "bytestack/core", "no controller addr specified");
                None
            } else {
                match ControllerClient::connect(Endpoint::from_str(&cfg.controller).unwrap()).await
                {
                    Ok(res) => Some(res),
                    Err(err) => {
                        panic!("connect to {} error: {}", &cfg.controller, err);
                    }
                }
            }
        };
        BytestackOpendalHandler {
            cfg,
            controller_cli: channel,
        }
    }

    fn get_operator_by_path(&self, path: &str) -> Operator {
        let url = Url::parse(path).expect(format!("Failed to parse URL {}", path).as_str());
        match url.scheme() {
            "s3" => {
                let (bucket, _) = parse_s3_url(path).unwrap();
                return init_s3_operator_via_builder(
                    &bucket,
                    &self.cfg.s3.region,
                    &self.cfg.s3.aws_access_key_id,
                    &self.cfg.s3.aws_secret_access_key,
                    &self.cfg.s3.endpoint,
                );
            }
            _ => {
                panic!("unknown scheme: {}, url: {}", url.scheme(), path)
            }
        }
    }

    /// open_reader return BytestackOpendalReader for giving path
    pub fn open_reader(&self, path: &str) -> Result<BytestackOpendalReader, ErrorKind> {
        let operator = self.get_operator_by_path(path);
        let (_, prefix) = match parse_s3_url(path) {
            Ok(a) => a,
            Err(e) => {
                return Err(e);
            }
        };
        let controller_cli = {
            match self.controller_cli {
                Some(cli) => Some(cli.clone()),
                None => None,
            }
        };
        Ok(BytestackOpendalReader::new(
            operator,
            prefix,
            controller_cli,
        ))
    }

    /// open_writer return BytestackOpendalWriter for giving path
    pub fn open_writer(&self, path: &str) -> Result<BytestackOpendalWriter, ErrorKind> {
        let operator = self.get_operator_by_path(path);
        let (_, prefix) = match parse_s3_url(path) {
            Ok(a) => a,
            Err(e) => {
                return Err(e);
            }
        };

        let controller_cli = {
            match self.controller_cli {
                Some(cli) => Some(cli.clone()),
                None => None,
            }
        };
        Ok(BytestackOpendalWriter::new(
            operator,
            prefix,
            controller_cli,
        ))
    }
    // pub fn open_appender(&self, path: &str) {}
}

fn init_s3_operator_via_builder(
    bucket: &str,
    region: &str,
    accesskey: &str,
    secretkey: &str,
    endpoint: &str,
) -> Operator {
    let mut builder = S3::default();
    builder
        .endpoint(endpoint)
        .bucket(bucket)
        .region(region)
        .access_key_id(accesskey)
        .secret_access_key(secretkey);
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
        Err(ErrorKind::InvalidArgument(CustomError::new(format!(
            "invalid s3 url: {}",
            path
        ))))
    }
}
