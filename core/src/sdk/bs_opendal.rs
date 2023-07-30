use std::str::FromStr;

use super::err::{CustomError, ErrorKind};
use super::BytestackOpendalReader;
use super::BytestackOpendalWriter;
use super::Config;
use log::{debug, info};
use opendal::services::{Fs, S3};
use opendal::{Builder, Operator};
use proto::controller::PreLoadAssignments;
use proto::controller::{controller_client::ControllerClient, CallPreLoadReq, StackSourceReq};

use tonic::transport::{Channel, Endpoint};
use tonic::Request;
use url::Url;

/// BytestackOpendalHandler is entrance of sdk
pub struct BytestackOpendalHandler {
    cfg: Config,
    controller_cli: ControllerClient<Channel>,
}

impl BytestackOpendalHandler {
    /// new BytestackOpendalHandler
    pub async fn new(cfg: Config) -> Self {
        debug!(
            target: "BytestackOpendalHandler",
            "connect to controller: {}", &cfg.controller
        );
        let channel = {
            if cfg.controller.is_empty() {
                panic!("no controller addr specified")
            }
            match ControllerClient::connect(Endpoint::from_str(&cfg.controller).unwrap()).await {
                Ok(res) => res,
                Err(err) => {
                    panic!("connect to {} error: {}", &cfg.controller, err);
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
            "file" => {
                let mut builder = Fs::default();
                builder.root(&path);
                let op = Operator::new(builder).unwrap().finish();
                op
            }
            _ => {
                panic!("unknown scheme: {}, url: {}", url.scheme(), path)
            }
        }
    }

    /// open_reader return BytestackOpendalReader for giving path
    pub fn open_reader(&self, path: &str) -> Result<BytestackOpendalReader, ErrorKind> {
        debug!(target: "BytestackOpendalHandler", "open_reader on path: {}", path);
        let operator = self.get_operator_by_path(path);
        let (_, prefix) = match parse_s3_url(path) {
            Ok(a) => a,
            Err(e) => {
                return Err(e);
            }
        };
        Ok(BytestackOpendalReader::new(
            operator,
            prefix,
            self.controller_cli.clone(),
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
        Ok(BytestackOpendalWriter::new(
            operator,
            prefix,
            self.controller_cli.clone(),
        ))
    }

    /// bind_stack so that stack can be preload by bserver
    pub async fn bind_stack(&mut self, stack_id: u64, path: &str) -> Result<(), ErrorKind> {
        let req = Request::new(StackSourceReq {
            stack_id,
            locations: vec![path.to_string()],
        });
        let _resp = match self.controller_cli.register_stack_source(req).await {
            Ok(resp) => resp,
            Err(e) => return Err(ErrorKind::ControllerError(CustomError::new(e.to_string()))),
        };
        Ok(())
    }

    /// unbind_stack so that stack can not be preload by bserver
    pub async fn unbind_stack(&mut self, stack_id: u64, path: &str) -> Result<(), ErrorKind> {
        let req = Request::new(StackSourceReq {
            stack_id,
            locations: vec![path.to_string()],
        });
        let _resp = match self.controller_cli.de_register_stack_source(req).await {
            Ok(resp) => resp,
            Err(e) => return Err(ErrorKind::ControllerError(CustomError::new(e.to_string()))),
        };
        Ok(())
    }

    /// preload so that stack can not be preload by bserver
    pub async fn preload(
        &mut self,
        stack_id: u64,
        replicas: i64,
    ) -> Result<PreLoadAssignments, ErrorKind> {
        let req = Request::new(CallPreLoadReq { stack_id, replicas });
        let _resp = match self.controller_cli.pre_load(req).await {
            Ok(resp) => return Ok(resp.into_inner()),
            Err(e) => return Err(ErrorKind::ControllerError(CustomError::new(e.to_string()))),
        };
    }
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
