use super::err::{CustomError, ErrorKind};
use proto::controller::controller_client::ControllerClient;
use proto::controller::Empty;
use tonic::transport::Channel;

pub struct RemoteIdGenerator {
    cli: ControllerClient<Channel>,
}

impl RemoteIdGenerator {
    pub async fn new(target_addr: &'static str) -> Self {
        let channel = match ControllerClient::connect(target_addr).await {
            Ok(res) => res,
            Err(err) => {
                panic!("connect to {} error: {}", target_addr, err);
            }
        };

        RemoteIdGenerator { cli: channel }
    }

    pub async fn next_stack_id(&mut self) -> Result<u64, ErrorKind> {
        let req = tonic::Request::new(Empty {});
        match self.cli.next_stack_id(req).await {
            Ok(resp) => return Ok(resp.get_ref().stack_id),
            Err(err) => {
                return Err(ErrorKind::IOError(CustomError::new(err.to_string())));
            }
        };
    }
}
