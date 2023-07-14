use proto::turbo::turbo::turbo_server::Turbo;
use proto::turbo::turbo::{MaintainReq, MaintainResp, PreLoadTaskStatus};
use std::collections::HashMap;
use std::sync::RwLock;
use tonic::{Request, Response, Status};
pub struct BytestackTurbo {
    tasks: RwLock<HashMap<u64, PreLoadTaskStatus>>,
}

#[tonic::async_trait]
impl Turbo for BytestackTurbo {
    async fn maintain(
        &self,
        request: Request<MaintainReq>,
    ) -> Result<Response<MaintainResp>, Status> {   
        todo!()
    }
}
