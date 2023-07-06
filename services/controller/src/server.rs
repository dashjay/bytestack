use tonic::{Request, Response, Status};

use mongodb::bson::{doc, Document};
use mongodb::Client;
use proto::controller::controller_server::Controller;
use proto::controller::{Empty, NextStackIdResp};
pub struct BytestackController {
    mongodb_client: Client,
}

const DB: &str = "bytestack";
const COLLECTION_CONFIG: &str = "config";

impl BytestackController {
    pub fn new(mongodb_client: Client) -> Self {
        BytestackController { mongodb_client }
    }
}

#[tonic::async_trait]
impl Controller for BytestackController {
    async fn next_stack_id(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<NextStackIdResp>, Status> {
        let db = self.mongodb_client.database(DB);
        let collection = db.collection::<Document>(COLLECTION_CONFIG);
        let res = collection
            .find_one_and_update(
                doc! {"config":"next_stack_id"},
                doc! {"next_stack_id": "$inc"},
                None,
            )
            .await;
        let res = match res {
            Ok(res) => match res {
                Some(res) => res,
                None => return Err(Status::internal(format!("mongo read nothing"))),
            },
            Err(e) => return Err(Status::internal(e.to_string())),
        };
        let next_stack_id = match res.get_i64("next_stack_id"){
            Ok(id)=>{id},
            Err(e)=>return Err(Status::internal(e.to_string())),
        };
        let reply = NextStackIdResp { stack_id: next_stack_id };
        Ok(Response::new(reply))
    }
}
