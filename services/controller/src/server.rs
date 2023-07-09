use mongodb::options::UpdateOptions;
use tonic::{Request, Response, Status};

use mongodb::bson::{doc, Document};
use mongodb::Client;
use proto::controller::controller_server::Controller;
use proto::controller::{
    CallPreLoadReq, Empty, PreLoads, QueryRegisteredSourceResp, StackId, StackSourceReq,
};
pub struct BytestackController {
    mongodb_client: Client,
}

const DB: &str = "bytestack";
const COLLECTION_CONFIG: &str = "config";
const COLLECTION_STACK: &str = "stacks";
const COLLECTION_PRELOADS: &str = "preloads";

impl BytestackController {
    pub fn new(mongodb_client: Client) -> Self {
        BytestackController { mongodb_client }
    }
}

#[tonic::async_trait]
impl Controller for BytestackController {
    async fn next_stack_id(&self, _request: Request<Empty>) -> Result<Response<StackId>, Status> {
        let db = self.mongodb_client.database(DB);
        let collection = db.collection::<Document>(COLLECTION_CONFIG);
        let res = collection
            .find_one_and_update(
                doc! {
                    "config": "next_stack_id"
                },
                doc! {
                    "$inc": {"next_stack_id": 1}
                },
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
        let next_stack_id = match res.get("next_stack_id") {
            Some(id) => match id.as_i64() {
                Some(e) => e as u64,
                None => match id.as_i32() {
                    Some(e) => e as u64,
                    None => {
                        return Err(Status::internal(format!(
                            "field next_stack_id unexpected type"
                        )));
                    }
                },
            },
            None => return Err(Status::internal(format!("mongo read nothing"))),
        };
        Ok(Response::new(StackId {
            stack_id: next_stack_id,
        }))
    }
    async fn register_stack_source(
        &self,
        request: Request<StackSourceReq>,
    ) -> Result<Response<Empty>, Status> {
        let db = self.mongodb_client.database(DB);
        let collection = db.collection::<Document>(COLLECTION_STACK);
        let stack_id = request.get_ref().stack_id as i64;
        let locations = request.get_ref().locations.clone();
        let res = collection
            .update_one(
                doc! {
                    "_id": stack_id,
                },
                doc! {
                    "$addToSet": {"locations": {"$each": locations } }
                },
                UpdateOptions::builder().upsert(true).build(),
            )
            .await;
        match res {
            Ok(_) => return Ok(Response::new(Empty {})),
            Err(e) => return Err(Status::internal(e.to_string())),
        };
    }
    async fn de_register_stack_source(
        &self,
        request: Request<StackSourceReq>,
    ) -> Result<Response<Empty>, Status> {
        let db = self.mongodb_client.database(DB);
        let collection = db.collection::<Document>(COLLECTION_STACK);
        let stack_id = request.get_ref().stack_id as i64;
        let locations = request.get_ref().locations.clone();
        let res = collection
            .update_one(
                doc! {
                    "_id": stack_id,
                },
                doc! {
                    "$pull": {"locations": {"$in": locations } }
                },
                None,
            )
            .await;
        match res {
            Ok(_) => return Ok(Response::new(Empty {})),
            Err(e) => return Err(Status::internal(e.to_string())),
        };
    }
    async fn query_registered_source(
        &self,
        request: Request<StackId>,
    ) -> Result<Response<QueryRegisteredSourceResp>, Status> {
        let db = self.mongodb_client.database(DB);
        let collection = db.collection::<Document>(COLLECTION_STACK);
        let stack_id = request.get_ref().stack_id as i64;
        let res = collection
            .find_one(
                doc! {
                    "_id": stack_id,
                },
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
        let locations = match res.get_array("locations") {
            Ok(res) => res,
            Err(e) => return Err(Status::internal(e.to_string())),
        };
        let locations = locations.iter().map(|item| item.to_string()).collect();
        Ok(Response::new(QueryRegisteredSourceResp { locations }))
    }
    async fn locate_stack(&self, request: Request<StackId>) -> Result<Response<PreLoads>, Status> {
        let db = self.mongodb_client.database(DB);
        let collection = db.collection::<PreLoads>(COLLECTION_PRELOADS);
        let stack_id = request.get_ref().stack_id as i64;
        let mut cursor = match collection
            .find(
                doc! {
                    "stack_id": stack_id,
                },
                None,
            )
            .await
        {
            Ok(cursor) => cursor,
            Err(e) => {
                return Err(Status::internal(e.to_string()));
            }
        };
        todo!()
    }
    async fn pre_load(
        &self,
        request: Request<CallPreLoadReq>,
    ) -> Result<Response<PreLoads>, Status> {
        todo!()
    }

    async fn un_pre_load(&self, request: Request<StackId>) -> Result<Response<Empty>, Status> {
        todo!()
    }
}
