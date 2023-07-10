use futures::TryStreamExt;
use mongodb::{
    bson::{doc, DateTime, Document},
    error::{
        Result as MongoResult, TRANSIENT_TRANSACTION_ERROR, UNKNOWN_TRANSACTION_COMMIT_RESULT,
    },
    options::{
        Acknowledgment, CountOptions, ReadConcern, SessionOptions, TransactionOptions,
        UpdateOptions, WriteConcern,
    },
    Client, ClientSession, Collection,
};

use proto::controller::controller_server::Controller;
use proto::controller::PreLoad as PbPreLoad;
use proto::controller::{
    CallPreLoadReq, Empty, PreLoads, QueryRegisteredSourceResp, StackId, StackSourceReq,
};
use serde::Deserialize;
use tonic::{Request, Response, Status};

pub struct BytestackController {
    mongodb_client: Client,
}

const _MAX_PRELAOD_REPLICAS: i64 = 5;

const DB: &str = "bytestack";
const COLLECTION_CONFIG: &str = "config";
const COLLECTION_STACK: &str = "stacks";
const COLLECTION_PRELOADS: &str = "preloads";

impl BytestackController {
    pub fn new(mongodb_client: Client) -> Self {
        BytestackController { mongodb_client }
    }
    async fn mongo_make_sure_preload_replicas(
        coll: &Collection<PreLoad>,
        session: &mut ClientSession,
        replicas: i64,
    ) -> MongoResult<()> {
        let count = coll
            .count_documents_with_session(doc! {}, CountOptions::builder().build(), session)
            .await?;
        if count as i64 == replicas {
            session.abort_transaction().await?;
            return Ok(());
        }
        todo!()
    }
}

#[derive(Debug, Deserialize)]
pub struct PreLoad {
    pub stack_id: u64,
    pub bserver: String,
    pub data_addr: String,
    pub total_size: i64,
    pub loaded: i64,
    pub creation_timestamp: DateTime,
    pub loaded_timestamp: DateTime,
    pub update_timestamp: DateTime,
}

impl Into<PbPreLoad> for PreLoad {
    fn into(self) -> PbPreLoad {
        PbPreLoad {
            stack_id: self.stack_id,
            bserver: self.bserver,
            data_addr: self.data_addr,
            total_size: self.total_size,
            loaded: self.loaded,
            creation_timestamp: self.creation_timestamp.timestamp_millis(),
            loaded_timestamp: self.loaded_timestamp.timestamp_millis(),
            update_timestamp: self.update_timestamp.timestamp_millis(),
        }
    }
}

impl From<PbPreLoad> for PreLoad {
    fn from(value: PbPreLoad) -> Self {
        Self {
            stack_id: value.stack_id,
            bserver: value.bserver,
            data_addr: value.data_addr,
            total_size: value.total_size,
            loaded: value.loaded,
            creation_timestamp: DateTime::from_millis(value.creation_timestamp),
            loaded_timestamp: DateTime::from_millis(value.loaded_timestamp),
            update_timestamp: DateTime::from_millis(value.update_timestamp),
        }
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
        let collection = db.collection::<PreLoad>(COLLECTION_PRELOADS);
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
        let mut out: Vec<PbPreLoad> = Vec::new();
        while let Ok(preload) = cursor.try_next().await {
            match preload {
                Some(preload) => out.push(preload.into()),
                None => {}
            }
        }
        Ok(Response::new(PreLoads { preloads: out }))
    }
    async fn pre_load(
        &self,
        request: Request<CallPreLoadReq>,
    ) -> Result<Response<PreLoads>, Status> {
        let replicas = {
            if request.get_ref().replicas > _MAX_PRELAOD_REPLICAS {
                _MAX_PRELAOD_REPLICAS
            } else {
                request.get_ref().replicas
            }
        };
        let db = self.mongodb_client.database(DB);
        let collection = db.collection::<PreLoad>(COLLECTION_PRELOADS);
        let sess = match self
            .mongodb_client
            .start_session(SessionOptions::builder().build())
            .await
        {
            Ok(sess) => sess,
            Err(e) => {
                return Err(Status::internal(e.to_string()));
            }
        };

        let tr_options = TransactionOptions::builder()
            .read_concern(ReadConcern::majority())
            .write_concern(WriteConcern::builder().w(Acknowledgment::Majority).build())
            .build();
        match sess.start_transaction(tr_options).await {
            Ok(_) => {}
            Err(e) => {
                return Err(Status::internal(e.to_string()));
            }
        }
    }

    async fn un_pre_load(&self, request: Request<StackId>) -> Result<Response<Empty>, Status> {
        todo!()
    }
}
