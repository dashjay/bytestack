use super::types::{InnerPreLoad, InnerPreLoadAssignment};
use futures::TryStreamExt;
use log::{debug, info};
use mongodb::{
    bson::{doc, Document},
    error::{
        Result as MongoResult, TRANSIENT_TRANSACTION_ERROR, UNKNOWN_TRANSACTION_COMMIT_RESULT,
    },
    options::{
        Acknowledgment, CountOptions, DeleteOptions, ReadConcern, SessionOptions,
        TransactionOptions, UpdateOptions, WriteConcern,
    },
    Client, ClientSession, Collection,
};
use proto::controller::{
    controller_server::Controller, CallPreLoadReq, Empty, PreLoadAssignment as PbPreLoadAssignment,
    PreLoadAssignments, PreLoadState, QueryRegisteredSourceResp, StackId, StackSourceReq,
};

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
}

#[tonic::async_trait]
impl Controller for BytestackController {
    /// next_stack_id returns the next stack id.
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

    /// register_stack_source bind source to stack.
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

    // de_register_stack_source unbind source to stack.
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

    /// query_registered_source  query source of stack.
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

    /// locate_stack find preloaded stack on bserver.
    async fn locate_stack(
        &self,
        request: Request<StackId>,
    ) -> Result<Response<PreLoadAssignments>, Status> {
        let db = self.mongodb_client.database(DB);
        let collection = db.collection::<InnerPreLoadAssignment>(COLLECTION_PRELOADS);
        let stack_id = request.get_ref().stack_id as i64;
        let mut cursor = match collection
            .find(doc! {"stack_id": stack_id as i64}, None)
            .await
        {
            Ok(cursor) => cursor,
            Err(e) => {
                return Err(Status::internal(e.to_string()));
            }
        };
        let mut out: Vec<PbPreLoadAssignment> = Vec::new();
        while let Ok(preload_asignment) = cursor.try_next().await {
            match preload_asignment {
                Some(preload_asignment) => out.push(preload_asignment.into()),
                None => {}
            }
        }
        Ok(Response::new(PreLoadAssignments { preloads: out }))
    }

    /// pre_load register preload for stack to bserver.
    async fn pre_load(
        &self,
        request: Request<CallPreLoadReq>,
    ) -> Result<Response<PreLoadAssignments>, Status> {
        let stack_id = request.get_ref().stack_id;
        let replicas = {
            if request.get_ref().replicas > _MAX_PRELAOD_REPLICAS {
                _MAX_PRELAOD_REPLICAS
            } else {
                request.get_ref().replicas
            }
        };
        info!(target: "service/controller->preload", "preload stack_id({}) to replica({})", stack_id, replicas);
        let db = self.mongodb_client.database(DB);
        let preload_coll = db.collection::<InnerPreLoad>(COLLECTION_PRELOADS);
        let mut sess = match self
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
            Ok(_) => {
                info!(target: "service/controller->preload", "transaction started")
            }
            Err(e) => {
                return Err(Status::internal(e.to_string()));
            }
        }
        while let Err(error) =
            mongo_adjust_preload_replicas(&preload_coll, &mut sess, stack_id, replicas).await
        {
            info!(target: "service/controller->preload", "run mongo_adjust_preload_replicas: {:?}", error);
            if !error.contains_label(TRANSIENT_TRANSACTION_ERROR) {
                return Err(Status::internal(error.to_string()));
            }
        }

        let preload_assignment_collection =
            db.collection::<InnerPreLoadAssignment>(COLLECTION_PRELOADS);
        let stack_id = request.get_ref().stack_id as i64;
        let mut cursor = match preload_assignment_collection
            .find(
                doc! {"stack_id": stack_id as i64, "state": {"$ne": PreLoadState::Deleting as i32} },
                None,
            )
            .await
        {
            Ok(cursor) => cursor,
            Err(e) => {
                return Err(Status::internal(e.to_string()));
            }
        };
        let mut out: Vec<PbPreLoadAssignment> = Vec::new();
        while let Ok(preload_asignment) = cursor.try_next().await {
            match preload_asignment {
                Some(preload_asignment) => out.push(preload_asignment.into()),
                None => {}
            }
        }
        Ok(Response::new(PreLoadAssignments { preloads: out }))
    }
}

/// mongo_adjust_preload_replicas change the replicas of stack preload to replicas.
async fn mongo_adjust_preload_replicas(
    coll: &Collection<InnerPreLoad>,
    session: &mut ClientSession,
    stack_id: u64,
    replicas: i64,
) -> MongoResult<()> {
    info!(target: "service/controller->mongo_adjust_preload_replicas", "start");
    // once delete triggered, we ignore it
    let count = coll
        .count_documents_with_session(
            doc! {"stack_id": stack_id as i64, "state": {"$ne": PreLoadState::Deleting as i32} },
            CountOptions::builder().build(),
            session,
        )
        .await? as i64;

    info!(target: "service/controller->mongo_adjust_preload_replicas", "count = {}", count);

    if count == replicas {
        info!(target: "service/controller->mongo_adjust_preload_replicas", "count = replicas, abort");
        session.abort_transaction().await?;
        return Ok(());
    }

    if count != 0 {
        // lock all exists preload
        let _update_res = coll
        .update_many_with_session(
            doc! {"stack_id": stack_id as i64, "state": {"$ne": PreLoadState::Deleting as i32} },
            doc! {"$currentDate": {"update_timestamp": true}},
            None,
            session,
        )
        .await?;
    }

    if count < replicas {
        let mut more_preload = replicas - count;
        while more_preload > 0 {
            info!(target: "service/controller->mongo_adjust_preload_replicas", "need more preload = {}", more_preload);
            let insert_result = coll
                .insert_one_with_session(InnerPreLoad::new(stack_id), None, session)
                .await?;
            if let Some(_obj_id) = insert_result.inserted_id.as_object_id() {
                more_preload -= 1
            }
        }
    } else {
        let mut less_preload = count - replicas;
        while less_preload > 0 {
            info!(target: "service/controller->mongo_adjust_preload_replicas", "need less preload = {}", less_preload);
            let deletion_result = coll.delete_one_with_session(
                doc! {"stack_id": stack_id as i64, "state": {"$ne": PreLoadState::Deleting as i32} },
                DeleteOptions::builder().build(), session).await?;
            if deletion_result.deleted_count == 1 {
                less_preload -= 1
            }
        }
    }

    // loop commit_transaction to success.
    loop {
        info!(target: "service/controller->mongo_adjust_preload_replicas", "commit tran");
        let result = session.commit_transaction().await;
        if let Err(ref error) = result {
            if error.contains_label(UNKNOWN_TRANSACTION_COMMIT_RESULT) {
                continue;
            }
        }
        result?
    }
}
