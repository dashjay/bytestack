use mongodb::bson::DateTime;
use proto::controller::{
    PreLoad as PbPreLoad, PreLoadAssignment as PbPreLoadAssignment, PreLoadState,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct InnerPreLoad {
    pub stack_id: u64,
    pub state: i32,
    pub bserver: String,
    pub creation_timestamp: DateTime,
    pub loaded_timestamp: DateTime,

    /// update_timestamp is used to lock PreLoad in mongodb by update it with $currentDate.
    pub update_timestamp: DateTime,
}

impl InnerPreLoad {
    pub fn new(stack_id: u64) -> Self {
        Self {
            stack_id,
            state: PreLoadState::Init as i32,
            bserver: String::new(),
            creation_timestamp: DateTime::now(),
            loaded_timestamp: DateTime::MIN,
            update_timestamp: DateTime::MIN,
        }
    }
}

impl Into<PbPreLoad> for InnerPreLoad {
    fn into(self) -> PbPreLoad {
        PbPreLoad {
            stack_id: self.stack_id,
            state: self.state,
            bserver: self.bserver,
            creation_timestamp: self.creation_timestamp.timestamp_millis(),
            loaded_timestamp: self.loaded_timestamp.timestamp_millis(),
            update_timestamp: self.update_timestamp.timestamp_millis(),
        }
    }
}

impl From<PbPreLoad> for InnerPreLoad {
    fn from(value: PbPreLoad) -> Self {
        Self {
            stack_id: value.stack_id,
            state: value.state,
            bserver: value.bserver,
            creation_timestamp: DateTime::from_millis(value.creation_timestamp),
            loaded_timestamp: DateTime::from_millis(value.loaded_timestamp),
            update_timestamp: DateTime::from_millis(value.update_timestamp),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct InnerPreLoadAssignment {
    pub stack_id: u64,
    pub total_size: u64,
    pub loaded: u64,
    pub bserver: String,
    pub data_addr: String,
    pub creation_timestamp: DateTime,
}

impl Into<PbPreLoadAssignment> for InnerPreLoadAssignment {
    fn into(self) -> PbPreLoadAssignment {
        PbPreLoadAssignment {
            stack_id: self.stack_id,
            total_size: self.total_size,
            loaded: self.loaded,
            bserver: self.bserver,
            data_addr: self.data_addr,
            creation_timestamp: self.creation_timestamp.timestamp_millis(),
        }
    }
}

impl From<PbPreLoadAssignment> for InnerPreLoadAssignment {
    fn from(value: PbPreLoadAssignment) -> Self {
        InnerPreLoadAssignment {
            stack_id: value.stack_id,
            total_size: value.total_size,
            loaded: value.loaded,
            bserver: value.bserver,
            data_addr: value.data_addr,
            creation_timestamp: DateTime::from_millis(value.creation_timestamp),
        }
    }
}
