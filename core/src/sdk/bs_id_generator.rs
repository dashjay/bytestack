use proto::controller::controller_client::ControllerClient;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use tonic::transport::Channel;

pub trait IdGenerator {
    fn next_stack_id(&self) -> i64;
}

pub struct StatcIdGenerator {
    next_stack_id: AtomicI64,
}

impl StatcIdGenerator {
    fn new(next_stack_id: i64) -> Self {
        StatcIdGenerator {
            next_stack_id: AtomicI64::new(next_stack_id),
        }
    }
}

impl IdGenerator for StatcIdGenerator {
    fn next_stack_id(&self) -> i64 {
        self.next_stack_id.fetch_add(1, Ordering::Relaxed)
    }
}

pub struct RemoteIdGenerator {
    cli: ControllerClient<Channel>,
}

impl RemoteIdGenerator {
    async pub fn new(target_addr: String) -> Self {
        let channel = match ControllerClient::connect(url::Url::from(&target_addr)).await {
            Ok(res) => res,
            Err(err) => {
                panic!("connect to {} error: {}", &target_addr, err);
            }
        };

        RemoteIdGenerator {
            cli: channel
        }
    }
}
