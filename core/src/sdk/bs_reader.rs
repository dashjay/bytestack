pub mod bs_reader {

    pub struct StackIDWithTime {
        pub stack_id: u64,
        pub last_modified: chrono::DateTime<chrono::Utc>,
    }
    pub struct BytestackReader {}

    impl BytestackReader {
        pub async fn stat_dir(&self) -> Result<Vec<StackIDWithTime>, opendal::Error> {
            todo!()
        }
    }
}
