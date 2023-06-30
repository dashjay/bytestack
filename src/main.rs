

#[tokio::main]
async fn main()  {
    let handler = bytestack::core::BytestackHandler::new();
    let br = handler.open_reader("s3://test/");
    let stack_list = br.list_all_stack().await.unwrap();
    for s in &stack_list {
        println!(
            "stack_id: {}, last_modified: {}",
            s.stack_id, s.last_modified
        )
    }
    for s in &stack_list{
        for id in br.list_stack(s.stack_id).await.unwrap(){
            println!("{}",id);
            let buf = br.get_by_index_id(id).await.unwrap();
            println!("data_length: {}",buf.len())
        }
    }
}
