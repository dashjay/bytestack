#[tokio::main]
async fn main() {
    let handler = bytestack::core::BytestackHandler::new();
    let mut bw = handler.open_writer("s3://test/dadadad.bs/").unwrap();
    let mut idx = 0;
    while idx < 100 {
        let content = vec![idx; 4096];
        let id = bw
            .put(content, format!("filename-{}", idx), None)
            .await
            .expect("put data file");
        println!("put {} success", id);
        idx += 1;
    }
    bw.close().await.unwrap();
    println!("close success");

    let br = handler.open_reader("s3://test/").unwrap();
    let stack_list = br.list_all_stack().await.unwrap();
    for s in &stack_list {
        println!(
            "stack_id: {}, last_modified: {}",
            s.stack_id, s.last_modified
        )
    }
    for s in &stack_list {
        for id in br.list_stack(s.stack_id).await.unwrap() {
            println!("{}", id);
            let buf = br.get_by_index_id(id).await.unwrap();
            println!("data_length: {}", buf.len())
        }
    }
}
