use bytestack::sdk;

#[tokio::main]
async fn main() {
    let handler = sdk::Handler::new(sdk::Config {
        s3: sdk::S3Config {
            aws_access_key_id: "minioadmin".to_string(),
            aws_secret_access_key: "minioadmin".to_string(),
            endpoint: "http://localhost:9000".to_string(),
            region: "default".to_string(),
        },
    });
    let mut bw = handler.open_writer("s3://test/dadadad.bs/").unwrap();
    let mut idx = 0;
    while idx < 100 {
        let content = vec![idx; 4096];
        let id = bw
            .put(content, format!("filename-{}", idx), None)
            .await
            .expect("put data file");
        idx += 1;
    }
    bw.close().await.unwrap();

    let br = handler.open_reader("s3://test/dadadad.bs/").unwrap();
    let stack_list = br.list_al().await.unwrap();
    for s in &stack_list {
        println!(
            "stack_id: {}, last_modified: {}, full_size: {}",
            s.stack_id, s.last_modified, s.full_size
        )
    }
    for s in &stack_list {
        let mut iter = br.list_stack_al_iter(s.stack_id).await.unwrap();
        while let Some((ir, _mr)) = iter.next().await {
            let data = match br
            .fetch(format!("{},{}", s.stack_id, ir.index_id()), true)
                .await
            {
                Ok(data) => data,
                Err(e) => {
                    eprintln!("fetch data error: {:?}", e);
                    return;
                }
            };
        }
    }

    for s in &stack_list {
        let mut iter = br.list_stack_al_with_data_iter(s.stack_id).await.unwrap();
        while let Some((ir, _mr, data)) = iter.next().await {
            assert!(ir.size_data as usize == data.len())
        }
    }
}
