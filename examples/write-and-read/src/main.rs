use bytestack::sdk;

#[tokio::main]
async fn main() {
    let handler = sdk::Handler::new(sdk::Config {
        s3: sdk::S3 {
            aws_access_key_id: "minioadmin".to_string(),
            aws_secret_access_key: "minioadmin".to_string(),
            endpoint: "http://localhost:9000".to_string(),
            region: "default".to_string(),
        },
    });
    let mut bw = handler
        .open_writer("http://localhost:8080", "s3://test/dadadad.bs/")
        .await
        .unwrap();
    let mut idx: i32 = 0;
    while idx < 2000 {
        let content = vec![(idx % 124) as u8; 4096];
        let _id = bw
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
            let _data = match br
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
