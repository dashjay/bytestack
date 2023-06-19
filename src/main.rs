use futures::StreamExt;
use opendal::layers::LoggingLayer;
use opendal::Operator;
use opendal::Result;
use opendal::services;
use tokio::io::AsyncWriteExt;
use bytes::Bytes;

#[tokio::main]
async fn main() -> Result<()> {
    // Pick a builder and configure it.
    let mut builder = services::S3::default();
    builder.bucket("test");

    // Init an operator
    let op = Operator::new(builder)?
        // Init with logging layer enabled.
        .layer(LoggingLayer::default())
        .finish();

    op.create_dir("test/").await?;
    let mut w = op
        .writer_with("test/new-file")
        .content_type("application/octet-stream")
        .content_length(8192)
        .await?;
    w.write(vec![0; 4096]).await?;
    w.write(vec![0; 4096]).await?;
    w.close().await?;

    // Write data
    op.write("test/hello.txt", "Hello, World!").await?;

    // Read data
    let bs = op.read("test/hello.txt").await?;

    // Fetch metadata
    let meta = op.stat("test/new-file").await?;
    let mode = meta.mode();
    let length = meta.content_length();
    println!("{}, {}", mode, length);

    let mut out = op.list("test/").await?;
    println!("{}", out.next_page().await.unwrap().unwrap().len());

    // Delete
    // op.remove_all("test/").await?;

    Ok(())
}