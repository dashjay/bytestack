use bytestack::core::bytestack::ByteStackReader;
use bytestack::core::bytestack::ByteStackWriter;
use opendal::services::Memory;
use opendal::Operator;
use opendal::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let op = Operator::new(Memory::default())?.finish();
    let iw = op.writer_with("1.idx").await.unwrap();
    let mw = op.writer_with("1.meta").await.unwrap();
    let dw = op.writer_with("1.data").await.unwrap();
    let mut bsw = ByteStackWriter::new(1, iw, mw, dw);
    bsw.write_files_magic_header().await;

    let mut i = 0;
    while i < 100 {
        match bsw.put(vec![0; 4096], format!("file-{}", i)).await {
            Ok(_) => {
                println!("{} write", i);
            }
            Err(e) => {
                panic!("{:?}", e);
            }
        }
        i += 1;
    }
    bsw.close().await;

    let ir = op.reader_with("1.idx").await.unwrap();
    let mr = op.reader_with("1.meta").await.unwrap();
    let dr = op.reader_with("1.data").await.unwrap();
    let mut bsr = ByteStackReader::new(1, ir, mr, dr);
    loop {
        if let Some((ir, mr, dr)) = bsr.next().await {
            println!("ir: {:?}\nmr: {:?}\ndr: {:?}\n", &ir, &mr, dr.header)
        } else {
            break;
        }
    }
    Ok(())
}
