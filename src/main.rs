use bytestack::core::reader::Reader;
use bytestack::core::writer::Writer;
use std::fs::File;

fn main() {
    {
        let data_file = File::create("/tmp/1.data").unwrap();
        let meta_file = File::create("/tmp/1.meta").unwrap();
        let index_file = File::create("/tmp/1.idx").unwrap();
        let mut writer = Writer::new(
            1,
            Box::new(index_file),
            Box::new(data_file),
            Box::new(meta_file),
        );
        writer.write_files_magic_header();

        match writer.put(vec![0; 4096], String::from("file1")) {
            Ok(_) => {}
            Err(e) => {
                println!("{:?}", e)
            }
        }
        match writer.put(vec![1; 4096], String::from("file2")) {
            Ok(_) => {}
            Err(e) => {
                println!("{:?}", e)
            }
        }
        match writer.put(vec![3; 4096], String::from("file3")) {
            Ok(_) => {}
            Err(e) => {
                println!("{:?}", e)
            }
        }
    }
    {
        let data_file = File::open("/tmp/1.data").unwrap();
        let meta_file = File::open("/tmp/1.meta").unwrap();
        let index_file = File::open("/tmp/1.idx").unwrap();
        let mut reader = Reader::new(
            1,
            Box::new(index_file),
            Box::new(data_file),
            Box::new(meta_file),
        );
        reader.init();
        for (ir, mr, dr) in reader {
            println!("ir: {:?}\nmr: {:?}\ndr: {:?}\n", &ir, &mr, dr.header)
        }
    }
}
