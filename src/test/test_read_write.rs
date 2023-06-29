#[test]
fn test_read_write() {
    use crate::core::reader::Reader;
    use crate::core::writer::Writer;
    use std::fs::File;
    {
        let data_file = File::create("/tmp/1.data").unwrap();
        let meta_file = File::create("/tmp/1.meta").unwrap();
        let index_file = File::create("/tmp/1.idx").unwrap();
        let mut writer = Writer::new(1, index_file, data_file, meta_file);
        writer.write_files_magic_header();

        let mut write_once = |filename: String| match writer.put(vec![0; 4096], filename) {
            Ok(_) => {}
            Err(e) => {
                panic!("{:?}", e);
            }
        };
        let mut i = 0;
        while i < 100 {
            write_once(String::from(format!("file-{}", i)));
            i += 1
        }
    }
    {
        let data_file = File::open("/tmp/1.data").unwrap();
        let meta_file = File::open("/tmp/1.meta").unwrap();
        let index_file = File::open("/tmp/1.idx").unwrap();
        let mut reader = Reader::new(1, index_file, data_file, meta_file);
        reader.read_and_check_magic_header();
        for (ir, mr, dr) in &mut reader {
            println!("ir: {:?}\nmr: {:?}\ndr: {:?}\n", &ir, &mr, dr.header)
        }
        reader.reset_to_head();
        for (ir, mr, dr) in &mut reader {
            println!("ir: {:?}\nmr: {:?}\ndr: {:?}\n", &ir, &mr, dr.header)
        }
    }
}
