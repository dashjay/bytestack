use bincode;
use serde::{Deserialize, Serialize};

use super::err::DecodeError;

const ALIGNMENT_SIZE: usize = 4096;

const _DATA_FILE_HEADER_MAGIC_NUMBER: u64 = 47494638; // respects to GIF file header

#[derive(Serialize, Deserialize, Debug)]
pub struct DataMagicHeader {
    data_magic_number: u64,
    stack_id: u64,
}

impl DataMagicHeader {
    pub fn new(stack_id: u64) -> Self {
        DataMagicHeader {
            data_magic_number: _DATA_FILE_HEADER_MAGIC_NUMBER,
            stack_id: stack_id,
        }
    }

    pub fn size() -> usize {
        let a = DataMagicHeader::new(0);
        bincode::serialized_size(&a).unwrap() as usize
    }
}

const _DATA_RECORD_HEADER_MAGIC_START: u32 = 257758;
const _DATA_RECORD_HEEADER_MAGIC_END: u32 = 857752;

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct DataRecordHeader {
    data_magic_record_start: u32,
    cookie: u32,
    size: u32,
    crc: u32,
    data_magic_record_end: u32,
}

impl PartialEq<DataRecordHeader> for DataRecordHeader {
    fn eq(&self, other: &DataRecordHeader) -> bool {
        self.cookie == other.cookie && self.size == other.size && self.crc == other.crc
    }
}

impl DataRecordHeader {
    pub fn new(cookie: u32, size: u32, crc: u32) -> Self {
        DataRecordHeader {
            data_magic_record_start: _DATA_RECORD_HEADER_MAGIC_START,
            cookie: cookie,
            size: size,
            crc: crc,
            data_magic_record_end: _DATA_RECORD_HEEADER_MAGIC_END,
        }
    }

    pub fn new_from_bytes(data: &[u8]) -> Result<DataRecordHeader, Box<bincode::ErrorKind>> {
        bincode::deserialize::<DataRecordHeader>(data)
    }

    pub fn new_from_reader(r: &mut dyn std::io::Read) -> Result<DataRecordHeader, DecodeError> {
        let mut buf = Vec::new();
        buf.resize(DataRecordHeader::size(), 0);
        if let Ok(n) = r.read(&mut buf) {
            if n != DataRecordHeader::size() {
                return Err(DecodeError::ShortRead);
            }
            if let Ok(dhr) = bincode::deserialize::<DataRecordHeader>(&buf) {
                return Ok(dhr);
            }
            return Err(DecodeError::DeserializeError);
        }
        return Err(DecodeError::IOError);
    }

    pub fn size() -> usize {
        let a = Self::default();
        bincode::serialized_size::<DataRecordHeader>(&a).unwrap() as usize
    }
}

#[derive(Debug)]
pub struct DataRecord {
    pub header: DataRecordHeader,
    pub data: Vec<u8>,
    pub padding: Vec<u8>,
}

fn padding_data_size(data_size: usize) -> usize {
    let data_with_header_size = data_size + DataRecordHeader::size();
    if data_with_header_size % ALIGNMENT_SIZE == 0 {
        return data_size;
    } else {
        return (data_size + ALIGNMENT_SIZE) - (data_with_header_size % ALIGNMENT_SIZE);
    }
}

#[test]
fn test_padding_data_size() {
    let ok = |size: usize| -> bool { (size + DataRecordHeader::size()) % ALIGNMENT_SIZE == 0 };
    assert!(ok(padding_data_size(0)));
    assert!(ok(padding_data_size(1234)));
    assert!(ok(padding_data_size(4321)));
}

impl DataRecord {
    pub fn new(cookie: u32, size: u32, crc: u32, data: Vec<u8>) -> Self {
        let data_size = data.len();
        let padding_size = padding_data_size(data_size) - data_size;
        let zero_padding: Vec<u8> = vec![0; padding_size];
        assert!(zero_padding.len() == padding_size);
        DataRecord {
            header: DataRecordHeader::new(cookie, size, crc),
            data: data,
            padding: zero_padding,
        }
    }
    pub fn new_from_bytes(data: Vec<u8>) -> Result<DataRecord, DecodeError> {
        let header = DataRecordHeader::new_from_bytes(&data[..DataRecordHeader::size()]);
        match header {
            Ok(hdr) => {
                let data_size = hdr.size as usize;
                if data_size > data.len() - DataRecordHeader::size() {
                    return Err(DecodeError::ShortRead);
                }
                let data_bytes =
                    &data[DataRecordHeader::size()..data_size + DataRecordHeader::size()];
                let padding_bytes = &data[data_size + DataRecordHeader::size()..];
                return Ok(DataRecord {
                    header: hdr,
                    data: data_bytes.to_vec(),
                    padding: padding_bytes.to_vec(),
                });
            }
            Err(_) => {
                return Err(DecodeError::DeserializeError);
            }
        }
    }

    pub fn new_from_reader(r: &mut dyn std::io::Read) -> Result<DataRecord, DecodeError> {
        match DataRecordHeader::new_from_reader(r) {
            Ok(hdr) => {
                let data_size_usize = hdr.size as usize;
                let mut data = Vec::new();
                data.resize(data_size_usize, 0);
                let padding_size = padding_data_size(data_size_usize) - data_size_usize;
                let mut padding = Vec::new();
                padding.resize(padding_size, 0);
                if let Ok(n) = r.read(&mut data) {
                    if n != data_size_usize{
                        return Err(DecodeError::ShortRead);
                    }
                } else {
                    return Err(DecodeError::IOError);
                }
                if let Ok(n) = r.read(&mut padding) {
                    if n != padding_size{
                        return Err(DecodeError::ShortRead);
                    }
                } else {
                    return Err(DecodeError::IOError);
                }
                Ok(DataRecord {
                    header: hdr,
                    data: data,
                    padding: padding,
                })
            },
            Err(e)=>{Err(e)}
        }
    }
    
    pub fn size(&self) -> usize {
        DataRecordHeader::size() + self.data.len() + self.padding.len()
    }
}

#[test]
fn test_data_struct_size() {
    use std::mem;
    assert!(
        24 == DataRecordHeader::size(),
        "DataRecordHeader struct should not be modified"
    );
    assert!(
        16 == mem::size_of::<DataMagicHeader>(),
        "DataMagicHeader struct should not be modified"
    );
}

#[test]
fn test_data_encode_and_decode() {
    use std::io::{Cursor, Write};
    let dr = DataRecord::new(1234, 4096, 1243, vec![1; 4096]);
    let mut buffer = Cursor::new(Vec::new());
    let header_bytes = bincode::serialize(&dr.header).unwrap();
    let mut write_once = |input: &[u8]| match buffer.write(input) {
        Ok(n) => {
            assert!(n == input.len())
        }
        Err(err) => {
            eprintln!("Failed to read header: {}", err);
            return;
        }
    };
    write_once(&header_bytes);
    write_once(&dr.data);
    write_once(&dr.padding);

    buffer.set_position(0);
    if let Ok(ndr) = DataRecord::new_from_reader(&mut buffer) {
        assert!(dr.header == ndr.header);
        assert!(dr.data == ndr.data);
        assert!(dr.padding == ndr.padding);
    }
}
