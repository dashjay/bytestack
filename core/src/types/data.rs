use super::err::{CustomError, DecodeError};
use bincode;
use futures::AsyncReadExt;
use opendal::Reader;
use serde::{Deserialize, Serialize};

const ALIGNMENT_SIZE: usize = 4096;

/// _DATA_HEADER_MAGIC is a magic number respects to GIF file header
pub const _DATA_HEADER_MAGIC: u64 = 47494638;

/// DataMagicHeader will be serialized with bincode and save to file header in every data file, which is used to
/// identification this is an data file, this struct SHOULD NOT BE MODIFIED!!!
#[derive(Serialize, Deserialize, Debug)]
pub struct DataMagicHeader {
    /// data_magic_number will always be _DATA_HEADER_MAGIC
    pub data_magic_number: u64,
    /// stack_id is used to identify which meta or index are associated with this file
    pub stack_id: u64,
}

impl DataMagicHeader {
    /// new return a DataMagicHeader by stack_id
    pub fn new(stack_id: u64) -> Self {
        DataMagicHeader {
            data_magic_number: _DATA_HEADER_MAGIC,
            stack_id: stack_id,
        }
    }

    /// size return the size of DataMagicHeader
    pub fn size() -> usize {
        16
    }
}

#[test]
fn test_data_magic_header_size() {
    let temp = DataMagicHeader::new(0);
    assert!(DataMagicHeader::size() == bincode::serialized_size(&temp).unwrap() as usize);
}

/// _DATA_RECORD_HEADER_MAGIC_START is a magic number used by data_record
pub const _DATA_RECORD_HEADER_MAGIC_START: u32 = 257758;
/// _DATA_RECORD_HEADER_MAGIC_END is a magic number used by data_record
pub const _DATA_RECORD_HEADER_MAGIC_END: u32 = 857752;

/// DataRecordHeader carries cookie, size and crc info of this data record
/// # Note
/// Every data item start with this DataRecordHeader like this:
/// `| data_magic_record_start: u32 | cookie: u32 | size: u32 | crc: u32 | data_magic_record_end: u32 | (20 bytes)`
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct DataRecordHeader {
    /// data_magic_record_start is used to recognize this is data_record_header start, which is always _DATA_RECORD_HEADER_MAGIC_START
    data_magic_record_start: u32,
    cookie: u32,
    size: u32,
    crc: u32,
    /// data_magic_record_end is used to recognize this is data_record_end start, which is always _DATA_RECORD_HEADER_MAGIC_END
    data_magic_record_end: u32,
}

impl PartialEq<DataRecordHeader> for DataRecordHeader {
    /// eq means only the same cookie, size and crc checksum
    fn eq(&self, other: &DataRecordHeader) -> bool {
        self.cookie == other.cookie && self.size == other.size && self.crc == other.crc
    }
}

impl DataRecordHeader {
    /// new received cookie, data_size, and crc
    fn new(cookie: u32, size: u32, crc: u32) -> Self {
        DataRecordHeader {
            data_magic_record_start: _DATA_RECORD_HEADER_MAGIC_START,
            cookie: cookie,
            size: size,
            crc: crc,
            data_magic_record_end: _DATA_RECORD_HEADER_MAGIC_END,
        }
    }

    /// validate_magic check if this data_record has correct magic
    /// In theory bytestack It is not responsible for data consistency, we
    /// just provides a simple crc.
    pub fn validate_magic(&self) -> bool {
        self.data_magic_record_start == _DATA_RECORD_HEADER_MAGIC_START
            && self.data_magic_record_end == self.data_magic_record_end
    }

    // get_cookie get the cookie of this data record header
    pub fn get_cookie(&self) -> u32 {
        self.cookie
    }

    /// get_data_size get the size of this data.
    pub fn get_data_size(&self) -> u32 {
        self.size
    }

    pub fn new_from_bytes(data: &[u8]) -> Result<DataRecordHeader, Box<bincode::ErrorKind>> {
        assert!(data.len() == Self::size());
        bincode::deserialize::<DataRecordHeader>(data)
    }

    pub fn size() -> usize {
       20
    }
}

#[test]
fn test_data_record_header_size() {
    let temp = DataRecordHeader::new(0,0,0);
    assert!(bincode::serialized_size::<DataRecordHeader>(&temp).unwrap() as usize == DataRecordHeader::size());
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
                    return Err(DecodeError::ShortRead(CustomError::new(format!(
                        "record data_size {} > input data len {} - header_size",
                        data_size,
                        data.len()
                    ))));
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
            Err(e) => {
                return Err(DecodeError::DeserializeError(CustomError::new(
                    e.to_string(),
                )));
            }
        }
    }

    pub fn size(&self) -> usize {
        DataRecordHeader::size() + self.data.len() + self.padding.len()
    }
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
