//! data will provide all data struct about data file.
use bincode;
use serde::{Deserialize, Serialize};

const ALIGNMENT_SIZE: usize = 4096;

/// _DATA_HEADER_MAGIC is a magic number respects to GIF file header, and identify this is a data file.
const _DATA_HEADER_MAGIC: u64 = 47494638;

/// DataMagicHeader will be serialized with bincode and save to file header in every data file, which is used to
/// identification this is an data file, this struct SHOULD NOT BE MODIFIED!!!
#[derive(Serialize, Deserialize, Debug)]
pub struct DataMagicHeader {
    /// data_magic_number should always be _DATA_HEADER_MAGIC
    data_magic_number: u64,
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

    /// valid check if data_magic_number is _DATA_HEADER_MAGIC
    pub fn valid(&self) -> bool {
        return self.data_magic_number == _DATA_HEADER_MAGIC;
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
    /// cookie is a random u32 that forbid user to guess the offset.
    pub cookie: u32,
    /// size of data
    pub size: u32,
    /// crc of data
    pub crc: u32,
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

    /// new_from_bytes help deserialize DataRecordHeader from &[u8]
    pub fn new_from_bytes(data: &[u8]) -> Result<DataRecordHeader, Box<bincode::ErrorKind>> {
        assert!(data.len() == Self::size());
        bincode::deserialize::<DataRecordHeader>(data)
    }

    /// size of DataRecordHeader is 20 now
    pub fn size() -> usize {
        20
    }
}

#[test]
fn test_data_record_header_size() {
    let temp = DataRecordHeader::new(0, 0, 0);
    assert!(
        bincode::serialized_size::<DataRecordHeader>(&temp).unwrap() as usize
            == DataRecordHeader::size()
    );
}

/// DataRecord is a dummy struct not on disk, records arrange like this:
/// | header (20 bytes) | data | padding | <- this item padding to 4K
#[derive(Debug)]
pub struct DataRecord {
    /// header is DataRecordHeader, hold some metadata.
    pub header: DataRecordHeader,
    /// data is the true user data.
    pub data: Vec<u8>,
    /// padding is all /0
    pub padding: Vec<u8>,
}

/// padding_data_size help to calculate the padding size
/// | data_record_header | data | padding |
/// ⬆️ DataRecord start here
/// (data_record_header(20 Byte) + data + padding) should padding to 4K, so we need to calculate the padding size by data size
pub fn padding_data_size(data_size: usize) -> usize {
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
    /// new create a DataRecord
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

    /// size calculate the full data size
    /// MUST PADDING to 4k.
    pub fn size(&self) -> usize {
        let size = DataRecordHeader::size() + self.data.len() + self.padding.len();
        assert!(size % ALIGNMENT_SIZE == 0);
        size
    }
}
