use std::{io::Read, mem};

use bytes::{Buf, BufMut, Bytes, BytesMut};

const ALIGNMENT_SIZE: usize = 4096;

const _data_header_magic: u64 = 47494638; // respects to GIF file header

pub struct DataMagicHeader {
    data_magic_number: u64,
    stack_id: u64,
}

impl DataMagicHeader {
    pub fn new(stack_id: u64) -> Self {
        DataMagicHeader {
            data_magic_number: _data_header_magic,
            stack_id: stack_id,
        }
    }
}

const _data_record_magic_start: u32 = 257758;
const _data_record_magic_end: u32 = 857752;

#[derive(Debug)]
pub struct DataRecordHeader {
    data_magic_record_start: u32,
    file_offset: u64,
    cookie: u32,
    size: u32,
    crc: u32,
    data_magic_record_end: u32,
}
const _data_record_header_size: usize = mem::size_of::<DataRecordHeader>();

impl DataRecordHeader {
    pub fn write_to(&self, buf: &mut BytesMut) {
        buf.put_u32(self.data_magic_record_start);
        buf.put_u64(self.file_offset);
        buf.put_u32(self.cookie);
        buf.put_u32(self.size);
        buf.put_u32(self.crc);
        buf.put_u32(self.data_magic_record_end);
    }

    pub fn new(file_offset: u64, cookie: u32, size: u32, crc: u32) -> Self {
        DataRecordHeader {
            data_magic_record_start: _data_record_magic_start,
            file_offset: file_offset,
            cookie: cookie,
            size: size,
            crc: crc,
            data_magic_record_end: _data_record_magic_end,
        }
    }

    pub fn new_from_bytes(data: &mut Bytes) -> Self {
        assert!(data.len() > _data_record_header_size as usize);
        DataRecordHeader {
            data_magic_record_start: data.get_u32(),
            file_offset: data.get_u64(),
            cookie: data.get_u32(),
            size: data.get_u32(),
            crc: data.get_u32(),
            data_magic_record_end: data.get_u32(),
        }
    }
}

#[derive(Debug)]
pub struct DataRecord {
    header: DataRecordHeader,
    data: Bytes,
    padding: Bytes,
}

fn padding_size(data_size: usize) -> usize {
    let data_with_header_size = data_size + _data_record_header_size;
    if data_with_header_size % ALIGNMENT_SIZE == 0 {
        return data_size;
    } else {
        return (data_size + ALIGNMENT_SIZE) - (data_with_header_size % ALIGNMENT_SIZE);
    }
}

impl DataRecord {
    pub fn new(file_offset: u64, cookie: u32, size: u32, crc: u32, data: Bytes) -> Self {
        let padding_size = padding_size(data.len()) - data.len();
        let bytes: Vec<u8> = vec![0; padding_size];
        let zero_padding = Bytes::from(bytes);
        DataRecord {
            header: DataRecordHeader::new(file_offset, cookie, size, crc),
            data: data,
            padding: zero_padding,
        }
    }
    pub fn new_from_bytes(data: &mut Bytes) -> Self {
        assert!(data.len() > (_data_record_header_size as usize));
        let header = DataRecordHeader::new_from_bytes(data);
        assert!(data.len() > (_data_record_header_size + (header.size as usize)));
        let data_end = _data_record_header_size + (header.size as usize);
        DataRecord {
            header: header,
            data: data.slice(_data_record_header_size..data_end),
            padding: data.slice(data_end..),
        }
    }
    pub fn size(&self) -> usize {
        _data_record_header_size + self.data.len() + self.padding.len()
    }
    pub fn write_to(&self, buf: &mut BytesMut) {
        self.header.write_to(buf);
        buf.extend_from_slice(&self.data);
        buf.extend_from_slice(&self.padding);
    }
}

#[test]
fn test_data() {
    let bytes: Vec<u8> = vec![1; 4096];
    let data = Bytes::from(bytes);
    let dr = DataRecord::new(0, 1234, 4096, 1243, data);
    let mut buf = BytesMut::default();
    dr.write_to(&mut buf);
    println!("{} != {}",buf.len(),dr.size());
    assert!(buf.len()== dr.size());
}
