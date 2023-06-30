use super::err::{CustomError, ErrorKind};
use crate::types::{
    data::{DataMagicHeader, DataRecord, DataRecordHeader, _DATA_HEADER_MAGIC},
    index::{IndexMagicHeader, IndexRecord, _INDEX_HEADER_MAGIC},
    meta::{MetaMagicHeader, MetaRecord, _META_HEADER_MAGIC},
};
use bincode;
use crc::{Crc, CRC_32_ISCSI};
use futures::TryStreamExt;
use opendal::services::S3;
use opendal::EntryMode;
use opendal::Metakey;
use opendal::Operator;
use rand::Rng;
use std::env;
use tokio::io::AsyncReadExt;
pub const CASTAGNOLI: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);
use rand::rngs::ThreadRng;
use url::Url;

type Item = (IndexRecord, MetaRecord, DataRecord);

use opendal::{Reader, Writer};

pub struct StackReader {
    operator: Operator,
    prefix: String,
}

pub struct StackIDWithTime {
    pub stack_id: u64,
    pub last_modified: chrono::DateTime<chrono::Utc>,
}

impl StackReader {
    pub fn new(operator: Operator, prefix: String) -> StackReader {
        StackReader {
            operator: operator,
            prefix: prefix,
        }
    }

    pub async fn list_all_stack(&self) -> Result<Vec<StackIDWithTime>, opendal::Error> {
        let mut out = Vec::<StackIDWithTime>::new();
        let mut ds = self.operator.list_with(self.prefix.as_str()).await?;
        while let Some(de) = ds.try_next().await? {
            let meta = self
                .operator
                .metadata(&de, Metakey::Mode & Metakey::LastModified)
                .await
                .unwrap();
            match meta.mode() {
                EntryMode::FILE => {
                    if de.name().ends_with(".idx") {
                        let stack_id_str = de.name().strip_suffix(".idx").unwrap();
                        let stack_id_u64 = u64::from_str_radix(stack_id_str, 10).unwrap();
                        out.push(StackIDWithTime {
                            stack_id: stack_id_u64,
                            last_modified: meta.last_modified().unwrap(),
                        });
                    }
                }
                EntryMode::DIR => {
                    println!("skip dir {}", de.path())
                }
                EntryMode::Unknown => continue,
            }
        }
        Ok(out)
    }

    pub async fn list_stack(&self, stack_id: u64) -> Result<Vec<String>, ErrorKind> {
        let mut out = Vec::<String>::new();

        let index_file_path = format!("{}{:09x}.idx", self.prefix, stack_id);
        let imh = match self
            .operator
            .reader_with(index_file_path.as_str())
            .range(0..IndexMagicHeader::size() as u64)
            .await
        {
            Ok(mut reader) => {
                let mut buf = Vec::new();
                buf.resize(IndexMagicHeader::size(), 0);
                match reader.read(&mut buf).await {
                    Ok(_) => {}
                    Err(e) => {
                        return Err(ErrorKind::ReadError(CustomError::new(e.to_string())));
                    }
                }
                let deserialized = match bincode::deserialize::<IndexMagicHeader>(&buf) {
                    Ok(h) => h,
                    Err(e) => {
                        return Err(ErrorKind::ReadError(CustomError::new(e.to_string())));
                    }
                };
                deserialized
            }
            Err(e) => {
                return Err(ErrorKind::ReadError(CustomError::new(e.to_string())));
            }
        };

        assert!(
            imh.index_header_magic == _INDEX_HEADER_MAGIC,
            "header magic mismatch"
        );
        assert!(imh.stack_id == stack_id, "stack_id mismatch");

        let bs = match self
            .operator
            .range_read(&index_file_path, IndexMagicHeader::size() as u64..)
            .await
        {
            Ok(bs) => bs,
            Err(e) => {
                return Err(ErrorKind::ReadError(CustomError::new(e.to_string())));
            }
        };
        for chunk in bs.chunks(IndexRecord::size()) {
            let ir = match bincode::deserialize::<IndexRecord>(chunk) {
                Ok(ir) => ir,
                Err(e) => {
                    return Err(ErrorKind::WriteError(CustomError::new(e.to_string())));
                }
            };
            out.push(format!("{},{}", stack_id, ir.index_id()))
        }
        Ok(out)
    }

    pub async fn get_by_index_id(&self, id: String) -> Result<Vec<u8>, ErrorKind> {
        const INDEX_ID_LENGTH: usize = 12;

        if let Some((stack_id, index_id)) = id.split_once(",") {
            let data_file_path = format!(
                "{}{:09x}.data",
                self.prefix,
                u64::from_str_radix(stack_id, 10).unwrap()
            );

            let bytes_index_id: Vec<u8> = index_id
                .as_bytes()
                .chunks(2)
                .map(|chunk| u8::from_str_radix(std::str::from_utf8(chunk).unwrap(), 16).unwrap())
                .collect();
            assert_eq!(bytes_index_id.len(), INDEX_ID_LENGTH);

            let data_offset = u64::from_le_bytes(bytes_index_id[..8].try_into().unwrap());
            let cookie = u32::from_le_bytes(bytes_index_id[8..].try_into().unwrap());

            let mut dat = self
                .operator
                .range_read(&data_file_path, data_offset..data_offset + 4096)
                .await
                .map_err(|e| ErrorKind::WriteError(CustomError::new(e.to_string())))?;

            let drh = bincode::deserialize::<DataRecordHeader>(&dat[..DataRecordHeader::size()])
                .map_err(|e| ErrorKind::ReadError(CustomError::new(e.to_string())))?;

            assert_eq!(drh.cookie(), cookie);
            if drh.data_size() < 4096 - (DataRecordHeader::size() as u32) {
                dat.drain(0..DataRecordHeader::size());
                return Ok(dat);
            }

            let dat_after_first_4096 = self
                .operator
                .range_read(
                    &data_file_path,
                    data_offset + 4096..data_offset + drh.data_size() as u64 - 4096,
                )
                .await
                .map_err(|e| ErrorKind::WriteError(CustomError::new(e.to_string())))?;
            dat.extend_from_slice(&dat_after_first_4096);
            Ok(dat)
        } else {
            Err(ErrorKind::WriteError(CustomError::new(
                "invalid index id".to_string(),
            )))
        }
    }
}

pub struct ByteStackReader {
    stack_id: i64,
    _index_reader: Reader,
    _meta_reader: Reader,
    _data_reader: Reader,
}

impl ByteStackReader {
    pub fn new(stack_id: i64, i: Reader, m: Reader, d: Reader) -> Self {
        ByteStackReader {
            stack_id: stack_id,
            _index_reader: i,
            _meta_reader: m,
            _data_reader: d,
        }
    }
    pub async fn next(&mut self) -> Option<Item> {
        let result_ir = IndexRecord::new_from_future_reader(&mut self._index_reader).await;
        let result_mr = MetaRecord::new_from_future_reader(&mut self._meta_reader).await;
        let result_dr = DataRecord::new_from_future_reader(&mut self._data_reader).await;

        if let Ok(ir) = result_ir {
            if let Ok(mr) = result_mr {
                if let Ok(dr) = result_dr {
                    return Some((ir, mr, dr));
                }
            }
        }
        return None;
    }
}

pub struct ByteStackWriter {
    stack_id: u64,
    _index_writer: Writer,
    _meta_writer: Writer,
    _data_writer: Writer,
    rng: ThreadRng,
    data_cursor: u64,
    meta_cursor: u64,
}

impl ByteStackWriter {
    pub fn new(stack_id: u64, i: Writer, m: Writer, d: Writer) -> Self {
        ByteStackWriter {
            stack_id: stack_id,
            _index_writer: i,
            _meta_writer: m,
            _data_writer: d,
            data_cursor: 0,
            meta_cursor: 0,
            rng: rand::thread_rng(),
        }
    }

    pub async fn write_files_magic_header(&mut self) {
        self.write_data_header().await.unwrap();
        self.write_index_header().await.unwrap();
        self.write_meta_header().await.unwrap();
    }

    async fn write_index_header(&mut self) -> Result<(), ErrorKind> {
        let index_file_header = IndexMagicHeader::new(self.stack_id);
        match bincode::serialize(&index_file_header) {
            Ok(data) => match self._index_writer.write(data).await {
                Ok(_) => Ok(()),
                Err(e) => {
                    return Err(ErrorKind::WriteError(CustomError::new(e.to_string())));
                }
            },
            Err(e) => {
                return Err(ErrorKind::WriteError(CustomError::new(e.to_string())));
            }
        }
    }
    async fn write_data_header(&mut self) -> Result<(), ErrorKind> {
        let data_file_header = DataMagicHeader::new(self.stack_id);
        match bincode::serialize(&data_file_header) {
            Ok(data) => match self._data_writer.write(data).await {
                Ok(_) => Ok(()),
                Err(e) => {
                    return Err(ErrorKind::WriteError(CustomError::new(e.to_string())));
                }
            },
            Err(e) => {
                return Err(ErrorKind::WriteError(CustomError::new(e.to_string())));
            }
        }
    }
    async fn write_meta_header(&mut self) -> Result<(), ErrorKind> {
        let meta_header = MetaMagicHeader::new(self.stack_id);
        match bincode::serialize(&meta_header) {
            Ok(data) => match self._meta_writer.write(data).await {
                Ok(_) => Ok(()),
                Err(e) => {
                    return Err(ErrorKind::WriteError(CustomError::new(e.to_string())));
                }
            },
            Err(e) => {
                return Err(ErrorKind::WriteError(CustomError::new(e.to_string())));
            }
        }
    }

    async fn write_index(&mut self, ir: IndexRecord) -> Result<(), ErrorKind> {
        match bincode::serialize(&ir) {
            Ok(data) => match self._index_writer.write(data).await {
                Ok(_) => {
                    self.meta_cursor += IndexRecord::size() as u64;
                    Ok(())
                }
                Err(e) => {
                    return Err(ErrorKind::WriteError(CustomError::new(e.to_string())));
                }
            },
            Err(e) => {
                return Err(ErrorKind::WriteError(CustomError::new(e.to_string())));
            }
        }
    }
    async fn write_data(&mut self, dr: DataRecord) -> Result<(), ErrorKind> {
        match bincode::serialize(&dr.header) {
            Ok(data) => match self._data_writer.write(data).await {
                Ok(_) => {
                    self.data_cursor += DataRecordHeader::size() as u64;
                }
                Err(e) => {
                    return Err(ErrorKind::WriteError(CustomError::new(e.to_string())));
                }
            },
            Err(e) => {
                return Err(ErrorKind::WriteError(CustomError::new(e.to_string())));
            }
        }
        let data_len = dr.data.len();
        match self._data_writer.write(dr.data).await {
            Ok(n) => {
                self.data_cursor += data_len as u64;
            }
            Err(e) => {
                return Err(ErrorKind::WriteError(CustomError::new(e.to_string())));
            }
        }

        let padding_len = dr.padding.len();
        match self._data_writer.write(dr.padding).await {
            Ok(n) => {
                self.data_cursor += padding_len as u64;
                Ok(())
            }
            Err(e) => {
                return Err(ErrorKind::WriteError(CustomError::new(e.to_string())));
            }
        }
    }
    async fn write_meta(&mut self, mr: MetaRecord) -> Result<(), ErrorKind> {
        match bincode::serialize(&mr) {
            Ok(data) => match self._meta_writer.write(data).await {
                Ok(_) => Ok(()),
                Err(e) => {
                    return Err(ErrorKind::WriteError(CustomError::new(e.to_string())));
                }
            },
            Err(e) => {
                return Err(ErrorKind::WriteError(CustomError::new(e.to_string())));
            }
        }
    }

    pub async fn put(&mut self, data: Vec<u8>, filename: String) -> Result<(), ErrorKind> {
        let cookie: u32 = self.rng.gen();
        let data_len = data.len() as u32;
        let crc_sum = CASTAGNOLI.checksum(&data);

        let ir = IndexRecord::new(cookie, data_len, self.data_cursor, self.meta_cursor);
        let dr = DataRecord::new(cookie, data_len, crc_sum, data);
        let mr = MetaRecord::new(self.data_cursor, cookie, data_len, filename.into_bytes());
        match self.write_index(ir).await {
            Ok(_) => {}
            Err(e) => {
                return Err(e);
            }
        }
        match self.write_data(dr).await {
            Ok(_) => {}
            Err(e) => {
                return Err(e);
            }
        }
        match self.write_meta(mr).await {
            Ok(_) => Ok(()),
            Err(e) => {
                return Err(e);
            }
        }
    }
    pub async fn close(&mut self) {
        self._index_writer.close().await.unwrap();
        self._meta_writer.close().await.unwrap();
        self._data_writer.close().await.unwrap();
    }
}

pub struct BytestackHandler;

const _ENV_OSS_ENDPOINT: &str = "OSS_ENDPOINT";

fn get_default_endpoint() -> String {
    if let Ok(path_value) = env::var(_ENV_OSS_ENDPOINT) {
        path_value
    } else {
        String::new()
    }
}
impl BytestackHandler {
    pub fn new() -> Self {
        BytestackHandler {}
    }

    fn get_operator_by_path(&self, path: &str) -> Operator {
        let url = Url::parse(path).expect(format!("Failed to parse URL {}", path).as_str());
        match url.scheme() {
            "s3" => {
                let res = parse_s3_url(path).unwrap();
                return init_s3_operator_via_builder(
                    res.0.as_str(),
                    "default",
                    "minioadmin",
                    "minioadmin",
                );
            }
            _ => {
                panic!("unknown scheme: {}, url: {}", url.scheme(), path)
            }
        }
    }

    pub fn open_reader(&self, path: &str) -> StackReader {
        let operator = self.get_operator_by_path(path);
        let bucket_and_prefix = parse_s3_url(path).unwrap();
        StackReader::new(operator, bucket_and_prefix.1)
    }
    pub fn open_writer(&self, path: &str) {}
    pub fn open_appender(&self, path: &str) {}
}

fn init_s3_operator_via_builder(
    bucket: &str,
    region: &str,
    accesskey: &str,
    secretkey: &str,
) -> Operator {
    let mut builder = S3::default();
    builder.endpoint(get_default_endpoint().as_str());
    builder.bucket(bucket);
    builder.region(region);
    builder.access_key_id(accesskey);
    builder.secret_access_key(secretkey);
    let op = Operator::new(builder).unwrap().finish();
    op
}

fn parse_s3_url(path: &str) -> Option<(String, String)> {
    let re = regex::Regex::new(r"s3://([^/]+)/(.*)").unwrap();
    if let Some(captures) = re.captures(path) {
        let bucket = captures.get(1).unwrap().as_str();
        let prefix = captures.get(2).unwrap().as_str();
        Some((bucket.to_string(), prefix.to_string()))
    } else {
        panic!("invalid s3 url: {}", path)
    }
}
