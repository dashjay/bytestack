use super::err::{CustomError, ErrorKind};
use crate::types::{
    data::{DataMagicHeader, DataRecord, DataRecordHeader, _DATA_HEADER_MAGIC},
    index::{IndexMagicHeader, IndexRecord, _INDEX_HEADER_MAGIC},
    meta::{MetaMagicHeader, MetaRecord, _META_HEADER_MAGIC},
};
use crc::{Crc, CRC_32_ISCSI};
pub const CASTAGNOLI: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);
use bincode;
use futures::TryStreamExt;
use opendal::services::S3;
use opendal::EntryMode;
use opendal::Metakey;
use opendal::Operator;
use rand::rngs::ThreadRng;
use rand::Rng;
use std::env;
use std::sync::Mutex;
use tokio::io::AsyncReadExt;
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
                        return Err(ErrorKind::IOError(CustomError::new(e.to_string())));
                    }
                }
                let deserialized = match bincode::deserialize::<IndexMagicHeader>(&buf) {
                    Ok(h) => h,
                    Err(e) => {
                        return Err(ErrorKind::IOError(CustomError::new(e.to_string())));
                    }
                };
                deserialized
            }
            Err(e) => {
                return Err(ErrorKind::IOError(CustomError::new(e.to_string())));
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
                return Err(ErrorKind::IOError(CustomError::new(e.to_string())));
            }
        };
        for chunk in bs.chunks(IndexRecord::size()) {
            let ir = match bincode::deserialize::<IndexRecord>(chunk) {
                Ok(ir) => ir,
                Err(e) => {
                    return Err(ErrorKind::IOError(CustomError::new(e.to_string())));
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
                .map_err(|e| ErrorKind::IOError(CustomError::new(e.to_string())))?;

            let drh = bincode::deserialize::<DataRecordHeader>(&dat[..DataRecordHeader::size()])
                .map_err(|e| ErrorKind::IOError(CustomError::new(e.to_string())))?;

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
                .map_err(|e| ErrorKind::IOError(CustomError::new(e.to_string())))?;
            dat.extend_from_slice(&dat_after_first_4096);
            Ok(dat)
        } else {
            Err(ErrorKind::IOError(CustomError::new(
                "invalid index id".to_string(),
            )))
        }
    }
}

struct InnerWriter {
    data_offset: u64,
    meta_offset: u64,
    rng: ThreadRng,
    _current_index_writer: Writer,
    _current_meta_writer: Writer,
    _current_data_writer: Writer,
}

impl InnerWriter {
    async fn close(mut self) -> Result<(), ErrorKind> {
        if let Err(err) = self._current_data_writer.close().await {
            return Err(ErrorKind::CloseError(CustomError::new(err.to_string())));
        }
        if let Err(err) = self._current_meta_writer.close().await {
            return Err(ErrorKind::CloseError(CustomError::new(err.to_string())));
        }
        if let Err(err) = self._current_index_writer.close().await {
            return Err(ErrorKind::CloseError(CustomError::new(err.to_string())));
        }
        Ok(())
    }

    async fn write_index(&mut self, ir: IndexRecord) -> Result<(), ErrorKind> {
        let data_bytes = bincode::serialize(&ir).unwrap();
        match self._current_index_writer.write(data_bytes).await {
            Ok(_) => return Ok(()),
            Err(err) => return Err(ErrorKind::IOError(CustomError::new(err.to_string()))),
        }
    }
    async fn write_meta(&mut self, mr: MetaRecord) -> Result<(), ErrorKind> {
        let data_bytes = bincode::serialize(&mr).unwrap();
        match self._current_meta_writer.write(data_bytes).await {
            Ok(_) => return Ok(()),
            Err(err) => return Err(ErrorKind::IOError(CustomError::new(err.to_string()))),
        }
    }
    async fn write_data(&mut self, dr: DataRecord) -> Result<(), ErrorKind> {
        let data_bytes = bincode::serialize(&dr.header).unwrap();
        match self._current_meta_writer.write(data_bytes).await {
            Ok(_) => {}
            Err(err) => return Err(ErrorKind::IOError(CustomError::new(err.to_string()))),
        }
        match self._current_meta_writer.write(dr.data).await {
            Ok(_) => {}
            Err(err) => return Err(ErrorKind::IOError(CustomError::new(err.to_string()))),
        }
        match self._current_meta_writer.write(dr.padding).await {
            Ok(_) => Ok(()),
            Err(err) => return Err(ErrorKind::IOError(CustomError::new(err.to_string()))),
        }
    }

    async fn write(
        &mut self,
        buf: Vec<u8>,
        filename: String,
        meta: Option<Vec<u8>>,
    ) -> Result<String, ErrorKind> {
        let meta = match meta {
            Some(meta) => meta,
            None => Vec::new(),
        };
        let crc_sum = CASTAGNOLI.checksum(&buf);
        let cookie: u32 = self.rng.gen();
        let ir = IndexRecord::new(cookie, buf.len() as u32, self.data_offset, self.meta_offset);
        let mr = MetaRecord::new(self.data_offset, cookie, buf.len() as u32, filename, meta);
        let index_id = ir.index_id();
        let dr = DataRecord::new(cookie, buf.len() as u32, crc_sum, buf);

        match self.write_index(ir).await {
            Ok(_) => {}
            Err(e) => return Err(e),
        }
        match self.write_meta(mr).await {
            Ok(_) => {}
            Err(e) => return Err(e),
        }
        match self.write_data(dr).await {
            Ok(_) => {}
            Err(e) => return Err(e),
        }
        Ok(index_id)
    }
}

pub struct StackWriter {
    operator: Operator,
    prefix: String,
    inner_writer: Mutex<Option<InnerWriter>>,
}

impl StackWriter {
    pub fn new(operator: Operator, prefix: String) -> Self {
        StackWriter {
            operator: operator,
            prefix: prefix,
            inner_writer: Mutex::<Option<InnerWriter>>::new(None),
        }
    }

    pub async fn put(
        &self,
        buf: Vec<u8>,
        filename: String,
        meta: Option<Vec<u8>>,
    ) -> Result<String, ErrorKind> {
        match  self.inner_writer.lock() {
            Ok(mut mu)=>{
                match mu.take(){
                    Some(mut writer)=>{
                        match writer.write(buf, filename, meta).await {
                            Ok(id) => {
                                return Ok(id);
                            }
                            Err(e) => {
                                return Err(e);
                            }
                        }
                    },
                    None=>{
                        panic!("should init writer")
                    }
                }
            },Err(e)=>{
                return Err(ErrorKind::IOError(CustomError::new(e.to_string())))
            }
        }
    }

    pub async fn close(&self) -> Result<(), ErrorKind> {
        if let Ok(mut mu) = self.inner_writer.lock() {
            if let Some(writer) = mu.take() {
                writer.close().await?
            }
        }
        Ok(())
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

    pub fn open_reader(&self, path: &str) -> Result<StackReader, ErrorKind> {
        let operator = self.get_operator_by_path(path);
        let (_, prefix) = match parse_s3_url(path) {
            Ok(a) => a,
            Err(e) => {
                return Err(e);
            }
        };
        Ok(StackReader::new(operator, prefix))
    }
    pub fn open_writer(&self, path: &str) -> Result<StackWriter, ErrorKind> {
        let operator = self.get_operator_by_path(path);
        let (_, prefix) = match parse_s3_url(path) {
            Ok(a) => a,
            Err(e) => {
                return Err(e);
            }
        };
        Ok(StackWriter::new(operator, prefix))
    }
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

fn parse_s3_url(path: &str) -> Result<(String, String), ErrorKind> {
    let re = regex::Regex::new(r"s3://([^/]+)/(.*)").unwrap();
    if let Some(captures) = re.captures(path) {
        let bucket = captures.get(1).unwrap().as_str();
        let prefix = captures.get(2).unwrap().as_str();
        Ok((bucket.to_string(), prefix.to_string()))
    } else {
        Err(ErrorKind::ConfigError(CustomError::new(format!(
            "invalid s3 url: {}",
            path
        ))))
    }
}
